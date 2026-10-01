//! Original-source correction and explicit finite standing decision policies.
use agent_mail::{
    decision_recovery as recovery,
    store::{Mailbox, Store},
    task_graph as model,
};
use anyhow::{Context, Result, ensure};
use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Args)]
pub(super) struct Correction {
    #[arg(long)]
    key: String,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    next_step: String,
    #[arg(long,value_parser=super::parse_deadline)]
    check_at: i64,
    #[arg(long,value_parser=super::parse_deadline)]
    escalate_at: i64,
    #[arg(long)]
    evidence: Vec<String>,
    /// Explicitly repair a missing plan; never infer absence from omitted flags.
    #[arg(long,conflicts_with_all=["plan_id","plan_version"],required_unless_present="plan_id")]
    plan_absent: bool,
    #[arg(long, requires = "plan_version")]
    plan_id: Option<i64>,
    #[arg(long, requires = "plan_id")]
    plan_version: Option<i64>,
    /// Every unresolved case at its observed version: ID=VERSION (repeat).
    #[arg(long)]
    case_version: Vec<String>,
}
impl Correction {
    fn prepare(self, source: recovery::Obligation) -> Result<Operation> {
        let expected_plan = if self.plan_absent {
            recovery::ExpectedPlan::Absent
        } else {
            recovery::ExpectedPlan::Present {
                id: self.plan_id.context("supply --plan-id")?,
                version: self.plan_version.context("supply --plan-version")?,
            }
        };
        let mut case_versions = BTreeMap::new();
        for pair in self.case_version {
            let (id, version) = pair
                .split_once('=')
                .context("case version must be ID=VERSION")?;
            ensure!(
                case_versions
                    .insert(id.parse()?, version.parse()?)
                    .is_none(),
                "duplicate case version"
            );
        }
        Ok(Operation::Correct(recovery::SourceCorrection {
            key: self.key,
            source,
            expected_plan,
            case_versions,
            reason: self.reason,
            evidence: self.evidence,
            next_step: self.next_step,
            next_check_at: self.check_at,
            escalation_at: self.escalate_at,
        }))
    }
}
#[derive(Subcommand)]
pub(super) enum TaskFollowup {
    /// Inspect the original source without recording retrieval or making a plan.
    Show {
        id: String,
        #[arg(long)]
        version: i64,
    },
    /// Original writer: correct finite attention metadata after inspecting it.
    Correct {
        id: String,
        #[arg(long)]
        version: i64,
        #[command(flatten)]
        changes: Correction,
    },
}
impl TaskFollowup {
    pub(super) fn prepare(self) -> Result<Operation> {
        match self {
            Self::Show { id, version } => Ok(Operation::Source(recovery::Obligation::Task {
                id,
                version,
            })),
            Self::Correct {
                id,
                version,
                changes,
            } => changes.prepare(recovery::Obligation::Task { id, version }),
        }
    }
}
#[derive(Subcommand)]
pub(super) enum MailFollowup {
    /// Inspect one original recipient delivery; does not receipt the message.
    Show {
        id: i64,
        #[arg(long)]
        recipient: String,
    },
    /// Original sender: correct one recipient's plan without settling the request.
    Correct {
        id: i64,
        #[arg(long)]
        recipient: String,
        #[command(flatten)]
        changes: Correction,
    },
}
impl MailFollowup {
    pub(super) fn prepare(self) -> Result<Operation> {
        match self {
            Self::Show { id, recipient } => Ok(Operation::Source(recovery::Obligation::Delivery {
                message: id,
                recipient,
            })),
            Self::Correct {
                id,
                recipient,
                changes,
            } => changes.prepare(recovery::Obligation::Delivery {
                message: id,
                recipient,
            }),
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum Action {
    ContinueStrategy,
    Recommend,
    Accept,
    Complete,
    Cancel,
    Fail,
}
impl From<Action> for model::DecisionAction {
    fn from(a: Action) -> Self {
        match a {
            Action::ContinueStrategy => Self::ContinueStrategy,
            Action::Recommend => Self::Recommend,
            Action::Accept => Self::Accept,
            Action::Complete => Self::Complete,
            Action::Cancel => Self::Cancel,
            Action::Fail => Self::Fail,
        }
    }
}
#[derive(Args)]
pub(super) struct Policy {
    /// Standing policy ID. This operation is separate from task creation.
    id: String,
    #[arg(long, conflicts_with = "mail", required_unless_present = "mail")]
    task: Option<String>,
    #[arg(long, requires = "task")]
    version: Option<i64>,
    #[arg(long, requires = "recipient")]
    mail: Option<i64>,
    #[arg(long, requires = "mail")]
    recipient: Option<String>,
    /// Exact semantic guard from source inspection; omit for legacy work or Mail.
    #[arg(long)]
    input_epoch: Option<i64>,
    #[arg(long)]
    candidate: Option<String>,
    #[arg(long)]
    outcome: Option<String>,
    #[arg(long)]
    policy_version: Option<i64>,
    #[arg(long)]
    key: String,
    #[arg(long)]
    reason: String,
    #[arg(long)]
    authority_ref: String,
    #[arg(long,value_parser=super::parse_deadline)]
    deadline: i64,
    #[arg(long)]
    reviewer: Option<String>,
    #[arg(long)]
    allow_writer_fallback: bool,
    #[arg(long)]
    revoke: bool,
    #[arg(long, value_enum, required = true)]
    action: Vec<Action>,
    #[command(flatten)]
    contract: super::task_contract::ContractFlags,
}
impl Policy {
    fn prepare(self) -> Result<Operation> {
        let source = if let Some(id) = self.task {
            recovery::Obligation::Task {
                id,
                version: self.version.context("task policy requires --version")?,
            }
        } else {
            recovery::Obligation::Delivery {
                message: self.mail.context("supply --task or --mail")?,
                recipient: self.recipient.context("supply --recipient")?,
            }
        };
        Ok(Operation::Policy(model::DecisionPolicyDecision {
            key: self.key,
            expected_revision: self.policy_version,
            source: model::DecisionSourceExpectation {
                source,
                input_epoch: self.input_epoch,
                candidate: self.candidate,
                outcome: self.outcome,
            },
            policy: model::DecisionPolicy {
                id: self.id,
                contract: self.contract.prepare(None)?,
                actions: self.action.into_iter().map(Into::into).collect(),
                reviewer: self.reviewer,
                allow_writer_fallback: self.allow_writer_fallback,
                deadline: self.deadline,
                authority_ref: self.authority_ref,
                revoked: self.revoke,
            },
            reason: self.reason,
        }))
    }
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum SuccessKind {
    Accepted,
    Completed,
}
impl From<SuccessKind> for model::OutcomeKind {
    fn from(value: SuccessKind) -> Self {
        match value {
            SuccessKind::Accepted => Self::Accepted,
            SuccessKind::Completed => Self::Completed,
        }
    }
}

#[derive(Subcommand)]
pub(super) enum Decision {
    /// Original writer: atomically grant finite source continuation and accept its decision candidate.
    ContinueStrategy {
        /// The materialized decision task, not its original source task.
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        case_version: i64,
        #[arg(long)]
        policy_version: i64,
        /// Original source execution revision, distinct from the decision task version.
        #[arg(long)]
        execution_version: i64,
        #[arg(long)]
        additional_segments: u32,
        #[arg(long,value_parser=super::parse_deadline)]
        expires_at: i64,
        #[arg(long)]
        candidate: String,
        #[arg(long, value_enum, default_value = "accepted")]
        kind: SuccessKind,
        /// Outer atomic operation key.
        #[arg(long)]
        key: String,
        /// Ordinary decision outcome key; must differ from the outer key.
        #[arg(long)]
        decision_key: String,
        /// Exact original source scheduler request key.
        #[arg(long)]
        continuation_key: String,
        #[arg(long)]
        reason: String,
    },
    /// Original writer: perform the single policy-authorized reviewer fallback.
    WriterFallback {
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        case_version: i64,
        #[arg(long)]
        policy_version: i64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        reason: String,
    },
    /// Inspect a finite responsibility case; transport receipt does not settle it.
    Show { id: i64 },
    /// Set or revoke a finite policy as the original source writer/sender.
    Policy(Box<Policy>),
    /// Advanced correction for an unmaterialized legacy case with exact source guards.
    Correct {
        id: i64,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        reason: String,
        /// Saved ObligationView from source inspection; never silently refreshed.
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        evidence: Vec<String>,
        #[arg(long,value_parser=super::parse_deadline)]
        check_at: i64,
        #[arg(long,value_parser=super::parse_deadline)]
        escalate_at: i64,
    },
}
impl Decision {
    pub(super) fn prepare(self) -> Result<Operation> {
        Ok(match self {
            Self::ContinueStrategy {
                id,
                version,
                case_version,
                policy_version,
                execution_version,
                additional_segments,
                expires_at,
                candidate,
                kind,
                key,
                decision_key,
                continuation_key,
                reason,
            } => {
                ensure!(
                    key != decision_key,
                    "distinct_decision_continuation_keys_required"
                );
                Operation::Continue(
                    id,
                    Box::new(model::DecisionContinuation {
                        key,
                        case_version,
                        policy_revision: policy_version,
                        continuation: agent_mail::execution::ContinueStrategy {
                            key: continuation_key,
                            reason: reason.clone(),
                            execution_revision: execution_version,
                            additional_segments,
                            expires_at,
                        },
                        decision: model::TaskDecision {
                            key: decision_key,
                            version,
                            reason,
                            work_patch: agent_mail::work::WorkPatch::default(),
                            scope: model::Change::Keep,
                            contract: model::Change::Keep,
                            authorization: model::Change::Keep,
                            requirements: model::Change::Keep,
                            parent: model::Change::Keep,
                            expected_parent_versions: BTreeMap::new(),
                            clear_invalidation: false,
                            outcome: model::OutcomeChange::Success {
                                kind: kind.into(),
                                candidate,
                            },
                            resolve_message: None,
                        },
                    }),
                )
            }
            Self::WriterFallback {
                id,
                version,
                case_version,
                policy_version,
                key,
                reason,
            } => Operation::Fallback(
                id,
                model::DecisionWriterFallback {
                    key,
                    version,
                    case_version,
                    policy_revision: policy_version,
                    reason,
                },
            ),
            Self::Show { id } => Operation::Case(id),
            Self::Policy(p) => return (*p).prepare(),
            Self::Correct {
                id,
                version,
                key,
                reason,
                source,
                evidence,
                check_at,
                escalate_at,
            } => Operation::CaseCorrection(recovery::CaseCorrection {
                key,
                case_id: id,
                version,
                source: serde_json::from_str(&super::read_body(&source)?)?,
                reason,
                evidence,
                review_at: check_at,
                hard_due: escalate_at,
            }),
        })
    }
}
pub(crate) enum Operation {
    Continue(String, Box<model::DecisionContinuation>),
    Fallback(String, model::DecisionWriterFallback),
    Source(recovery::Obligation),
    Correct(recovery::SourceCorrection),
    Case(i64),
    CaseCorrection(recovery::CaseCorrection),
    Policy(model::DecisionPolicyDecision),
}
impl Operation {
    pub(crate) async fn run(self, store: &Store, actor: &Mailbox, time: i64) -> Result<Value> {
        Ok(match self {
            Self::Continue(id, request) => {
                json!({"schema_version":1,"result":store.decision_continue_strategy(actor,&id,*request,time).await?})
            }
            Self::Fallback(id, request) => serde_json::to_value(
                store
                    .decision_writer_fallback(actor, &id, request, time)
                    .await?,
            )?,
            // Raw typed guard can be saved for an explicit future case correction.
            Self::Source(source) => {
                serde_json::to_value(store.inspect_obligation(actor, source).await?)?
            }
            Self::Correct(request) => store.correct_obligation(actor, request, time).await?,
            Self::Case(id) => {
                json!({"schema_version":1,"case":store.decision_case(actor,id).await?})
            }
            Self::CaseCorrection(request) => {
                json!({"schema_version":1,"case":store.correct_decision_case(actor,request,time).await?})
            }
            Self::Policy(request) => {
                json!({"schema_version":1,"policy":store.decision_policy(actor,request,time).await?})
            }
        })
    }
}
