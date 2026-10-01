//! Product grammar translated once into the model owner's atomic requests.
use agent_mail::{
    store::{Mailbox, Store},
    task_graph as model,
    work::{WorkDraft, WorkPatch},
};
use anyhow::{Context, Result, ensure};
use clap::{Args, ValueEnum};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum Outcome {
    Accepted,
    Completed,
    Cancelled,
    Failed,
}
impl From<Outcome> for model::OutcomeKind {
    fn from(value: Outcome) -> Self {
        match value {
            Outcome::Accepted => Self::Accepted,
            Outcome::Completed => Self::Completed,
            Outcome::Cancelled => Self::Cancelled,
            Outcome::Failed => Self::Failed,
        }
    }
}
#[derive(Clone, Copy, ValueEnum)]
pub(super) enum Phase {
    Execute,
    Accept,
}
impl From<Phase> for model::Phase {
    fn from(value: Phase) -> Self {
        match value {
            Phase::Execute => Self::Execute,
            Phase::Accept => Self::Accept,
        }
    }
}

fn pair(value: &str) -> Result<(String, String)> {
    let (key, value) = value.split_once('=').context("use NAME=VALUE")?;
    ensure!(
        !key.trim().is_empty() && !value.trim().is_empty(),
        "NAME and VALUE must be nonempty"
    );
    Ok((key.to_owned(), value.to_owned()))
}
fn unique_pairs(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for value in values {
        let (key, value) = pair(value)?;
        ensure!(
            result.insert(key, value).is_none(),
            "duplicate NAME in repeated flags"
        );
    }
    Ok(result)
}
fn outcome(value: &str) -> Result<model::OutcomeKind> {
    Ok(match value {
        "accepted" => model::OutcomeKind::Accepted,
        "completed" => model::OutcomeKind::Completed,
        "cancelled" => model::OutcomeKind::Cancelled,
        "failed" => model::OutcomeKind::Failed,
        _ => anyhow::bail!("outcome must be accepted, completed, cancelled or failed"),
    })
}

#[derive(Args, Default)]
#[group(id = "contract_flags", multiple = true)]
pub(super) struct ContractFlags {
    /// Concrete deliverable; creation uses the task description if omitted.
    #[arg(long)]
    deliverable: Option<String>,
    /// Acceptance criterion ID=TEXT (repeat).
    #[arg(long)]
    criterion: Vec<String>,
    /// Literal authorized scope unit (repeat).
    #[arg(long = "allow")]
    allowed_scope: Vec<String>,
    /// Permit completed outcomes; writer acceptance is the default.
    #[arg(long)]
    completion_allowed: bool,
    #[arg(long)]
    allow_delegation: bool,
    /// Required consent to invalidate dependent inputs when their basis changes.
    #[arg(long)]
    allow_input_invalidation: bool,
    #[arg(long)]
    max_attempts: Option<u32>,
    #[arg(long, value_parser = super::parse_duration)]
    max_elapsed: Option<i64>,
    /// Exact integer cost cap, paired with its runtime-supported unit.
    #[arg(long, requires = "cost_unit")]
    max_cost: Option<u64>,
    #[arg(long, requires = "max_cost")]
    cost_unit: Option<String>,
}
impl ContractFlags {
    pub(super) fn prepare(self, default_deliverable: Option<&str>) -> Result<model::Contract> {
        let mut missing = Vec::new();
        if self.deliverable.is_none() && default_deliverable.is_none() {
            missing.push("--deliverable");
        }
        if self.criterion.is_empty() {
            missing.push("--criterion ID=TEXT");
        }
        if self.allowed_scope.is_empty() {
            missing.push("--allow UNIT");
        }
        if self.max_attempts.is_none() {
            missing.push("--max-attempts N");
        }
        if self.max_elapsed.is_none() {
            missing.push("--max-elapsed DURATION");
        }
        ensure!(
            missing.is_empty(),
            "incomplete_contract: supply {} (or explicitly use --untracked for legacy creation)",
            missing.join(", ")
        );
        ensure!(
            self.max_attempts != Some(0),
            "incomplete_contract: --max-attempts must be positive"
        );
        Ok(model::Contract {
            deliverable: self
                .deliverable
                .or_else(|| default_deliverable.map(str::to_owned))
                .context("deliverable required")?,
            criteria: unique_pairs(&self.criterion)?
                .into_iter()
                .map(|(id, description)| model::Criterion { id, description })
                .collect(),
            allowed_scope: self.allowed_scope,
            completion: if self.completion_allowed {
                model::Completion::CompletionAllowed
            } else {
                model::Completion::WriterAcceptance
            },
            allow_delegation: self.allow_delegation,
            allow_input_invalidation: self.allow_input_invalidation,
            budget: model::Budget {
                max_attempts: self.max_attempts.context("attempt budget required")?,
                max_elapsed_seconds: self.max_elapsed.context("elapsed budget required")? as u64,
                max_cost: self.max_cost.map(|amount| model::CostLimit {
                    amount,
                    unit: self.cost_unit.unwrap_or_default(),
                }),
            },
        })
    }
}

#[derive(Args, Default)]
#[group(id = "authority_flags", multiple = true)]
pub(super) struct AuthorityFlags {
    /// Reference to actual prior authorization. Reading Mail does not grant it.
    #[arg(long, conflicts_with = "inherit_authority")]
    authorize: Option<String>,
    /// Inherit authority only from the declared parent.
    #[arg(long, requires = "parent")]
    inherit_authority: bool,
    /// Record this authorization as held; no execution is authorized.
    #[arg(long, conflicts_with = "revoke_authority")]
    held: bool,
    #[arg(long)]
    revoke_authority: bool,
    /// Approved scope unit; omitted means the explicitly supplied contract scope.
    #[arg(long)]
    approved_scope: Vec<String>,
}
impl AuthorityFlags {
    fn prepare(
        self,
        scope: &[String],
        parent: Option<&model::ParentLink>,
        reason: &str,
    ) -> Result<model::Authorization> {
        let source = if self.inherit_authority {
            model::AuthoritySource::Parent {
                task: parent
                    .context("inherit_authority requires a parent")?
                    .task
                    .clone(),
            }
        } else {
            model::AuthoritySource::Direct {authority_ref:self.authorize.context("authority_required: supply --authorize REF or --inherit-authority with --parent")?}
        };
        Ok(model::Authorization {
            state: if self.held {
                model::AuthorityState::Held
            } else if self.revoke_authority {
                model::AuthorityState::Revoked
            } else {
                model::AuthorityState::Authorized
            },
            source,
            approved_scope: if self.approved_scope.is_empty() {
                scope.to_vec()
            } else {
                self.approved_scope
            },
            reason: reason.into(),
        })
    }
    fn supplied(&self) -> bool {
        self.authorize.is_some()
            || self.inherit_authority
            || self.held
            || self.revoke_authority
            || !self.approved_scope.is_empty()
    }
}

#[derive(Args, Default)]
#[group(id = "graph_flags", multiple = true)]
pub(super) struct GraphFlags {
    /// ALL-of prerequisite, requiring accepted outcome unless overridden (repeat).
    #[arg(long)]
    after: Vec<String>,
    /// Override a declared prerequisite: TASK=accepted|completed|cancelled|failed.
    #[arg(long)]
    after_outcome: Vec<String>,
    /// Optional exact revision pin for a declared prerequisite: TASK=REVISION.
    #[arg(long)]
    after_revision: Vec<String>,
    #[arg(long)]
    parent: Option<String>,
    #[arg(long, requires = "parent")]
    optional_child: bool,
    #[arg(long, requires = "parent", value_enum)]
    parent_outcome: Option<Outcome>,
    #[arg(long, requires = "parent")]
    parent_revision: Option<String>,
    /// Every affected parent at its observed version: TASK=VERSION (repeat).
    #[arg(long)]
    parent_version: Vec<String>,
}
struct PreparedGraph {
    requirements: Vec<model::Requirement>,
    parent: Option<model::ParentLink>,
    expected_parent_versions: BTreeMap<String, i64>,
}

impl GraphFlags {
    fn prepare(self) -> Result<PreparedGraph> {
        let mut outcomes = unique_pairs(&self.after_outcome)?;
        let mut revisions = unique_pairs(&self.after_revision)?;
        let mut requirements = Vec::new();
        for task in self.after {
            ensure!(
                !requirements
                    .iter()
                    .any(|r: &model::Requirement| r.task == task),
                "duplicate prerequisite"
            );
            let kind = outcomes
                .remove(&task)
                .map(|value| outcome(&value))
                .transpose()?
                .unwrap_or(model::OutcomeKind::Accepted);
            requirements.push(model::Requirement {
                revision: revisions.remove(&task),
                task,
                outcome: kind,
            });
        }
        ensure!(
            outcomes.is_empty() && revisions.is_empty(),
            "prerequisite overrides require a corresponding --after TASK"
        );
        let parent = self.parent.map(|task| model::ParentLink {
            task,
            required: !self.optional_child,
            outcome: self
                .parent_outcome
                .map(Into::into)
                .unwrap_or(model::OutcomeKind::Accepted),
            revision: self.parent_revision,
        });
        let versions = unique_pairs(&self.parent_version)?
            .into_iter()
            .map(|(id, value)| {
                Ok((
                    id,
                    value
                        .parse::<i64>()
                        .context("parent version must be an integer")?,
                ))
            })
            .collect::<Result<_>>()?;
        Ok(PreparedGraph {
            requirements,
            parent,
            expected_parent_versions: versions,
        })
    }
}

#[derive(Args)]
pub(super) struct CreateFlags {
    /// Explicit compatibility path without a contract or execution accounting.
    #[arg(long, conflicts_with_all = ["contract_flags", "authority_flags", "graph_flags", "key", "reason"])]
    pub(super) untracked: bool,
    #[arg(long, required_unless_present = "untracked")]
    key: Option<String>,
    #[arg(long, required_unless_present = "untracked")]
    reason: Option<String>,
    #[command(flatten)]
    contract: ContractFlags,
    #[command(flatten)]
    authority: AuthorityFlags,
    #[command(flatten)]
    graph: GraphFlags,
}
impl CreateFlags {
    pub(super) fn prepare(self, work: WorkDraft) -> Result<Operation> {
        let contract = self.contract.prepare(Some(&work.scope))?;
        let PreparedGraph {
            requirements,
            parent,
            expected_parent_versions,
        } = self.graph.prepare()?;
        let reason = self.reason.context("supply --reason")?;
        let authorization =
            self.authority
                .prepare(&contract.allowed_scope, parent.as_ref(), &reason)?;
        Ok(Operation::Create(model::TaskCreate {
            key: self.key.context("supply --key")?,
            reason,
            draft: model::TaskDraft {
                work,
                contract,
                authorization,
                requirements,
                parent,
            },
            expected_parent_versions,
        }))
    }
}

#[derive(Args)]
pub(super) struct Adopt {
    pub(super) id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    key: String,
    #[arg(long)]
    reason: String,
    #[command(flatten)]
    contract: ContractFlags,
    #[command(flatten)]
    authority: AuthorityFlags,
    #[command(flatten)]
    graph: GraphFlags,
}
impl Adopt {
    pub(super) fn prepare(self) -> Result<Operation> {
        let contract = self.contract.prepare(None)?;
        let PreparedGraph {
            requirements,
            parent,
            expected_parent_versions,
        } = self.graph.prepare()?;
        let authorization =
            self.authority
                .prepare(&contract.allowed_scope, parent.as_ref(), &self.reason)?;
        Ok(Operation::Adopt(
            self.id,
            model::TaskAdopt {
                key: self.key,
                version: self.version,
                reason: self.reason,
                contract,
                authorization,
                requirements,
                parent,
                expected_parent_versions,
            },
        ))
    }
}

#[derive(Args)]
#[group(id = "input_flags", multiple = true)]
struct InputFlags {
    #[arg(long)]
    input_task_version: Option<i64>,
    #[arg(long)]
    input_epoch: Option<i64>,
    #[arg(long)]
    input_owner_generation: Option<i64>,
    #[arg(long, value_enum)]
    input_phase: Option<Phase>,
    /// Exact snapshot prerequisite TASK=OUTCOME_ID (repeat).
    #[arg(long)]
    prerequisite_outcome: Vec<String>,
    /// Exact snapshot required child TASK=OUTCOME_ID (repeat).
    #[arg(long)]
    required_child_outcome: Vec<String>,
    /// Exact snapshot ancestor TASK=AUTHORITY_DIGEST (repeat).
    #[arg(long)]
    ancestor_authority: Vec<String>,
}
impl InputFlags {
    fn prepare(self, group: &str, task: &str) -> Result<model::InputSnapshot> {
        Ok(model::InputSnapshot {
            group: group.into(),
            task: task.into(),
            task_version_at_capture: self
                .input_task_version
                .context("supply --input-task-version from the original snapshot")?,
            input_epoch: self.input_epoch.context("supply --input-epoch")?,
            owner_binding_generation: self
                .input_owner_generation
                .context("supply --input-owner-generation")?,
            phase: self
                .input_phase
                .context("supply --input-phase from the original snapshot")?
                .into(),
            prerequisite_outcome_ids: unique_pairs(&self.prerequisite_outcome)?,
            required_child_outcome_ids: unique_pairs(&self.required_child_outcome)?,
            ancestor_authority_digests: unique_pairs(&self.ancestor_authority)?,
        })
    }
}

#[derive(Args)]
pub(super) struct Candidate {
    id: String,
    #[arg(long)]
    version: i64,
    #[arg(long)]
    key: String,
    #[arg(long)]
    revision: String,
    #[arg(long)]
    summary: String,
    /// Capture inputs BEFORE producing the artifact; never refresh stale output.
    #[arg(long, conflicts_with = "input_flags")]
    inputs: Option<PathBuf>,
    #[command(flatten)]
    snapshot: InputFlags,
    /// Criterion ID=REFERENCE, repeated for multiple references.
    #[arg(long, required = true)]
    criterion_evidence: Vec<String>,
}
impl Candidate {
    pub(super) fn prepare(self, group: &str) -> Result<Operation> {
        let inputs = if let Some(path) = self.inputs {
            serde_json::from_str(&super::read_body(&path)?)?
        } else {
            self.snapshot.prepare(group, &self.id)?
        };
        let mut evidence: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for value in self.criterion_evidence {
            let (id, reference) = pair(&value)?;
            evidence.entry(id).or_default().push(reference);
        }
        Ok(Operation::Candidate(
            self.id,
            model::CandidateRequest {
                version: self.version,
                key: self.key,
                candidate: model::CandidateDraft {
                    revision: self.revision,
                    summary: self.summary,
                    inputs,
                    criterion_evidence: evidence
                        .into_iter()
                        .map(|(criterion_id, references)| model::CriterionEvidence {
                            criterion_id,
                            references,
                        })
                        .collect(),
                },
            },
        ))
    }
}

#[derive(Args)]
pub(super) struct Decide {
    id: String,
    /// Complete advanced atomic decision; cannot be mixed with flags.
    #[arg(long, conflicts_with_all=["decision_flags", "contract_flags", "authority_flags", "graph_flags"])]
    file: Option<PathBuf>,
    #[command(flatten)]
    flags: DecisionFlags,
    #[command(flatten)]
    contract: ContractFlags,
    #[command(flatten)]
    authority: AuthorityFlags,
    #[command(flatten)]
    graph: GraphFlags,
}
#[derive(Args)]
#[group(id = "decision_flags", multiple = true)]
struct DecisionFlags {
    #[arg(long)]
    version: Option<i64>,
    #[arg(long)]
    key: Option<String>,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    state: Option<agent_mail::states::TaskState>,
    #[arg(long)]
    next_action: Option<String>,
    #[arg(long)]
    scope: Option<String>,
    #[arg(long, value_parser=super::parse_deadline, conflicts_with="clear_deadline")]
    deadline: Option<i64>,
    #[arg(long)]
    clear_deadline: bool,
    #[arg(long, conflicts_with = "clear_evidence")]
    evidence: Vec<String>,
    #[arg(long)]
    clear_evidence: bool,
    #[arg(long)]
    resolve: Option<i64>,
    /// Replace the complete contract; budgets cannot be replenished.
    #[arg(long)]
    replace_contract: bool,
    /// Replace the complete prerequisite set; empty explicitly clears it.
    #[arg(long)]
    replace_requirements: bool,
    #[arg(long, conflicts_with = "parent")]
    detach_parent: bool,
    #[arg(long)]
    clear_invalidation: bool,
    #[arg(long, value_enum, conflicts_with = "withdraw_outcome")]
    kind: Option<Outcome>,
    #[arg(long, requires = "kind")]
    candidate: Option<String>,
    #[arg(long, requires = "kind")]
    revision: Option<String>,
    #[arg(long)]
    withdraw_outcome: bool,
}
impl Decide {
    pub(super) fn prepare(self) -> Result<Operation> {
        if let Some(path) = self.file {
            return Ok(Operation::Decide(
                self.id,
                serde_json::from_str(&super::read_body(&path)?)?,
            ));
        }
        let f = self.flags;
        let had_parent = self.graph.parent.is_some();
        let had_requirements = !self.graph.after.is_empty()
            || !self.graph.after_outcome.is_empty()
            || !self.graph.after_revision.is_empty();
        ensure!(
            !had_requirements || f.replace_requirements,
            "supply --replace-requirements for the entire prerequisite set"
        );
        let PreparedGraph {
            requirements,
            parent,
            expected_parent_versions,
        } = self.graph.prepare()?;
        let reason = f.reason.context("supply --reason")?;
        let authority_supplied = self.authority.supplied();
        let contract = if f.replace_contract {
            Some(self.contract.prepare(None)?)
        } else {
            ensure!(
                self.contract.deliverable.is_none()
                    && self.contract.criterion.is_empty()
                    && self.contract.allowed_scope.is_empty()
                    && !self.contract.completion_allowed
                    && !self.contract.allow_delegation
                    && !self.contract.allow_input_invalidation
                    && self.contract.max_attempts.is_none()
                    && self.contract.max_elapsed.is_none()
                    && self.contract.max_cost.is_none()
                    && self.contract.cost_unit.is_none(),
                "supply --replace-contract with the complete contract"
            );
            None
        };
        let authorization = if authority_supplied {
            let scope = contract
                .as_ref()
                .map(|c| c.allowed_scope.as_slice())
                .unwrap_or(&[]);
            ensure!(
                !scope.is_empty() || !self.authority.approved_scope.is_empty(),
                "authorization change requires explicit --approved-scope or a complete replacement contract"
            );
            model::Change::Set(self.authority.prepare(scope, parent.as_ref(), &reason)?)
        } else {
            model::Change::Keep
        };
        let outcome = if f.withdraw_outcome {
            model::OutcomeChange::Withdraw
        } else if let Some(kind) = f.kind {
            match kind {
                Outcome::Accepted | Outcome::Completed => {
                    ensure!(
                        f.revision.is_none(),
                        "success uses the immutable candidate revision; omit --revision"
                    );
                    model::OutcomeChange::Success {
                        kind: kind.into(),
                        candidate: f.candidate.context("success requires --candidate ID")?,
                    }
                }
                Outcome::Cancelled | Outcome::Failed => {
                    ensure!(
                        f.candidate.is_none(),
                        "negative outcomes do not take --candidate"
                    );
                    model::OutcomeChange::Negative {
                        kind: kind.into(),
                        revision: f.revision.context("negative outcome requires --revision")?,
                    }
                }
            }
        } else {
            model::OutcomeChange::Keep
        };
        Ok(Operation::Decide(
            self.id,
            model::TaskDecision {
                key: f.key.context("supply --key")?,
                version: f.version.context("supply --version from task inspect")?,
                reason,
                work_patch: WorkPatch {
                    owner: f.owner,
                    state: f.state,
                    next_action: f.next_action,
                    deadline: if f.clear_deadline {
                        Some(None)
                    } else {
                        f.deadline.map(Some)
                    },
                    accepted_revision: None,
                    evidence: if f.clear_evidence {
                        Some(vec![])
                    } else if f.evidence.is_empty() {
                        None
                    } else {
                        Some(f.evidence)
                    },
                },
                scope: f.scope.map_or(model::Change::Keep, model::Change::Set),
                contract: contract.map_or(model::Change::Keep, model::Change::Set),
                authorization,
                requirements: if f.replace_requirements {
                    model::Change::Set(requirements)
                } else {
                    model::Change::Keep
                },
                parent: if f.detach_parent {
                    model::Change::Set(None)
                } else if had_parent {
                    model::Change::Set(parent)
                } else {
                    model::Change::Keep
                },
                expected_parent_versions,
                clear_invalidation: f.clear_invalidation,
                outcome,
                resolve_message: f.resolve,
            },
        ))
    }
}

pub(crate) enum Operation {
    Tracking {
        after: String,
        limit: u32,
    },
    Create(model::TaskCreate),
    Adopt(String, model::TaskAdopt),
    Decide(String, model::TaskDecision),
    Candidate(String, model::CandidateRequest),
    Inspect(String),
    Inputs {
        id: String,
        version: i64,
        phase: model::Phase,
    },
    Results {
        id: String,
        after: Option<i64>,
    },
}
impl Operation {
    pub(crate) async fn run(self, store: &Store, actor: &Mailbox, time: i64) -> Result<Value> {
        Ok(match self {
            Self::Tracking { after, limit } => {
                agent_mail::status::task_tracking(store, actor, &after, limit).await?
            }
            Self::Create(request) => {
                serde_json::to_value(store.task_create(actor, request, time).await?)?
            }
            Self::Adopt(id, request) => {
                serde_json::to_value(store.task_adopt(actor, &id, request, time).await?)?
            }
            Self::Decide(id, request) => {
                serde_json::to_value(store.task_decide(actor, &id, request, time).await?)?
            }
            Self::Candidate(id, request) => {
                json!({"schema_version":1,"candidate":store.task_candidate(actor,&id,request,time).await?})
            }
            Self::Inspect(id) => {
                json!({"schema_version":1,"observed_at":time,"task":store.task_inspect(actor,&id).await?,"execution":store.execution_inspect(actor,&id).await?})
            }
            // Raw typed snapshot can be redirected and later supplied as --inputs.
            Self::Inputs { id, version, phase } => serde_json::to_value(
                store
                    .task_capture_inputs(actor, &id, version, phase)
                    .await?,
            )?,
            Self::Results { id, after } => {
                json!({"schema_version":1,"results":store.task_results(actor,&id,after).await?})
            }
        })
    }
}
