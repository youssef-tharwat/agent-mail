//! Flag translation for attention reports. Reports never settle business work.
use agent_mail::{
    followup::{Checkpoint, WaitFor},
    states::TaskState,
};
use anyhow::{Context, Result, ensure};
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
#[group(id = "checkpoint_flags", multiple = true)]
pub(super) struct CheckpointFlags {
    /// Observed attention plan version; distinct from the task version.
    #[arg(long, alias = "attention-version")]
    plan_version: Option<i64>,
    #[arg(long)]
    next_step: Option<String>,
    /// Absolute next review time in UTC. Reuse this value on an identical retry.
    #[arg(long, value_parser = super::parse_deadline)]
    check_at: Option<i64>,
    #[arg(long, conflicts_with_all = ["wait_task", "wait_for"])]
    wait_mail: Option<i64>,
    #[arg(long, conflicts_with = "wait_for")]
    wait_task: Option<String>,
    #[arg(long, requires = "wait_task")]
    wait_state: Vec<TaskState>,
    #[arg(long, requires = "wait_reason")]
    wait_for: Option<String>,
    #[arg(long, requires = "wait_for")]
    wait_reason: Option<String>,
    #[arg(long)]
    evidence: Vec<String>,
    /// Writer-only attention extension; never extends execution budgets.
    #[arg(long, value_parser = super::parse_deadline, requires = "reason")]
    extend_until: Option<i64>,
    #[arg(long, requires = "extend_until")]
    reason: Option<String>,
}

#[derive(Args)]
pub(super) struct CheckpointArgs {
    /// Advanced complete report, or - for bounded stdin; cannot mix with flags.
    #[arg(long, conflicts_with = "checkpoint_flags")]
    file: Option<PathBuf>,
    #[command(flatten)]
    flags: CheckpointFlags,
}

impl CheckpointArgs {
    pub(super) fn prepare(self) -> Result<Checkpoint> {
        if let Some(path) = self.file {
            return Ok(serde_json::from_str(&super::read_body(&path)?)?);
        }
        let f = self.flags;
        let waiting = if let Some(id) = f.wait_mail {
            Some(WaitFor::Mail { id })
        } else if let Some(id) = f.wait_task {
            ensure!(
                !f.wait_state.is_empty(),
                "supply --wait-state for --wait-task"
            );
            Some(WaitFor::Task {
                id,
                states: f.wait_state,
            })
        } else if let Some(responsible) = f.wait_for {
            Some(WaitFor::External {
                responsible,
                reason: f.wait_reason.context("supply --wait-reason")?,
            })
        } else {
            None
        };
        Ok(Checkpoint {
            version: f.plan_version.context(
                "supply --plan-version from the source followup (zero for an initial plan)",
            )?,
            next_step: f.next_step.context("supply --next-step")?,
            next_check_at: f.check_at.context("supply --check-at UTC")?,
            waiting,
            evidence: f.evidence,
            extend_until: f.extend_until,
            reason: f.reason,
        })
    }
}

use agent_mail::{
    progress as owner,
    store::{Mailbox, Store},
    task_graph,
};
use clap::{Subcommand, ValueEnum};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum Judgment {
    Qualify,
    Reject,
    Revoke,
}
#[derive(Subcommand)]
pub(super) enum Progress {
    /// Immutable policy/judgment history. A report alone is not qualified progress.
    History {
        id: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    /// Writer: replace the visible milestone policy without resetting budgets.
    Policy {
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        progress_version: Option<i64>,
        #[arg(long)]
        key: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        max_segments: u32,
        #[arg(long,value_parser=super::parse_duration)]
        max_elapsed: Option<i64>,
        /// Stable milestone ID (repeat); unchanged IDs must retain their meaning.
        #[arg(long)]
        milestone: Vec<String>,
        /// MILESTONE=CRITERION, repeated for multiple criteria.
        #[arg(long)]
        milestone_criterion: Vec<String>,
        /// MILESTONE=SCOPE_UNIT, repeated for multiple scope units.
        #[arg(long)]
        milestone_scope: Vec<String>,
    },
    /// Qualify/reject an immutable report, or explicitly revoke a judgment.
    Judge {
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        progress_version: i64,
        #[arg(long)]
        key: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        milestone: String,
        #[arg(long, value_enum)]
        judgment: Judgment,
        #[arg(long)]
        report: Option<i64>,
        #[arg(long)]
        supersedes: Option<i64>,
        #[arg(long)]
        revoke_judgment: Option<i64>,
        #[arg(long, requires = "grant_version")]
        grant: Option<String>,
        #[arg(long, requires = "grant")]
        grant_version: Option<i64>,
    },
    /// Writer: grant/revoke narrowly scoped authority to judge one milestone.
    Grant {
        id: String,
        #[arg(long)]
        version: i64,
        #[arg(long)]
        grant_version: Option<i64>,
        #[arg(long)]
        key: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        grant: String,
        #[arg(long)]
        decider: String,
        #[arg(long)]
        milestone: String,
        #[arg(long, required = true)]
        criterion: Vec<String>,
        #[arg(long)]
        authority_ref: String,
        #[arg(long)]
        revoke: bool,
    },
}
fn grouped(values: Vec<String>) -> Result<BTreeMap<String, Vec<String>>> {
    let mut result: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for value in values {
        let (id, item) = value.split_once('=').context("use MILESTONE=VALUE")?;
        ensure!(
            !id.is_empty() && !item.is_empty(),
            "milestone and value must be nonempty"
        );
        result.entry(id.into()).or_default().push(item.into());
    }
    Ok(result)
}
impl Progress {
    pub(super) fn prepare(self) -> Result<Operation> {
        Ok(match self {
            Self::History { id, after } => Operation::History(id, after),
            Self::Policy {
                id,
                version,
                progress_version,
                key,
                reason,
                max_segments,
                max_elapsed,
                milestone,
                milestone_criterion,
                milestone_scope,
            } => {
                let mut criteria = grouped(milestone_criterion)?;
                let mut scopes = grouped(milestone_scope)?;
                let mut milestones = Vec::new();
                for id in milestone {
                    ensure!(
                        !milestones.iter().any(|m: &owner::Milestone| m.id == id),
                        "duplicate milestone"
                    );
                    milestones.push(owner::Milestone {
                        criterion_ids: criteria
                            .remove(&id)
                            .context("milestone requires --milestone-criterion")?,
                        scope_units: scopes
                            .remove(&id)
                            .context("milestone requires --milestone-scope")?,
                        id,
                    });
                }
                ensure!(
                    criteria.is_empty() && scopes.is_empty(),
                    "milestone mappings require --milestone ID"
                );
                Operation::Policy(
                    id,
                    owner::PolicyChange {
                        key,
                        task_version: version,
                        expected_revision: progress_version,
                        reason,
                        policy: owner::ProgressPolicy {
                            max_segments_without_milestone: max_segments,
                            max_elapsed_without_milestone: max_elapsed.map(|v| v as u64),
                            milestones,
                        },
                    },
                )
            }
            Self::Judge {
                id,
                version,
                progress_version,
                key,
                reason,
                milestone,
                judgment,
                report,
                supersedes,
                revoke_judgment,
                grant,
                grant_version,
            } => {
                let change = match judgment {
                    Judgment::Qualify => {
                        ensure!(
                            revoke_judgment.is_none(),
                            "qualify does not accept --revoke-judgment"
                        );
                        owner::JudgmentChange::Qualify {
                            report: report.context("qualify requires --report")?,
                            supersedes,
                        }
                    }
                    Judgment::Reject => {
                        ensure!(
                            supersedes.is_none() && revoke_judgment.is_none(),
                            "reject takes only --report"
                        );
                        owner::JudgmentChange::Reject {
                            report: report.context("reject requires --report")?,
                        }
                    }
                    Judgment::Revoke => {
                        ensure!(
                            report.is_none() && supersedes.is_none(),
                            "revoke takes only --revoke-judgment"
                        );
                        owner::JudgmentChange::Revoke {
                            judgment: revoke_judgment
                                .context("revoke requires --revoke-judgment")?,
                        }
                    }
                };
                Operation::Judge(
                    id,
                    owner::JudgmentRequest {
                        key,
                        task_version: version,
                        progress_revision: progress_version,
                        milestone,
                        judge_grant: grant
                            .map(|id| {
                                Ok::<_, anyhow::Error>(task_graph::JudgeGrantRef {
                                    id,
                                    revision: grant_version.context("supply --grant-version")?,
                                })
                            })
                            .transpose()?,
                        reason,
                        change,
                    },
                )
            }
            Self::Grant {
                id,
                version,
                grant_version,
                key,
                reason,
                grant,
                decider,
                milestone,
                criterion,
                authority_ref,
                revoke,
            } => Operation::Grant(
                id,
                task_graph::JudgeGrantDecision {
                    key,
                    task_version: version,
                    expected_revision: grant_version,
                    reason,
                    grant: task_graph::JudgeGrant {
                        id: grant,
                        decider,
                        milestone,
                        criterion_ids: criterion,
                        authority_ref,
                        revoked: revoke,
                    },
                },
            ),
        })
    }
}
pub(crate) enum Operation {
    History(String, i64),
    Policy(String, owner::PolicyChange),
    Judge(String, owner::JudgmentRequest),
    Grant(String, task_graph::JudgeGrantDecision),
}
impl Operation {
    pub(crate) async fn run(self, store: &Store, actor: &Mailbox, time: i64) -> Result<Value> {
        Ok(match self {
            Self::History(id, after) => {
                let mut items = store.progress_history(actor, &id, after, 51).await?;
                let more = items.len() > 50;
                items.truncate(50);
                let next_after = items.last().and_then(|v| v["id"].as_i64()).unwrap_or(after);
                json!({"schema_version":1,"items":items,"more":more,"next_after":next_after})
            }
            Self::Policy(id, request) => {
                json!({"schema_version":1,"receipt":store.progress_policy(actor,&id,&request,time).await?})
            }
            Self::Judge(id, request) => {
                json!({"schema_version":1,"receipt":store.progress_judge(actor,&id,&request,time).await?})
            }
            Self::Grant(id, request) => {
                json!({"schema_version":1,"receipt":store.task_judge_grant(actor,&id,request,time).await?})
            }
        })
    }
}
