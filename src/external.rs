//! Bounded observations of declared GitHub pull-request merge conditions.
//! Facts are persisted across worker restarts; query failures never qualify a wait.
use crate::store::Store;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// A validated GitHub owner/repository identity, normalized for durable lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct GitHubRepository(String);
impl GitHubRepository {
    /// Validate a GitHub repository identity.
    /// # Errors
    /// The value is not two bounded ASCII name components separated by a slash.
    pub fn new(value: &str) -> Result<Self> {
        let parts: Vec<_> = value.split('/').collect();
        ensure!(
            parts.len() == 2
                && parts.iter().all(|p| !p.is_empty()
                    && p.len() <= 100
                    && *p != "."
                    && *p != ".."
                    && !p.starts_with('-')
                    && p.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))),
            "expected a GitHub owner/repository"
        );
        Ok(Self(value.to_ascii_lowercase()))
    }
    /// Canonical repository identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for GitHubRepository {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(&value)
    }
}
impl From<GitHubRepository> for String {
    fn from(value: GitHubRepository) -> Self {
        value.0
    }
}

/// Full Git object identity; abbreviated or symbolic revisions cannot qualify a wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CommitId(String);
impl CommitId {
    /// Validate a full SHA-1 or SHA-256 object identity.
    /// # Errors
    /// The value is not 40 or 64 hexadecimal characters.
    pub fn new(value: &str) -> Result<Self> {
        ensure!(
            matches!(value.len(), 40 | 64) && value.bytes().all(|c| c.is_ascii_hexdigit()),
            "expected a full Git object identity"
        );
        Ok(Self(value.to_ascii_lowercase()))
    }
    /// Canonical hexadecimal object identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for CommitId {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(&value)
    }
}
impl From<CommitId> for String {
    fn from(value: CommitId) -> Self {
        value.0
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum PullRequestState {
    Open,
    Closed,
    Merged,
}
impl PullRequestState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closed => "closed",
            Self::Merged => "merged",
        }
    }
}
#[derive(Debug, Deserialize)]
struct MergeCommit {
    oid: CommitId,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullRequestFact {
    number: i64,
    state: PullRequestState,
    head_ref_oid: CommitId,
    merge_commit: Option<MergeCommit>,
}
fn parse_fact(bytes: &[u8], number: i64) -> Result<PullRequestFact> {
    ensure!(bytes.len() <= 4096, "GitHub fact exceeded output limit");
    let fact: PullRequestFact = serde_json::from_slice(bytes)?;
    ensure!(
        fact.number == number,
        "GitHub returned a different pull request"
    );
    ensure!(
        !matches!(fact.state, PullRequestState::Merged) || fact.merge_commit.is_some(),
        "merged pull request lacks a merge commit"
    );
    Ok(fact)
}
async fn read_github(repository: &GitHubRepository, number: i64) -> Result<PullRequestFact> {
    let mut child = tokio::process::Command::new("gh")
        .args([
            "pr",
            "view",
            &number.to_string(),
            "--repo",
            &format!("github.com/{}", repository.as_str()),
            "--json",
            "number,state,headRefOid,mergeCommit",
        ])
        .env("GH_PROMPT_DISABLED", "1")
        .env("GH_HOST", "github.com")
        .env_remove("GH_DEBUG")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("GitHub adapter requires gh and an authenticated account")?;
    let mut stdout = child
        .stdout
        .take()
        .context("missing GitHub output")?
        .take(4097);
    let mut stderr = child
        .stderr
        .take()
        .context("missing GitHub error output")?
        .take(4097);
    let mut out = Vec::new();
    let mut err = Vec::new();
    let (status, _, _) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::try_join!(
            child.wait(),
            stdout.read_to_end(&mut out),
            stderr.read_to_end(&mut err)
        )
    })
    .await
    .context("GitHub observation timed out")??;
    ensure!(
        status.success(),
        "GitHub observation failed ({status}); check gh authentication and repository access"
    );
    parse_fact(&out, number)
}

async fn persist(
    store: &Store,
    repository: &GitHubRepository,
    number: i64,
    time: i64,
    fact: Result<PullRequestFact>,
) -> Result<()> {
    let (state, head, merge, error) = match fact {
        Ok(f) => (
            Some(f.state.as_str()),
            Some(f.head_ref_oid),
            f.merge_commit.map(|m| m.oid),
            None,
        ),
        Err(e) => (
            None,
            None,
            None,
            Some(format!("{e:#}").chars().take(512).collect::<String>()),
        ),
    };
    sqlx::query("INSERT INTO external_pr_facts(repository,number,state,head,merge_commit,checked_at,error) VALUES(?,?,?,?,?,?,?) ON CONFLICT(repository,number) DO UPDATE SET state=excluded.state,head=excluded.head,merge_commit=excluded.merge_commit,checked_at=excluded.checked_at,error=excluded.error")
        .bind(repository.as_str()).bind(number).bind(state).bind(head.as_ref().map(CommitId::as_str)).bind(merge.as_ref().map(CommitId::as_str)).bind(time).bind(error).execute(store.pool()).await?;
    crate::stream::hint(store.root()).await;
    Ok(())
}

/// Refresh a fair bounded page of declared merge conditions, independently of delivery.
/// # Errors
/// Reading declarations or persisting observations fails. Adapter errors are saved as facts.
pub async fn refresh(store: &Store, time: i64) -> Result<()> {
    let rows=sqlx::query("SELECT DISTINCT json_extract(f.checkpoint,'$.waiting.repository') AS repository,json_extract(f.checkpoint,'$.waiting.number') AS number,COALESCE(x.checked_at,0) AS checked FROM active_followups f JOIN followup_policy p ON p.group_name=f.group_name JOIN groups g ON g.name=f.group_name LEFT JOIN external_pr_facts x ON x.repository=json_extract(f.checkpoint,'$.waiting.repository') AND x.number=json_extract(f.checkpoint,'$.waiting.number') WHERE json_extract(f.checkpoint,'$.waiting.kind')='pull_request' AND p.mode='enabled' AND g.paused=0 AND (x.checked_at IS NULL OR x.checked_at<=?) ORDER BY checked,repository,number LIMIT 2")
        .bind(time.saturating_sub(120)).fetch_all(store.pool()).await?;
    for row in rows {
        let repository = GitHubRepository::new(&row.get::<String, _>("repository"))?;
        let number: i64 = row.get("number");
        persist(
            store,
            &repository,
            number,
            time,
            read_github(&repository, number).await,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn condition_status(
    store: &Store,
    wait: &crate::followup::WaitFor,
) -> Result<serde_json::Value> {
    use crate::followup::WaitFor;
    use serde_json::json;
    match wait {
        WaitFor::External {
            responsible,
            reason,
        } => Ok(
            json!({"supported":false,"responsible":responsible,"reason":reason,"supervision":"manual"}),
        ),
        WaitFor::PullRequest {
            repository,
            number,
            head,
        } => {
            let row=sqlx::query("SELECT state,head,merge_commit,checked_at,error FROM external_pr_facts WHERE repository=? AND number=?")
                .bind(repository.as_str()).bind(number).fetch_optional(store.pool()).await?;
            Ok(match row {
                Some(row) => {
                    json!({"supported":true,"provider":"github","repository":repository,"number":number,"expected_head":head,"state":row.get::<Option<String>,_>("state"),"observed_head":row.get::<Option<String>,_>("head"),"merge_commit":row.get::<Option<String>,_>("merge_commit"),"checked_at":row.get::<i64,_>("checked_at"),"error":row.get::<Option<String>,_>("error")})
                }
                None => {
                    json!({"supported":true,"provider":"github","repository":repository,"number":number,"expected_head":head,"state":"unobserved"})
                }
            })
        }
        _ => Ok(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authoritative_facts_require_identity_head_and_merge_evidence() -> Result<()> {
        let head = "a".repeat(40);
        let merge = "b".repeat(40);
        let good = serde_json::json!({"number":7,"state":"MERGED","headRefOid":head,"mergeCommit":{"oid":merge}});
        assert!(parse_fact(&serde_json::to_vec(&good)?, 7).is_ok());
        assert!(parse_fact(&serde_json::to_vec(&good)?, 8).is_err());
        let mut bad = good.clone();
        bad["mergeCommit"] = serde_json::Value::Null;
        assert!(parse_fact(&serde_json::to_vec(&bad)?, 7).is_err());
        assert!(CommitId::new("main").is_err());
        assert!(GitHubRepository::new("owner/repo; echo injected").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn persisted_merge_conditions_require_the_expected_head_and_recheck_before_receipt()
    -> Result<()> {
        use crate::{
            followup::{self, Checkpoint, Source, WaitFor},
            names::DeliveryConsumer,
            states::{AttentionReason, TaskState},
            work::WorkDraft,
        };
        let dir = tempfile::tempdir()?;
        let store = Store::open(dir.path(), true).await?;
        store.enroll("g", None).await?;
        store.register("g", "writer", false).await?;
        store.register("g", "owner", false).await?;
        let writer = store.mailbox("g", "writer").await?;
        let owner = store.mailbox("g", "owner").await?;
        store
            .work_create(
                &writer,
                WorkDraft {
                    id: "dependent".into(),
                    scope: "Wait for merge".into(),
                    owner: "owner".into(),
                    state: TaskState::Active,
                    next_action: "Wait".into(),
                    deadline: None,
                    evidence: vec![],
                },
                100,
            )
            .await?;
        let repository = GitHubRepository::new("Owner/Repo")?;
        let expected = "a".repeat(40);
        store
            .checkpoint(
                &owner,
                Source::Task {
                    id: "dependent".into(),
                    version: 1,
                },
                "merge",
                Checkpoint {
                    version: 0,
                    next_step: "Rebase".into(),
                    next_check_at: 200,
                    waiting: Some(WaitFor::PullRequest {
                        repository: repository.clone(),
                        number: 7,
                        head: CommitId::new(&expected)?,
                    }),
                    evidence: vec![],
                    extend_until: None,
                    reason: None,
                },
                100,
            )
            .await?;
        let make_fact = |head: &str| -> Result<PullRequestFact> {
            parse_fact(
                &serde_json::to_vec(
                    &serde_json::json!({"number":7,"state":"MERGED","headRefOid":head,"mergeCommit":{"oid":"b".repeat(40)}}),
                )?,
                7,
            )
        };
        persist(&store, &repository, 7, 101, make_fact(&"c".repeat(40))).await?;
        followup::reconcile(&store, 101).await?;
        assert!(store.attention_snapshot(&owner).await?.items.is_empty());
        persist(&store, &repository, 7, 102, make_fact(&expected)).await?;
        followup::reconcile(&store, 102).await?;
        let batch = store
            .claim_attention(&owner, DeliveryConsumer::Native, 102)
            .await?
            .unwrap();
        assert_eq!(
            batch.attention.items[0].reason,
            AttentionReason::DependencyReady
        );
        persist(
            &store,
            &repository,
            7,
            103,
            Err(anyhow::anyhow!("adapter unavailable")),
        )
        .await?;
        assert!(
            store
                .acknowledge_attention(&owner, &batch.token)
                .await
                .is_err()
        );
        assert!(
            store.attention_snapshot(&owner).await?.items.is_empty(),
            "failed authoritative observations cannot qualify a wait"
        );
        let status = store.followup_status(Some("g"), 103).await?;
        assert_eq!(
            status["items"][0]["external_condition"]["error"],
            "adapter unavailable"
        );
        store.close().await;
        let recovered = Store::open(dir.path(), false).await?;
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT error FROM external_pr_facts")
                .fetch_one(recovered.pool())
                .await?,
            "adapter unavailable"
        );
        Ok(())
    }
}
