//! Closed vocabularies shared by storage, JSON and command parsing.
use serde::{Deserialize, Serialize};
// One declaration supplies the wire spelling, parsing and display for every variant.
macro_rules! string_enum {
    ($(#[$meta:meta])* pub enum $name:ident { $($(#[$doc:meta])* $variant:ident => $wire:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
        pub enum $name { $($(#[$doc])* #[serde(rename=$wire)] #[sqlx(rename=$wire)] $variant),+ }
        impl $name {
            /// Stable storage and wire value.
            pub const fn as_str(self) -> &'static str { match self { $(Self::$variant => $wire),+ } }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.as_str()) }
        }
        impl std::str::FromStr for $name {
            type Err = anyhow::Error;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value { $($wire => Ok(Self::$variant),)+ _ => anyhow::bail!("invalid {} {value:?}; expected one of: {}", stringify!($name), [$($wire),+].join(", ")) }
            }
        }
    };
}
string_enum! {
    /// Durable agent registration, independent of runtime liveness.
    #[derive(clap::ValueEnum)]
    pub enum AgentState {
        /// May own tasks and exchange messages.
        Registered => "registered",
        /// Explicitly withdrawn from coordination.
        Retired => "retired",
    }
}
string_enum! {
    /// Current delivery capability and evidence; never a business disposition.
    pub enum DeliveryReadiness {
        /// Diagnostics failed; no readiness claim can be made.
        Unknown => "unknown",
        /// Explicit agent acknowledgment and a currently healthy route.
        Verified => "verified",
        /// A challenge is pending acknowledgment.
        Verifying => "verifying",
        /// No challenge has run for this route.
        Unverified => "unverified",
        /// The bounded challenge cycle expired.
        Expired => "expired",
        /// No supported endpoint is attached.
        MissingEndpoint => "missing_endpoint",
        /// The local worker is not running.
        WorkerStopped => "worker_stopped",
        /// Delivery is explicitly paused.
        Paused => "paused",
        /// Herdr is restricted to operator notifications.
        NotifyOnly => "notify_only",
        /// The registration is retired.
        Retired => "retired",
        /// Current endpoint or health evidence is unavailable.
        Unavailable => "unavailable",
        /// Remote verification is outside the local protocol.
        RemoteUnsupported => "remote_unsupported",
    }
}
string_enum! {
    /// Task lifecycle; the designated writer chooses transitions.
    #[derive(clap::ValueEnum)]
    pub enum TaskState {
        /// Captured work.
        Open => "open",
        /// Selected and ready to begin.
        Ready => "ready",
        /// Work is underway.
        Active => "active",
        /// A blocker prevents progress.
        Blocked => "blocked",
        /// Awaiting review.
        Review => "review",
        /// Finished without claiming workflow acceptance.
        Done => "done",
        /// Explicitly accepted by the writer.
        Accepted => "accepted",
        /// Explicitly cancelled by the writer.
        Cancelled => "cancelled",
    }
}
string_enum! {
    /// Explicit communication effect, independent of delivery or task acceptance.
    #[derive(clap::ValueEnum,Default)]
    pub enum MessageIntent {
        /// A recipient owes a business disposition.
        #[default]
        Request => "request",
        /// Information available without a response obligation or interruption.
        Notice => "notice",
        /// A final answer to an existing request; no reciprocal request is created.
        Response => "response",
    }
}
impl MessageIntent {
    /// Preserve the canonical JSON of requests published before intent existed.
    pub fn is_request(&self) -> bool {
        *self == Self::Request
    }
}
string_enum! {
    /// Recipient disposition, independent of transport receipt.
    pub enum MessageState {
        /// Action remains owed.
        Pending => "pending",
        /// Recipient resolved the request.
        Resolved => "resolved",
        /// Sender withdrew the request.
        Withdrawn => "withdrawn",
    }
}
string_enum! {
    /// Durable coordination event category.
    pub enum EventKind {
        /// New pending request.
        MailPending => "mail_pending",
        /// Request disposition changed.
        MailChanged => "mail_changed",
        /// Task changed.
        WorkChanged => "work_changed",
        /// A scheduled attention occurrence, not a business state change.
        AttentionDue => "attention_due",
    }
}
string_enum! {
    /// Current reason to reconsider a source, separate from its business state.
    pub enum AttentionReason {
        /// The current assignment was closed or transferred.
        StopWork => "stop_work",
        /// A request has not been observed in this binding.
        UnreadRequest => "unread_request",
        /// A final response is available for inspection.
        ResponseAvailable => "response_available",
        /// A current assignment revision changed.
        AssignmentChanged => "assignment_changed",
        /// A declared prerequisite qualified.
        DependencyReady => "dependency_ready",
        /// The decision authority must review overdue work.
        ReviewDue => "review_due",
        /// A child's material progress asks its parent's writer to reassess.
        SubtaskChanged => "subtask_changed",
        /// A bounded follow-up is due.
        ReminderDue => "reminder_due",
    }
}
string_enum! {
    /// Native Claude inbox lifecycle.
    pub enum InboxActivity {
        /// Ready for input.
        Idle => "idle",
        /// A turn is running.
        Active => "active",
        /// The session ended.
        Ended => "ended",
    }
}
string_enum! {
    /// Registration association, independent of liveness.
    pub enum BindingKind {
        /// Verified pane binding.
        Herdr => "herdr",
        /// Local credential binding.
        Standalone => "standalone",
        /// Address hosted on another machine.
        Remote => "remote",
    }
}
string_enum! {
    /// Observed registration availability.
    pub enum Availability {
        /// Registration provides no liveness evidence.
        Unknown => "unknown",
    }
}
string_enum! {
    /// Supported native identity format.
    pub enum SessionKind {
        /// Native session identifier.
        Id => "id",
        /// Native session file path.
        Path => "path",
    }
}
string_enum! {
    /// Client with managed lifecycle hooks.
    #[derive(clap::ValueEnum)]
    pub enum NativeRuntime {
        /// Codex client.
        Codex => "codex",
        /// Claude client.
        Claude => "claude",
    }
}
string_enum! {
    /// Hook execution evidence, not model consumption.
    pub enum RecoveryState {
        /// No current launch evidence.
        NotObserved => "not_observed",
        /// Configured but no hook ran.
        AwaitingHook => "awaiting_hook",
        /// Current-session hook executed.
        HookObserved => "hook_observed",
    }
}
impl TaskState {
    /// Whether the assignment remains actionable; never independently writable.
    pub const fn is_open(self) -> bool {
        match self {
            Self::Open | Self::Ready | Self::Active | Self::Blocked | Self::Review => true,
            Self::Done | Self::Accepted | Self::Cancelled => false,
        }
    }
}

string_enum! {
    /// Client lifecycle boundary reported by a trusted hook.
    pub enum HookEvent {
        /// SessionStart lifecycle event.
        SessionStart => "SessionStart",
        /// SessionEnd lifecycle event.
        SessionEnd => "SessionEnd",
        /// UserPromptSubmit lifecycle event.
        UserPromptSubmit => "UserPromptSubmit",
        /// PreToolUse lifecycle event.
        PreToolUse => "PreToolUse",
        /// PostToolUse lifecycle event.
        PostToolUse => "PostToolUse",
        /// PostCompact lifecycle event.
        PostCompact => "PostCompact",
        /// Stop lifecycle event.
        Stop => "Stop",
        /// StopFailure lifecycle event.
        StopFailure => "StopFailure",
    }
}

string_enum! {
    /// Delivery observation, independent of task and mail resolution.
    pub enum DeliveryState {
        /// Delivery disabled or group paused.
        Paused => "paused",
        /// Herdr plugin disabled or unlinked.
        PluginDisabled => "plugin_disabled",
        /// Automatic prompts require explicit opt in.
        PromptDisabled => "prompt_disabled",
        /// Endpoint binding changed; reattach explicitly.
        BindingChanged => "binding_changed",
        /// Live identity differs from its binding.
        BindingMismatch => "binding_mismatch",
        /// All transport notifications are settled.
        Settled => "settled",
        /// Changes are retained for the next recovery.
        Passive => "passive",
        /// The bounded retry budget is exhausted.
        Exhausted => "exhausted",
        /// Waiting for the next retry deadline.
        Waiting => "waiting",
        /// Runtime cannot currently accept input.
        Busy => "busy",
        /// Reservation is no longer eligible.
        Ineligible => "ineligible",
        /// Submitted; awaiting a correlated lifecycle receipt.
        AwaitingReceipt => "awaiting_receipt",
        /// Runtime accepted the update; business state is unchanged.
        Queued => "queued",
        /// Delivery failed with an uncertain receipt.
        Uncertain => "uncertain",
        /// Delivery timed out; receipt is unknown.
        TimedOut => "timed_out",
        /// Runtime availability is unknown.
        Unavailable => "unavailable",
        /// Outstanding work is overdue.
        Overdue => "overdue",
        /// Runtime changed before delivery.
        StateChanged => "state_changed",
        /// Delivery scan failed; obligations remain stored.
        Held => "held",
        /// Operator notification failed.
        NotificationFailed => "notification_failed",
    }
}
