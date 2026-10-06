//! Bounded, process-local measurements of delivery transactions and runtime I/O.
//!
//! Cloned stores share these counters. They never write to SQLite or retain
//! recipient identities, and reset when the worker opens a new store.

use anyhow::Result;
use serde::Serialize;
use sqlx::{Sqlite, Transaction};
use std::{
    collections::BTreeMap,
    future::Future,
    ops::{Deref, DerefMut},
    sync::Mutex,
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Operation {
    HerdrDelivery,
    NativeDelivery,
    HerdrVerification,
    NativeVerification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    /// Includes pool acquisition, beginning the transaction and acquiring the writer lock.
    WriterAcquire,
    /// From acquisition through commit, or until an unfinished transaction is dropped.
    WriterHold,
    Transport,
}

#[derive(Debug, Default, Clone, Serialize)]
struct Summary {
    in_flight: u64,
    completed: u64,
    failed: u64,
    /// Dropped without a result, including cancellation and early error returns.
    aborted: u64,
    total_us: u64,
    max_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Measurement {
    operation: Operation,
    phase: Phase,
    #[serde(flatten)]
    summary: Summary,
}

#[derive(Debug, Default)]
pub(crate) struct Diagnostics(Mutex<BTreeMap<(Operation, Phase), Summary>>);

impl Diagnostics {
    fn summaries(&self) -> std::sync::MutexGuard<'_, BTreeMap<(Operation, Phase), Summary>> {
        // A diagnostic failure must not poison delivery or database cleanup.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn start(&self, operation: Operation, phase: Phase) -> Timer<'_> {
        let mut summaries = self.summaries();
        let summary = summaries.entry((operation, phase)).or_default();
        summary.in_flight = summary.in_flight.saturating_add(1);
        Timer {
            diagnostics: self,
            operation,
            phase,
            started: Instant::now(),
            succeeded: None,
        }
    }

    pub(crate) async fn measure<T>(
        &self,
        operation: Operation,
        phase: Phase,
        future: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let timer = self.start(operation, phase);
        let result = future.await;
        timer.finish(result.is_ok());
        result
    }

    pub(crate) fn snapshot(&self) -> Vec<Measurement> {
        self.summaries()
            .iter()
            .map(|(&(operation, phase), summary)| Measurement {
                operation,
                phase,
                summary: summary.clone(),
            })
            .collect()
    }
}

pub(crate) struct Timer<'a> {
    diagnostics: &'a Diagnostics,
    operation: Operation,
    phase: Phase,
    started: Instant,
    succeeded: Option<bool>,
}

impl Timer<'_> {
    fn finish(mut self, succeeded: bool) {
        self.succeeded = Some(succeeded);
    }
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        let elapsed = u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let mut summaries = self.diagnostics.summaries();
        let summary = summaries.entry((self.operation, self.phase)).or_default();
        summary.in_flight = summary.in_flight.saturating_sub(1);
        match self.succeeded {
            Some(true) => summary.completed = summary.completed.saturating_add(1),
            Some(false) => summary.failed = summary.failed.saturating_add(1),
            None => summary.aborted = summary.aborted.saturating_add(1),
        }
        summary.total_us = summary.total_us.saturating_add(elapsed);
        summary.max_us = summary.max_us.max(elapsed);
    }
}

/// Retain the existing binding lock and measure it until commit or abandonment.
pub(crate) struct DeliveryTransaction<'a> {
    pub(crate) transaction: Transaction<'a, Sqlite>,
    pub(crate) timer: Timer<'a>,
}

impl DeliveryTransaction<'_> {
    pub(crate) async fn commit(self) -> Result<()> {
        let Self { transaction, timer } = self;
        let result = transaction.commit().await;
        timer.finish(result.is_ok());
        Ok(result?)
    }
}

impl<'a> Deref for DeliveryTransaction<'a> {
    type Target = Transaction<'a, Sqlite>;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl DerefMut for DeliveryTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transaction
    }
}

/// Limit free-form diagnostic text without cutting a UTF-8 code point.
pub(crate) fn error_text(mut text: String) -> String {
    const LIMIT: usize = 1024;
    if text.len() > LIMIT {
        let mut end = LIMIT - 3;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("...");
    }
    text
}

#[cfg(test)]
mod tests {
    #[test]
    fn diagnostic_error_limits_preserve_unicode_and_short_errors() {
        let short = "failed: café";
        assert_eq!(super::error_text(short.into()), short);
        let long = super::error_text("界".repeat(1000));
        assert!(long.len() <= 1024);
        assert!(long.ends_with("..."));
        assert!(long[..long.len() - 3].chars().all(|c| c == '界'));
    }
}
