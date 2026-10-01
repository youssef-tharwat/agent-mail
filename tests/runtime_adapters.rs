//! Integration tests for artifact isolation and capability identity and freshness.

use agent_mail::{
    managed_runtime::NativeClient,
    runtime_adapter::{
        CapabilityObservation, CapabilityState, ManagedTargetSpec, RuntimeCapabilities,
        RuntimeCapability, RuntimeProfile,
    },
    runtime_effects::ContentDigest,
};
use std::path::PathBuf;

#[test]
fn artifact_storage_must_be_separate_from_the_child_workspace() {
    let mut spec = ManagedTargetSpec {
        target: "managed".into(),
        owner: "worker".into(),
        client: NativeClient::Codex,
        cwd: PathBuf::from("/work/task"),
        profile: RuntimeProfile::StagedFiles,
        artifact_root: Some(PathBuf::from("/private/artifacts")),
        configuration: "approved-config".into(),
    };
    assert!(spec.validate().is_ok());
    for root in [
        "/work",
        "/work/task",
        "/work/task/artifacts",
        "/private/../work/task",
    ] {
        spec.artifact_root = Some(PathBuf::from(root));
        assert!(spec.validate().is_err(), "accepted {root}");
    }
    spec.artifact_root = None;
    assert!(spec.validate().is_err());
    spec.profile = RuntimeProfile::ReadOnly;
    assert!(spec.validate().is_ok());
}

#[test]
fn capability_projection_freshness_is_bound_to_exact_identity_and_interval() {
    let policy = ContentDigest::of_bytes(b"policy");
    let executable = ContentDigest::of_bytes(b"binary");
    let mut capabilities = RuntimeCapabilities {
        target_version: 2,
        policy_digest: policy.clone(),
        executable_digest: executable.clone(),
        observed_at: 10,
        valid_until: 20,
        observations: vec![CapabilityObservation {
            capability: RuntimeCapability::Quiescence,
            state: CapabilityState::Unverified {
                reason: "no runtime witness".into(),
            },
        }],
    };
    assert!(capabilities.matches_current(2, &policy, &executable, 10));
    // A fresh diagnostic can honestly say unverified. Freshness is not a positive capability.
    assert!(matches!(
        capabilities.observations[0].state,
        CapabilityState::Unverified { .. }
    ));
    assert!(!capabilities.matches_current(3, &policy, &executable, 10));
    assert!(!capabilities.matches_current(
        2,
        &ContentDigest::of_bytes(b"changed"),
        &executable,
        10
    ));
    assert!(!capabilities.matches_current(2, &policy, &executable, 9));
    assert!(!capabilities.matches_current(2, &policy, &executable, 20));
    capabilities
        .observations
        .push(capabilities.observations[0].clone());
    assert!(capabilities.validate().is_err());
    assert!(!capabilities.matches_current(2, &policy, &executable, 10));
}
