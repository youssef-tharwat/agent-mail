//! Integration tests for artifact manifests, content digests, and bounded staged text results.

use agent_mail::runtime_effects::{
    ArtifactFile, ArtifactManifest, ContentDigest, FILE_BYTES_LIMIT,
};

fn manifest(paths: &[&str]) -> ArtifactManifest {
    ArtifactManifest {
        version: 1,
        files: paths
            .iter()
            .map(|path| ArtifactFile {
                path: (*path).to_owned(),
                digest: ContentDigest::of_bytes(b"content"),
                bytes: 7,
            })
            .collect(),
    }
}

#[test]
fn artifact_paths_cannot_alias_or_escape_the_virtual_tree() {
    for path in [
        "",
        "/root",
        "../escape",
        "a/../escape",
        "a/./b",
        "a//b",
        "a/",
        "C:drive",
        "a\\b",
        "a\nb",
    ] {
        assert!(
            manifest(&[path]).canonical_bytes().is_err(),
            "accepted {path:?}"
        );
    }
    for paths in [vec!["a", "a"], vec!["a", "a/b"], vec!["a/b/c", "a/b"]] {
        assert!(manifest(&paths).canonical_bytes().is_err());
    }
    assert!(
        manifest(&["a.txt", "nested/a.txt"])
            .canonical_bytes()
            .is_ok()
    );
}

#[test]
fn canonical_identity_ignores_order_but_preserves_content_and_length() {
    let bytes = manifest(&["z", "a"]).canonical_bytes().unwrap();
    assert_eq!(bytes, manifest(&["a", "z"]).canonical_bytes().unwrap());
    let mut altered = manifest(&["a", "z"]);
    altered.files[0].bytes += 1;
    assert_ne!(
        ContentDigest::of_bytes(&bytes),
        ContentDigest::of_bytes(&altered.canonical_bytes().unwrap())
    );
    assert_eq!(ArtifactManifest::decode(&bytes).unwrap().files[0].path, "a");
}

#[test]
fn manifest_and_digest_bounds_reject_unsupported_inputs() {
    assert!(ContentDigest::parse("F".repeat(64)).is_err());
    assert!(ContentDigest::parse("0".repeat(63)).is_err());
    assert_eq!(
        ContentDigest::of_bytes(b"abc").as_str(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    let mut item = manifest(&["large"]);
    item.files[0].bytes = FILE_BYTES_LIMIT + 1;
    assert!(item.canonical_bytes().is_err());
    item.files[0].bytes = 0;
    item.version = 2;
    assert!(item.canonical_bytes().is_err());
    assert!(ArtifactManifest::decode(br#"{"version":1,"files":[],"authorized":true}"#).is_err());
}

fn text_result() -> serde_json::Value {
    serde_json::json!({"kind":"yield","schema_version":1,"binding":"result-tree",
        "summary":"Partial result retained","next_step":"Finish the remaining section",
        "review_after_seconds":30,"files":[{"path":"report.txt","text":"partial"}]})
}

#[test]
fn managed_text_parser_rejects_duplicate_unknown_and_authority_fields() {
    use agent_mail::runtime_effects::ManagedTextResult;
    let valid = serde_json::to_string(&text_result()).unwrap();
    assert!(ManagedTextResult::decode(valid.as_bytes()).is_ok());
    for invalid in [
        valid.replacen(
            "\"kind\":\"yield\"",
            "\"kind\":\"yield\",\"kind\":\"artifact\"",
            1,
        ),
        valid.replacen(
            "\"text\":\"partial\"",
            "\"text\":\"partial\",\"text\":\"changed\"",
            1,
        ),
        valid.replacen('{', "{\"authority\":true,", 1),
        format!("{valid}{valid}"),
        format!("```json\n{valid}\n```"),
    ] {
        assert!(
            ManagedTextResult::decode(invalid.as_bytes()).is_err(),
            "{invalid}"
        );
    }
    for interval in [0, 3601] {
        let mut invalid = text_result();
        invalid["review_after_seconds"] = interval.into();
        assert!(ManagedTextResult::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    for interval in [1, 30, 3600] {
        let mut valid = text_result();
        valid["review_after_seconds"] = interval.into();
        assert!(ManagedTextResult::decode(&serde_json::to_vec(&valid).unwrap()).is_ok());
    }
}

#[test]
fn managed_text_profile_checks_utf8_bytes_and_total_before_publication() {
    use agent_mail::runtime_effects::{ManagedTextResult, TEXT_RESULT_LIMIT};
    let mut value = text_result();
    value["files"][0]["text"] = "é".repeat(2048).into();
    assert!(ManagedTextResult::decode(&serde_json::to_vec(&value).unwrap()).is_ok());
    value["files"][0]["text"] = "é".repeat(2049).into();
    assert!(ManagedTextResult::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    value["files"] = serde_json::json!([
        {"path":"a","text":"a".repeat(4096)},
        {"path":"b","text":"b".repeat(4096)},
        {"path":"c","text":"c".repeat(4096)}]);
    assert!(ManagedTextResult::decode(&serde_json::to_vec(&value).unwrap()).is_ok());
    value["files"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"path":"d","text":"d"}));
    assert!(ManagedTextResult::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut bytes = serde_json::to_vec(&text_result()).unwrap();
    bytes.resize(TEXT_RESULT_LIMIT + 1, b' ');
    assert!(ManagedTextResult::decode(&bytes).is_err());
}

#[test]
fn text_manifest_uses_actual_bytes_and_complete_object_set() {
    use agent_mail::runtime_effects::{ManagedTextObject, ManagedTextResult};
    let mut value = text_result();
    value["files"] = serde_json::json!([
        {"path":"z.txt","text":"identical"},{"path":"a.txt","text":"identical"}]);
    let (manifest, contents) = ManagedTextResult::decode(&serde_json::to_vec(&value).unwrap())
        .unwrap()
        .artifact()
        .unwrap();
    assert_eq!(manifest.files[0].path, "a.txt");
    assert_eq!(manifest.files[0].bytes, 9);
    assert_eq!(contents.objects.len(), 1);
    assert_eq!(
        manifest.files[0].digest,
        ContentDigest::of_bytes(b"identical")
    );
    let mut missing = contents.clone();
    missing.objects.clear();
    assert!(missing.validate(&manifest).is_err());
    let mut corrupt = contents.clone();
    corrupt.objects[0].text = "different".into();
    assert!(corrupt.validate(&manifest).is_err());
    let mut extra = contents.clone();
    extra.objects.push(ManagedTextObject {
        digest: ContentDigest::of_bytes(b"extra"),
        text: "extra".into(),
    });
    assert!(extra.validate(&manifest).is_err());
    let mut conflicting = manifest.clone();
    conflicting.files[1].bytes += 1;
    assert!(contents.validate(&conflicting).is_err());
}

#[test]
fn text_producer_rejects_aliases_and_distinguishes_partial_and_final_bytes() {
    use agent_mail::runtime_effects::ManagedTextResult;
    for paths in [["a", "a"], ["a", "a/b"], ["../escape", "valid"]] {
        let mut invalid = text_result();
        invalid["files"] = serde_json::json!([
            {"path":paths[0],"text":"x"},{"path":paths[1],"text":"y"}]);
        assert!(ManagedTextResult::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    let partial = ManagedTextResult::decode(&serde_json::to_vec(&text_result()).unwrap())
        .unwrap()
        .artifact()
        .unwrap()
        .0;
    let final_value = serde_json::json!({"kind":"artifact","schema_version":1,"binding":"result-tree",
        "summary":"Finished report","files":[{"path":"report.txt","text":"finished"}]});
    let final_manifest = ManagedTextResult::decode(&serde_json::to_vec(&final_value).unwrap())
        .unwrap()
        .artifact()
        .unwrap()
        .0;
    assert_ne!(
        ContentDigest::of_bytes(&partial.canonical_bytes().unwrap()),
        ContentDigest::of_bytes(&final_manifest.canonical_bytes().unwrap())
    );
}
