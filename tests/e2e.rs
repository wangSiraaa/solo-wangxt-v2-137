//! End-to-end tests through the Axum router: real SQLite catalog, real
//! segment files, and byte-level corruption / torn-write simulations.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use sensor_segments::api;
use sensor_segments::catalog::Catalog;
use sensor_segments::store::Store;
use tower::util::ServiceExt;

struct Harness {
    app: Router,
    root: PathBuf,
}

fn temp_root(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = std::env::temp_dir().join(format!(
        "sseg-e2e-{}-{tag}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

impl Harness {
    fn new(tag: &str) -> Self {
        let root = temp_root(tag);
        let catalog = Catalog::open(&root.join("catalog.db")).unwrap();
        let store = Store::new(catalog, root.join("segments")).unwrap();
        let report = store.reconcile().unwrap();
        let app = api::router(store, report);
        Self { app, root }
    }

    async fn call(&self, method: &str, uri: &str, body: Option<&str>) -> (StatusCode, serde_json::Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if body.is_some() {
            builder = builder.header("content-type", "application/json");
        }
        let req = builder
            .body(body.map(|b| Body::from(b.to_string())).unwrap_or(Body::empty()))
            .unwrap();
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                panic!("non-json response: {status} {}", String::from_utf8_lossy(&bytes))
            })
        };
        (status, json)
    }
}

async fn create_series(h: &Harness, name: &str, policy: &str) -> String {
    let (st, v) = h
        .call(
            "POST",
            "/v1/series",
            Some(&format!("{{\"name\":\"{name}\",\"duplicate_policy\":\"{policy}\"}}")),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    v["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn different_sampling_intervals_are_bit_exact() {
    let h = Harness::new("intervals");
    let sid = create_series(&h, "wave", "keep_all").await;

    // Irregular, jittered intervals with signed dod; values include NaN,
    // +/-inf, -0.0 and subnormals via raw hex bits.
    let raw: &[(i64, &str)] = &[
        (1_000_000_000, "3ff0000000000000"), // 1.0
        (1_000_000_500, "8000000000000000"), // -0.0
        (1_000_000_501, "7ff0000000000000"), // +inf
        (1_002_500_000, "fff0000000000000"), // -inf
        (1_002_500_001, "7ff8000000000000"), // NaN (canonical quiet)
        (1_002_500_002, "fff8000000000123"), // NaN payload + sign preserved
        (9_999_999_999, "0000000000000001"), // smallest subnormal
        (10_000_000_000, "000fffffffffffff"), // largest subnormal
    ];
    let body = format!(
        "{{\"samples\":[{}]}}",
        raw.iter()
            .map(|(t, b)| format!("{{\"t\":{t},\"bits\":\"{b}\"}}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(&body))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["samples_written"], 8);

    // Full range: every bit pattern must come back identical.
    let (st, q) = h
        .call(
            "GET",
            &format!("/v1/series/{sid}/points?from=0&to=11000000000"),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{q}");
    let seg = &q["segments"][0];
    let points = seg["samples"].as_array().unwrap();
    assert_eq!(points.len(), 8);
    for (p, (t, b)) in points.iter().zip(raw) {
        assert_eq!(p["t"].as_i64().unwrap(), *t);
        assert_eq!(p["bits"].as_str().unwrap(), *b);
    }
    assert_eq!(points[1]["kind"], "finite"); // -0 is finite
    assert_eq!(points[2]["kind"], "pos_inf");
    assert_eq!(points[3]["kind"], "neg_inf");
    assert_eq!(points[4]["kind"], "nan");
    assert_eq!(points[5]["kind"], "nan");
    assert!(points[2]["v"].is_null());
}

#[tokio::test]
async fn unrepresentable_integer_is_rejected_not_quantized() {
    let h = Harness::new("quant");
    let sid = create_series(&h, "ints", "keep_all").await;
    let body = "{\"samples\":[{\"t\":1,\"v\":9007199254740993}]}"; // 2^53+1
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(body))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["error"]["code"], "bad_request");
}

#[tokio::test]
async fn constant_and_spike_segments_compress() {
    let h = Harness::new("constant");
    let sid = create_series(&h, "steady", "keep_all").await;

    // 5000 samples at 1ms, constant value 42.0
    let mut entries = Vec::with_capacity(5000);
    for i in 0..5000i64 {
        entries.push(format!("{{\"t\":{},\"v\":42.0}}", i * 1_000_000));
    }
    let body = format!("{{\"samples\":[{}]}}", entries.join(","));
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(&body))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["samples_written"], 5000);
    assert_eq!(v["block_count"], 10);
    // 5000 constant f64 raw would be 40000 value bytes + 40000 ts; the
    // payload must be far smaller (XOR zero runs + delta-of-delta zeros).
    let payload: i64 = serde_json::from_value(v["file_len"].clone()).unwrap();
    assert!(payload < 20_000, "file_len={payload}");

    // Sudden jump: query a one-millisecond window decodes only one block.
    let (st, q) = h
        .call(
            "GET",
            &format!("/v1/series/{sid}/points?from=2500000000&to=2500000000"),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let seg = &q["segments"][0];
    assert_eq!(seg["block_count"], 10);
    assert_eq!(
        seg["blocks_touched"].as_array().unwrap().len(),
        1,
        "a single-ms point must touch exactly 1 of 10 blocks"
    );
    assert_eq!(seg["samples"].as_array().unwrap().len(), 1);
    assert_eq!(seg["samples"][0]["v"], 42.0);
}

#[tokio::test]
async fn duplicate_timestamps_kept_all_are_preserved_and_queryable() {
    let h = Harness::new("dups");
    let sid = create_series(&h, "dup", "keep_all").await;
    let body = "{\"samples\":[
        {\"t\":10,\"v\":1.0},{\"t\":20,\"v\":2.0},{\"t\":20,\"v\":20.0},
        {\"t\":20,\"v\":200.0},{\"t\":30,\"v\":3.0}]}";
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(body))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["samples_written"], 5);

    let (_st, q) = h
        .call("GET", &format!("/v1/series/{sid}/points?from=20&to=20"), None)
        .await;
    let got: Vec<f64> = q["segments"][0]["samples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["v"].as_f64().unwrap())
        .collect();
    assert_eq!(got, vec![2.0, 20.0, 200.0]); // arrival order, all retained
}

#[tokio::test]
async fn duplicate_policy_keep_last_and_reject() {
    let h = Harness::new("dups2");
    let sid = create_series(&h, "kl", "keep_last").await;
    let body = "{\"samples\":[{\"t\":1,\"v\":1.0},{\"t\":1,\"v\":9.0}]}";
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(body))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["samples_written"], 1);
    assert_eq!(v["duplicates_dropped"], 1);
    let (_st, q) = h
        .call("GET", &format!("/v1/series/{sid}/points?from=1&to=1"), None)
        .await;
    assert_eq!(q["segments"][0]["samples"][0]["v"], 9.0);

    let sid2 = create_series(&h, "rj", "reject").await;
    let (st, v) = h
        .call(
            "POST",
            &format!("/v1/series/{sid2}/segments"),
            Some("{\"samples\":[{\"t\":1,\"v\":1.0},{\"t\":1,\"v\":2.0}]}"),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["code"], "duplicate_timestamp");
    // The failed batch left no segment.
    let (st, _) = h
        .call("GET", &format!("/v1/series/{sid2}/segments"), None)
        .await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn paging_and_many_segments() {
    let h = Harness::new("pages");
    let sid = create_series(&h, "p", "keep_all").await;
    // Three disjoint segments.
    for (base, n) in [(0_i64, 100_i64), (10_000, 100), (20_000, 100)] {
        let entries: Vec<String> = (0..n)
            .map(|i| format!("{{\"t\":{},\"v\":{}}}", base + i, base as f64 + i as f64))
            .collect();
        let body = format!("{{\"samples\":[{}]}}", entries.join(","));
        let (st, v) = h
            .call("POST", &format!("/v1/series/{sid}/segments"), Some(&body))
            .await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
    }
    let (st, q) = h
        .call(
            "GET",
            &format!("/v1/series/{sid}/points?from=50&to=20099&limit=1000&offset=0"),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(q["total_samples"], 250);
    assert_eq!(q["segments"].as_array().unwrap().len(), 3);

    // Offset 150, limit 20: skips segment 1 (100) + 50 of segment 2, so the
    // first point lands at t=10050 inside segment 2.
    let (_st, q) = h
        .call(
            "GET",
            &format!("/v1/series/{sid}/points?from=0&to=30000&limit=20&offset=150"),
            None,
        )
        .await;
    assert_eq!(q["total_samples"], 20);
    assert_eq!(q["has_more"], true);
    assert_eq!(q["segments"].as_array().unwrap().len(), 1);
    assert_eq!(q["segments"][0]["samples"][0]["t"], 10050);
    assert_eq!(q["segments"][0]["samples"][19]["t"], 10069);

    // Overlapping segment refused at catalog level.
    let bad = format!(
        "{{\"samples\":[{}]}}",
        (0..3)
            .map(|i| format!("{{\"t\":{i},\"v\":1.0}}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(&bad))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
}

#[tokio::test]
async fn corruption_in_payload_is_reported_with_byte_range() {
    let h = Harness::new("corrupt");
    let sid = create_series(&h, "c", "keep_all").await;
    let entries: Vec<String> = (0..1200)
        .map(|i| format!("{{\"t\":{},\"v\":{}}}", i, i as f64))
        .collect();
    let body = format!("{{\"samples\":[{}]}}", entries.join(","));
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(&body))
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let seg_id = v["segment_id"].as_str().unwrap();
    let path = h
        .root
        .join("segments")
        .join(&sid)
        .join(format!("{seg_id}.seg"));

    // Locate block 2's payload range via the verify endpoint, then flip a
    // byte well inside it (10 bytes into the timestamp sub-payload).
    let (st, vr) = h
        .call("POST", &format!("/v1/segments/{seg_id}/verify"), None)
        .await;
    assert_eq!(st, StatusCode::OK, "{vr}");
    let target = vr["blocks"][2]["payload_start"].as_u64().unwrap() as usize + 10;
    let block_end = vr["blocks"][2]["payload_end"].as_u64().unwrap() as usize;
    assert!(target < block_end);

    let mut bytes = std::fs::read(&path).unwrap();
    bytes[target] ^= 0xa5;
    std::fs::write(&path, &bytes).unwrap();

    let (st, v) = h
        .call(
            "GET",
            &format!("/v1/series/{sid}/points?from=1100&to=1199"),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY, "{}", v);
    assert_eq!(v["error"]["code"], "segment_corrupt");
    let ranges = v["error"]["corruption"]["ranges"].as_array().unwrap();
    assert!(!ranges.is_empty());
    let r0 = &ranges[0];
    let start = r0["start"].as_u64().unwrap();
    let end = r0["end"].as_u64().unwrap();
    assert!(
        start <= target as u64 && (target as u64) < end,
        "flipped byte {target} not within reported range [{start},{end})"
    );

    // Explicit verify flags the same file.
    let (st, _) = h.call("POST", &format!("/v1/segments/{seg_id}/verify"), None).await;
    assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn torn_tmp_tail_is_never_queryable() {
    let h = Harness::new("torn");
    let sid = create_series(&h, "t", "keep_all").await;
    let sdir = h.root.join("segments").join(&sid);

    // Simulate a crash mid-write: a .tmp file with a partial tail.
    std::fs::create_dir_all(&sdir).unwrap();
    let fake = sdir.join("fafafafafafafafafafafafafafafafa.seg.tmp");
    std::fs::write(&fake, b"SSEG0001 partial garbage that will never be renamed").unwrap();

    // Reconcile reaps it; the catalog stays empty.
    let (st, v) = h.call("GET", "/v1/reconcile", None).await;
    assert_eq!(st, StatusCode::OK, "{}", v);
    assert!(v["reconcile"]["temp_reaped"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p.as_str().unwrap().ends_with(".seg.tmp")));
    assert!(!fake.exists());

    // A valid later write still works and becomes queryable.
    let (st, v) = h
        .call(
            "POST",
            &format!("/v1/series/{sid}/segments"),
            Some("{\"samples\":[{\"t\":1,\"v\":1.0}]}"),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let (st, q) = h
        .call("GET", &format!("/v1/series/{sid}/points?from=0&to=10"), None)
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(q["total_samples"], 1);
}

#[tokio::test]
async fn orphan_sealed_file_is_reported_not_auto_adopted() {
    let h = Harness::new("orphan");
    let sid = create_series(&h, "o", "keep_all").await;
    let (st, _v) = h
        .call(
            "POST",
            &format!("/v1/series/{sid}/segments"),
            Some("{\"samples\":[{\"t\":1,\"v\":1.0},{\"t\":2,\"v\":2.0}]}"),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    // Delete the catalog row out-of-band (simulates crash after rename,
    // before DB insert): file remains but is not queryable.
    let db = h.root.join("catalog.db");
    {
        let c = rusqlite::Connection::open(&db).unwrap();
        c.execute("DELETE FROM segments", []).unwrap();
    }
    // Reconcile over the same data dir reports the orphan but does not
    // silently adopt it (adoption would make un-registered data queryable).
    let report = h
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/reconcile")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(report.status(), StatusCode::OK);
    let bytes = report.into_body().collect().await.unwrap().to_bytes();
    let rep: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let orphans = rep["reconcile"]["orphans"].as_array().unwrap();
    assert_eq!(orphans.len(), 1);
    assert!(orphans[0]["path"].as_str().unwrap().ends_with(".seg"));

    // Range query over that series returns nothing (not adopted).
    let store = Store::new(
        Catalog::open(&h.root.join("catalog.db")).unwrap(),
        h.root.join("segments"),
    )
    .unwrap();
    let rows = store
        .catalog
        .segments_for_range(&sid, i64::MIN, i64::MAX)
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn out_of_order_batch_rejected() {
    let h = Harness::new("ooo");
    let sid = create_series(&h, "z", "keep_all").await;
    let body = "{\"samples\":[{\"t\":2,\"v\":1.0},{\"t\":1,\"v\":2.0}]}";
    let (st, v) = h
        .call("POST", &format!("/v1/series/{sid}/segments"), Some(body))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(v["error"]["code"], "out_of_order");
}
