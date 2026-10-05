//! Server entrypoint: `sensor-segments --data-dir ./data --bind 0.0.0.0:8080`

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use sensor_segments::api;
use sensor_segments::catalog::Catalog;
use sensor_segments::store::Store;

#[derive(Debug, Clone)]
struct Args {
    data_dir: PathBuf,
    catalog: PathBuf,
    bind: SocketAddr,
}

fn parse_args() -> Args {
    let mut data_dir = PathBuf::from("./sensor-data");
    let mut bind: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--data-dir" => {
                data_dir = PathBuf::from(args.next().expect("--data-dir needs a value"))
            }
            "--bind" => {
                bind = args
                    .next()
                    .expect("--bind needs a value")
                    .parse()
                    .expect("invalid --bind address");
            }
            "-h" | "--help" => {
                println!("usage: sensor-segments [--data-dir DIR] [--bind ADDR]");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    let catalog = data_dir.join("catalog.db");
    Args { data_dir, catalog, bind }
}

#[tokio::main]
async fn main() -> sensor_segments::Result<()> {
    let args = parse_args();
    std::fs::create_dir_all(&args.data_dir).map_err(|e| {
        sensor_segments::Error::io(
            e,
            sensor_segments::error::IoCtx::new("create data dir", args.data_dir.display()),
        )
    })?;

    let catalog = Catalog::open(&args.catalog)?;
    let store: Arc<Store> = Store::new(catalog, args.data_dir.join("segments"))?;

    // Startup reconciliation: torn .tmp tails are removed, orphan .seg files
    // (sealed but never catalog-registered) are reported, never served.
    let report = store.reconcile()?;
    for p in &report.temp_reaped {
        eprintln!("startup: removed unfinished temp segment: {p}");
    }
    for o in &report.orphans {
        eprintln!(
            "startup: ORPHAN sealed segment not in catalog: {} (series {}, [{},{}]); \
             it will NOT be queryable until manually adopted/removed",
            o.path, o.series_id, o.min_t, o.max_t
        );
    }

    let app = api::router(store, report).fallback(fallback);
    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .map_err(|e| sensor_segments::Error::io(
            e,
            sensor_segments::error::IoCtx::new("bind", args.bind.to_string()),
        ))?;
    eprintln!("sensor-segments listening on http://{}", args.bind);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
        .map_err(|e| sensor_segments::Error::io(
            e,
            sensor_segments::error::IoCtx::new("serve", args.bind.to_string()),
        ))?;
    Ok(())
}

async fn fallback() -> (axum::http::StatusCode, axum::Json<serde_json::Value>) {
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({
            "error": { "code": "not_found", "message": "no such route" }
        })),
    )
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("shutting down");
}
