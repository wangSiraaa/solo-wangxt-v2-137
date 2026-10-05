mod api;
mod catalog;
mod codec;
mod samples;
mod segment;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use api::AppState;
use catalog::Catalog;

#[tokio::main]
async fn main() {
    let data_dir: PathBuf = std::env::var("DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data"));
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    std::fs::create_dir_all(data_dir.join("tmp")).expect("create tmp dir");
    std::fs::create_dir_all(data_dir.join("segments")).expect("create segments dir");

    // 启动恢复 1/3：目录中仍处于 writing 的段 -> aborted（永不参与查询）
    let catalog = Catalog::open(&data_dir.join("catalog.db")).expect("open catalog");

    // 启动恢复 2/3：清理中断写入留下的临时文件
    let mut cleaned = 0usize;
    if let Ok(entries) = std::fs::read_dir(data_dir.join("tmp")) {
        for e in entries.flatten() {
            if std::fs::remove_file(e.path()).is_ok() {
                cleaned += 1;
            }
        }
    }

    // 启动恢复 3/3：删除未在目录登记的孤儿段文件
    //（例如 rename 完成后、seal 登记前崩溃留下的文件）
    let known: HashSet<String> = catalog
        .all_segment_paths()
        .expect("list known segment paths")
        .into_iter()
        .collect();
    let seg_root = data_dir.join("segments");
    if let Ok(series_dirs) = std::fs::read_dir(&seg_root) {
        for d in series_dirs.flatten() {
            if let Ok(files) = std::fs::read_dir(d.path()) {
                for f in files.flatten() {
                    let rel = f
                        .path()
                        .strip_prefix(&data_dir)
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_default();
                    if !known.contains(&rel) && std::fs::remove_file(f.path()).is_ok() {
                        cleaned += 1;
                    }
                }
            }
        }
    }
    if cleaned > 0 {
        eprintln!("recovery: removed {cleaned} leftover file(s) from interrupted writes");
    }

    let state = Arc::new(AppState {
        catalog: Mutex::new(catalog),
        data_dir: data_dir.clone(),
    });
    let app = api::router(state);

    let addr = format!("0.0.0.0:{port}");
    println!("sensor-tsdb listening on {addr}, data dir: {}", data_dir.display());
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
    axum::serve(listener, app).await.expect("serve");
}
