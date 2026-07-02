//! Dump a live bridge's admin space. Usage:
//!   admin_dump [selector] [endpoint]
//! Defaults: selector "@/*/ros2/**", endpoint tcp/127.0.0.1:7448

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let selector = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "@/*/ros2/**".to_string());
    let endpoint = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "tcp/127.0.0.1:7448".to_string());

    let mut cfg = zenoh::Config::default();
    cfg.insert_json5("mode", "\"client\"").unwrap();
    cfg.insert_json5("connect/endpoints", &format!("[\"{endpoint}\"]"))
        .unwrap();
    cfg.insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    let session = zenoh::open(cfg).await.expect("open session");

    let replies = session
        .get(&selector)
        .timeout(std::time::Duration::from_secs(10))
        .await
        .expect("get failed");
    let mut n = 0;
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                let payload = String::from_utf8_lossy(&sample.payload().to_bytes()).to_string();
                println!("== {}\n{}", sample.key_expr(), payload);
                n += 1;
            }
            Err(e) => println!("!! reply error: {e:?}"),
        }
    }
    eprintln!("({n} replies)");
}
