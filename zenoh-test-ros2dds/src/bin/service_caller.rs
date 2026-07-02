//! Standalone ROS 2 service-caller process used by the service tests.
//!
//! It must be a separate OS process because rcl reads ROS_DOMAIN_ID once per
//! process — the test hosts the service SERVER on one domain in-process and
//! spawns this caller on the other domain.
//!
//! Environment:
//!   ROS_DOMAIN_ID    - DDS domain (read by rcl)
//!   SERVICE_NAME     - fully-qualified service to call (AddTwoInts)
//!   CALL_TIMEOUT_S   - how long to wait for the response (default 30)
//!   WAIT_AVAILABLE_S - how long to wait for service availability (default 20)
//!
//! Prints exactly one verdict line on stdout:
//!   CALLER_SERVICE_UNAVAILABLE | CALLER_GOT_RESPONSE sum=<n> |
//!   CALLER_TIMEOUT | CALLER_ERROR <e>

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    std::env::set_var("RMW_IMPLEMENTATION", "rmw_cyclonedds_cpp");
    let service = std::env::var("SERVICE_NAME").expect("SERVICE_NAME must be set");
    let call_timeout = Duration::from_secs(env_u64("CALL_TIMEOUT_S", 30));
    let wait_available = Duration::from_secs(env_u64("WAIT_AVAILABLE_S", 20));

    let ctx = r2r::Context::create().expect("create ROS context");
    let mut node = r2r::Node::create(ctx, "svc_caller", "").expect("create node");
    let client = node
        .create_client::<r2r::example_interfaces::srv::AddTwoInts::Service>(
            &service,
            r2r::QosProfile::default(),
        )
        .expect("create client");
    let available = r2r::Node::is_available(&client).expect("is_available");

    // Spin on a plain thread; the async part only awaits futures.
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let spinner = std::thread::spawn(move || {
        while !stop2.load(Ordering::Relaxed) {
            node.spin_once(Duration::from_millis(10));
        }
    });

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        if tokio::time::timeout(wait_available, available)
            .await
            .is_err()
        {
            println!("CALLER_SERVICE_UNAVAILABLE");
            std::process::exit(2);
        }

        let req = r2r::example_interfaces::srv::AddTwoInts::Request { a: 20, b: 22 };
        let call = client.request(&req).expect("send request");
        match tokio::time::timeout(call_timeout, call).await {
            Ok(Ok(resp)) => println!("CALLER_GOT_RESPONSE sum={}", resp.sum),
            Ok(Err(e)) => println!("CALLER_ERROR {e}"),
            Err(_) => println!("CALLER_TIMEOUT"),
        }
    });

    stop.store(true, Ordering::Relaxed);
    let _ = spinner.join();
}
