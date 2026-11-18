//! Standalone ROS 2 action-caller process used by the action tests.
//!
//! rcl reads ROS_DOMAIN_ID once per process, so the test hosts the server in-process
//! on one domain and spawns this caller on the other.
//!
//! Environment:
//!   ROS_DOMAIN_ID      DDS domain, read by rcl
//!   ACTION_NAME        fully qualified Fibonacci action to call
//!   CALL_TIMEOUT_S     seconds to wait for the result, default 30
//!   WAIT_AVAILABLE_S   seconds to wait for the action server, default 20
//!
//! Sends the goal `order=5` and prints exactly one verdict line on stdout:
//!   CALLER_ACTION_UNAVAILABLE | CALLER_GOT_RESULT sequence=<a,b,..> feedback=<a,b,..> |
//!   CALLER_TIMEOUT | CALLER_ERROR <e>

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::StreamExt;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn join(sequence: &[i32]) -> String {
    sequence
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn main() {
    std::env::set_var("RMW_IMPLEMENTATION", "rmw_cyclonedds_cpp");
    let action = std::env::var("ACTION_NAME").expect("ACTION_NAME must be set");
    let call_timeout = Duration::from_secs(env_u64("CALL_TIMEOUT_S", 30));
    let wait_available = Duration::from_secs(env_u64("WAIT_AVAILABLE_S", 20));

    let ctx = r2r::Context::create().expect("create ROS context");
    let mut node = r2r::Node::create(ctx, "action_caller", "").expect("create node");
    let client = node
        .create_action_client::<r2r::example_interfaces::action::Fibonacci::Action>(&action)
        .expect("create action client");
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
            println!("CALLER_ACTION_UNAVAILABLE");
            std::process::exit(2);
        }

        let goal = r2r::example_interfaces::action::Fibonacci::Goal { order: 5 };
        let call = async {
            let (_handle, result, mut feedback) = client.send_goal_request(goal)?.await?;
            let feedback = feedback
                .next()
                .await
                .map(|f| f.sequence)
                .unwrap_or_default();
            let (_status, result) = result.await?;
            Ok::<_, r2r::Error>((result.sequence, feedback))
        };
        match tokio::time::timeout(call_timeout, call).await {
            Ok(Ok((sequence, feedback))) => println!(
                "CALLER_GOT_RESULT sequence={} feedback={}",
                join(&sequence),
                join(&feedback)
            ),
            Ok(Err(e)) => println!("CALLER_ERROR {e}"),
            Err(_) => println!("CALLER_TIMEOUT"),
        }
    });

    stop.store(true, Ordering::Relaxed);
    let _ = spinner.join();
}
