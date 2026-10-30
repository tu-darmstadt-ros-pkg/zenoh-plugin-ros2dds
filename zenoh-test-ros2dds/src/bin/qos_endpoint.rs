//! ROS 2 endpoint process for the QoS tests, one per DDS domain because rcl reads ROS_DOMAIN_ID once per process.
//!
//! Usage: qos_endpoint <role> <topic> <rel|be> <tl|vol> <depth> [tag]
//!
//! pub publishes `<tag>:h0..h4` at once and then `<tag>:l<i>` at 10 Hz.
//! lazy_pub publishes `<tag>:l<i>` at 10 Hz while it has a matched subscription.
//! sub and tf_sub count std_msgs/String messages or collect tf child frames.
//! tf_pub publishes one tf2_msgs/TFMessage with child frame `<tag>`.
//!
//! Prints `READY` once its entities exist, subscribers then print a cumulative `RESULT ...` line every 200 ms.
//! Runs until stdin closes, which also happens when the test dies, and then deletes its entities so DDS peers see it leave.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, Write},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use futures::{FutureExt, StreamExt};
use r2r::QosProfile;

const HISTORY_MSGS: usize = 5;

static STOP: AtomicBool = AtomicBool::new(false);

fn stopping() -> bool {
    STOP.load(Ordering::Relaxed)
}

fn qos(reliability: &str, durability: &str, depth: usize) -> QosProfile {
    let q = QosProfile::default().keep_last(depth);
    let q = match reliability {
        "rel" => q.reliable(),
        "be" => q.best_effort(),
        other => panic!("bad reliability {other}"),
    };
    match durability {
        "tl" => q.transient_local(),
        "vol" => q.volatile(),
        other => panic!("bad durability {other}"),
    }
}

/// Prints one line, a closed stdout means the test is gone.
fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    if writeln!(out, "{line}").and_then(|_| out.flush()).is_err() {
        STOP.store(true, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct SubStats {
    history: BTreeSet<String>,
    history_dups: usize,
    live: usize,
    live_by_tag: BTreeMap<String, usize>,
}

fn by_tag(m: &BTreeMap<String, usize>) -> String {
    m.iter().map(|(t, n)| format!("{t}:{n}")).collect::<Vec<_>>().join(",")
}

impl SubStats {
    fn record(&mut self, data: &str) {
        let (tag, kind) = data.split_once(':').unwrap_or(("?", data));
        if kind.starts_with('h') {
            if !self.history.insert(data.to_string()) {
                self.history_dups += 1;
            }
        } else {
            self.live += 1;
            *self.live_by_tag.entry(tag.to_string()).or_default() += 1;
        }
    }

    fn line(&self) -> String {
        let mut history_by_tag = BTreeMap::<String, usize>::new();
        for h in &self.history {
            let tag = h.split_once(':').map_or("?", |(t, _)| t);
            *history_by_tag.entry(tag.to_string()).or_default() += 1;
        }
        format!(
            "RESULT history={} history_dups={} live={} live_by_tag={} history_by_tag={}",
            self.history.len(),
            self.history_dups,
            self.live,
            by_tag(&self.live_by_tag),
            by_tag(&history_by_tag)
        )
    }
}

fn main() {
    std::env::set_var("RMW_IMPLEMENTATION", "rmw_cyclonedds_cpp");
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!("usage: qos_endpoint <role> <topic> <rel|be> <tl|vol> <depth> [tag]");
        std::process::exit(64);
    }
    let (role, topic) = (args[1].as_str(), args[2].as_str());
    let profile = qos(&args[3], &args[4], args[5].parse().expect("depth"));
    let tag = args.get(6).cloned().unwrap_or_else(|| "p".to_string());

    std::thread::spawn(|| {
        for line in std::io::stdin().lock().lines() {
            match line {
                Ok(l) if l.trim() != "QUIT" => continue,
                _ => break,
            }
        }
        STOP.store(true, Ordering::Relaxed);
    });

    let ctx = r2r::Context::create().expect("create ROS context");
    let node_name = format!("qos_{}_{}", role, std::process::id());
    let mut node = r2r::Node::create(ctx, &node_name, "").expect("create node");

    match role {
        "pub" | "lazy_pub" => {
            let publisher = node
                .create_publisher::<r2r::std_msgs::msg::String>(topic, profile)
                .expect("create publisher");
            say("READY");
            if role == "pub" {
                for i in 0..HISTORY_MSGS {
                    let data = format!("{tag}:h{i}");
                    publisher
                        .publish(&r2r::std_msgs::msg::String { data })
                        .expect("publish");
                }
            }
            let mut i = 0u64;
            while !stopping() {
                std::thread::sleep(Duration::from_millis(100));
                if role == "lazy_pub"
                    && publisher
                        .get_inter_process_subscription_count()
                        .expect("subscription count")
                        == 0
                {
                    continue;
                }
                let data = format!("{tag}:l{i}");
                publisher
                    .publish(&r2r::std_msgs::msg::String { data })
                    .expect("publish");
                i += 1;
            }
        }
        "tf_pub" => {
            let publisher = node
                .create_publisher::<r2r::tf2_msgs::msg::TFMessage>(topic, profile)
                .expect("create publisher");
            let mut t = r2r::geometry_msgs::msg::TransformStamped::default();
            t.header.frame_id = "base_link".to_string();
            t.child_frame_id = tag;
            t.transform.rotation.w = 1.0;
            publisher
                .publish(&r2r::tf2_msgs::msg::TFMessage { transforms: vec![t] })
                .expect("publish");
            say("READY");
            while !stopping() {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        "sub" => {
            let mut stream = node
                .subscribe::<r2r::std_msgs::msg::String>(topic, profile)
                .expect("subscribe");
            let mut stats = SubStats::default();
            run_subscriber(&mut node, &mut stream, &mut stats, |s, m| s.record(&m.data), |s| s.line());
        }
        "tf_sub" => {
            let mut stream = node
                .subscribe::<r2r::tf2_msgs::msg::TFMessage>(topic, profile)
                .expect("subscribe");
            let mut frames = BTreeSet::<String>::new();
            run_subscriber(
                &mut node,
                &mut stream,
                &mut frames,
                |f, m| {
                    for t in m.transforms {
                        f.insert(t.child_frame_id);
                    }
                },
                |f| {
                    let v: Vec<&str> = f.iter().map(|s| s.as_str()).collect();
                    format!("RESULT frames={}", v.join(","))
                },
            );
        }
        other => panic!("unknown role {other}"),
    }
}

/// Spins and drains on one thread, r2r drops samples once its 10 slot stream channel is full.
fn run_subscriber<M, S>(
    node: &mut r2r::Node,
    stream: &mut (impl futures::Stream<Item = M> + Unpin),
    state: &mut S,
    on_msg: impl Fn(&mut S, M),
    line: impl Fn(&S) -> String,
) {
    say("READY");
    let mut last_print = Instant::now();
    say(&line(state));
    while !stopping() {
        node.spin_once(Duration::from_millis(10));
        while let Some(Some(m)) = stream.next().now_or_never() {
            on_msg(state, m);
        }
        if last_print.elapsed() >= Duration::from_millis(200) {
            say(&line(state));
            last_print = Instant::now();
        }
    }
}
