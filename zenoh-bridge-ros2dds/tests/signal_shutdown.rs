//
// The bridge stops on SIGINT and SIGTERM, also when it was started with SIGINT ignored.
// A script that starts it in the background (`cmd &`) passes SIGINT on as ignored and bash
// cannot reset that in the background subshell, so Ctrl+C on the robot's wrapper did nothing.
// Linux only, it reads the ignored signals from /proc.
#![cfg(target_os = "linux")]

use std::{
    io::{BufRead, BufReader},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

const BRIDGE: &str = env!("CARGO_BIN_EXE_zenoh-bridge-ros2dds");
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);
const SIGINT_MASK: u64 = 1 << (2 - 1);

/// Runs `program` from bash with SIGINT ignored, as the wrapper's background job inherits it.
fn with_sigint_ignored(program: &str) -> Command {
    let mut cmd = Command::new("bash");
    cmd.args(["-c", "trap '' INT; exec \"$0\" \"$@\"", program]);
    cmd
}

/// Kills the bridge when a test fails before stopping it, so that no bridge outlives the test.
struct Bridge(Child);

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts the bridge with SIGINT ignored and waits until its ROS 2 plugin runs.
fn start_bridge(domain: u32, port: u16) -> Bridge {
    let mut bridge = Bridge(
        with_sigint_ignored(BRIDGE)
            .args([
                "--no-multicast-scouting",
                "-d",
                &domain.to_string(),
                "-l",
                &format!("tcp/127.0.0.1:{port}"),
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn bridge"),
    );

    // keep draining stdout so that logging never blocks the bridge
    let (started_tx, started_rx) = mpsc::channel();
    let stdout = BufReader::new(bridge.0.stdout.take().unwrap());
    std::thread::spawn(move || {
        for line in stdout.lines().map_while(Result::ok) {
            if line.contains("zenoh-plugin-ros2dds version") {
                let _ = started_tx.send(());
            }
        }
    });
    if let Err(e) = started_rx.recv_timeout(STARTUP_TIMEOUT) {
        panic!(
            "the bridge's ROS 2 plugin did not start ({e}), exit status {:?}",
            bridge.0.try_wait()
        );
    }
    bridge
}

/// Sends `signal` to the bridge and returns its exit status.
fn stop_bridge(mut bridge: Bridge, signal: &str) -> ExitStatus {
    let sent = Command::new("kill")
        .args([&format!("-{signal}"), &bridge.0.id().to_string()])
        .status()
        .expect("run kill");
    assert!(sent.success(), "kill -{signal} failed");
    let deadline = Instant::now() + EXIT_TIMEOUT;
    loop {
        if let Some(status) = bridge.0.try_wait().expect("wait for bridge") {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "the bridge did not exit within {EXIT_TIMEOUT:?} after SIG{signal}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn launcher_ignores_sigint() {
    let out = with_sigint_ignored("grep")
        .args(["SigIgn", "/proc/self/status"])
        .output()
        .expect("run grep");
    let line = String::from_utf8_lossy(&out.stdout);
    let mask = u64::from_str_radix(line.trim().trim_start_matches("SigIgn:").trim(), 16)
        .expect("parse SigIgn");
    assert_ne!(mask & SIGINT_MASK, 0, "SIGINT not ignored: {line}");
}

#[test]
fn sigint_stops_bridge_started_with_sigint_ignored() {
    let bridge = start_bridge(70, 7910);
    let status = stop_bridge(bridge, "INT");
    assert_eq!(status.code(), Some(130), "exit status after SIGINT");
}

#[test]
fn sigterm_stops_bridge() {
    let bridge = start_bridge(71, 7911);
    let status = stop_bridge(bridge, "TERM");
    assert_eq!(status.code(), Some(143), "exit status after SIGTERM");
}
