//
// The DDS endpoints a bridge creates for remote ROS endpoints exist exactly while such remote endpoints are announced.
// A Writer without remote publisher made TRANSIENT_LOCAL subscribers of robot-local topics report an incompatible publisher.
//
//   robot bridge (listens) <- operator bridge(s) (connect), one DDS domain each
//
// DdsProbe counts the endpoints of all participants of a domain on the DDS builtin topics.

pub mod common;

use std::time::Duration;

use common::{operator_bridge_cfg, robot_bridge_cfg, DdsProbe, Endpoint};

const WAIT: Duration = Duration::from_secs(20);
const SETTLE: Duration = Duration::from_secs(3);

/// Robot-local topic like the e-stops: the bridge must neither publish on it nor break TRANSIENT_LOCAL readers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_topic_gets_no_bridge_writer() {
    common::init_env();
    let (rd, od, port) = (60, 61, 7900);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(&operator_bridge_cfg(od, port)).await;
    let probe = DdsProbe::new(rd);
    let reader = probe.transient_local_reader("rt/estop");

    let sub = Endpoint::spawn("sub", rd, &["sub", "/estop", "rel", "tl", "1"]).await;
    tokio::time::sleep(SETTLE).await;
    let without_pub = (
        probe.writers("rt/estop"),
        DdsProbe::incompatible_writers(reader),
    );

    let publ = Endpoint::spawn("pub", rd, &["pub", "/estop", "rel", "tl", "10", "p"]).await;
    let delivered = common::eventually(WAIT, || sub.live() > 0).await;
    tokio::time::sleep(SETTLE).await;
    let with_pub = (
        probe.writers("rt/estop"),
        DdsProbe::incompatible_writers(reader),
    );

    publ.stop().await;
    sub.stop().await;
    common::close_bridge(operator).await;
    common::close_bridge(robot).await;

    assert_eq!(
        without_pub,
        (0, 0),
        "(writers, incompatible writers) of a subscribed topic without any publisher"
    );
    assert!(delivered, "the local subscriber got no data");
    assert_eq!(
        with_pub,
        (1, 0),
        "(writers, incompatible writers) of a topic with only a local publisher"
    );
}

/// The subscriber-side Writer stays while any remote publisher remains, and leaves with the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn topic_writer_follows_remote_publishers() {
    common::init_env();
    let (rd, o1, o2, port) = (62, 63, 64, 7901);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let op1 = common::create_bridge_with(&operator_bridge_cfg(o1, port)).await;
    let op2 = common::create_bridge_with(&operator_bridge_cfg(o2, port)).await;
    let probe = DdsProbe::new(rd);
    let writers = || probe.writers("rt/chatter");

    let sub = Endpoint::spawn("sub", rd, &["sub", "/chatter", "rel", "vol", "10"]).await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(writers(), 0, "bridge Writer without remote publisher");

    let pub_a = Endpoint::spawn("a", o1, &["pub", "/chatter", "rel", "vol", "10", "a"]).await;
    assert!(
        common::eventually(WAIT, || writers() == 1 && sub.live_from("a") > 0).await,
        "no bridge Writer or data for the first remote publisher: writers={} {}",
        writers(),
        sub.raw_result()
    );

    let pub_b = Endpoint::spawn("b", o2, &["pub", "/chatter", "rel", "vol", "10", "b"]).await;
    assert!(
        common::eventually(WAIT, || sub.live_from("b") > 0).await,
        "no data from the second remote publisher: {}",
        sub.raw_result()
    );
    assert_eq!(writers(), 1, "one Writer serves both remote publishers");

    pub_a.stop().await;
    let b_before = sub.live_from("b");
    assert!(
        common::eventually(WAIT, || sub.live_from("b") > b_before + 10).await,
        "data of the remaining publisher stopped when the other one left: {}",
        sub.raw_result()
    );
    assert_eq!(writers(), 1, "Writer gone while a remote publisher remains");

    pub_b.stop().await;
    assert!(
        common::eventually(WAIT, || writers() == 0).await,
        "bridge Writer stayed after the last remote publisher left"
    );

    let pub_c = Endpoint::spawn("c", o1, &["pub", "/chatter", "rel", "vol", "10", "c"]).await;
    assert!(
        common::eventually(WAIT, || writers() == 1 && sub.live_from("c") > 0).await,
        "no bridge Writer or data after a remote publisher returned: writers={} {}",
        writers(),
        sub.raw_result()
    );
    common::close_bridge(op1).await;
    assert!(
        common::eventually(WAIT, || writers() == 0).await,
        "bridge Writer stayed after the publisher's bridge left"
    );

    pub_c.stop().await;
    sub.stop().await;
    common::close_bridge(op2).await;
    common::close_bridge(robot).await;
}

/// The server-side request Writer and reply Reader stay while any remote client remains, and leave with the last one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_proxy_follows_remote_clients() {
    common::init_env();
    let (rd, o1, o2, port) = (65, 66, 67, 7902);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let op1 = common::create_bridge_with(&operator_bridge_cfg(o1, port)).await;
    let op2 = common::create_bridge_with(&operator_bridge_cfg(o2, port)).await;
    let probe = DdsProbe::new(rd);
    let proxy = || (probe.writers("rq/addRequest"), probe.readers("rr/addReply"));

    let server = Endpoint::spawn("server", rd, &["server", "/add", "rel", "vol", "10"]).await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        proxy(),
        (0, 0),
        "bridge client endpoints without remote client"
    );

    let client_1 = Endpoint::spawn("client_1", o1, &["client", "/add", "rel", "vol", "10"]).await;
    assert!(
        common::eventually(WAIT, || proxy() == (1, 1)).await,
        "bridge client endpoints for a remote client: {:?}",
        proxy()
    );
    assert_eq!(
        common::call_service(o1, "/add").await,
        "CALLER_GOT_RESPONSE sum=42"
    );

    let client_2 = Endpoint::spawn("client_2", o2, &["client", "/add", "rel", "vol", "10"]).await;
    tokio::time::sleep(SETTLE).await;
    client_1.stop().await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        proxy(),
        (1, 1),
        "bridge client endpoints while a remote client remains"
    );
    assert_eq!(
        common::call_service(o2, "/add").await,
        "CALLER_GOT_RESPONSE sum=42"
    );

    client_2.stop().await;
    assert!(
        common::eventually(WAIT, || proxy() == (0, 0)).await,
        "bridge client endpoints stayed after the last remote client left: {:?}",
        proxy()
    );

    let client_3 = Endpoint::spawn("client_3", o1, &["client", "/add", "rel", "vol", "10"]).await;
    assert!(
        common::eventually(WAIT, || proxy() == (1, 1)).await,
        "bridge client endpoints after a remote client returned: {:?}",
        proxy()
    );
    common::close_bridge(op1).await;
    assert!(
        common::eventually(WAIT, || proxy() == (0, 0)).await,
        "bridge client endpoints stayed after the client's bridge left: {:?}",
        proxy()
    );

    client_3.stop().await;
    server.stop().await;
    common::close_bridge(op2).await;
    common::close_bridge(robot).await;
}

/// Action proxies on both sides follow the remote partner, and a ROS action call works through two bridges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn action_proxies_follow_remote_partners() {
    common::init_env();
    let (rd, od, port) = (68, 69, 7903);
    let robot = common::create_bridge_with(&robot_bridge_cfg(rd, port)).await;
    let operator = common::create_bridge_with(&operator_bridge_cfg(od, port)).await;
    let robot_probe = DdsProbe::new(rd);
    let operator_probe = DdsProbe::new(od);
    let goal_writers = || robot_probe.writers("rq/fib/_action/send_goalRequest");
    let feedback_writers = || operator_probe.writers("rt/fib/_action/feedback");

    let client =
        Endpoint::spawn("client", od, &["action_client", "/fib", "rel", "vol", "10"]).await;
    tokio::time::sleep(SETTLE).await;
    assert_eq!(
        feedback_writers(),
        0,
        "bridge feedback Writer without remote action server"
    );

    let server =
        Endpoint::spawn("server", rd, &["action_server", "/fib", "rel", "vol", "10"]).await;
    assert!(
        common::eventually(WAIT, || goal_writers() == 1 && feedback_writers() == 1).await,
        "bridge proxies for remote partners: goal writers={} feedback writers={}",
        goal_writers(),
        feedback_writers()
    );
    assert_eq!(
        common::call_action(od, "/fib").await,
        "CALLER_GOT_RESULT sequence=0,1,1,2,3,5 feedback=0,1,1,2,3"
    );

    client.stop().await;
    assert!(
        common::eventually(WAIT, || goal_writers() == 0).await,
        "bridge goal Writer stayed after the last remote action client left"
    );
    server.stop().await;
    assert!(
        common::eventually(WAIT, || feedback_writers() == 0).await,
        "bridge feedback Writer stayed after the remote action server left"
    );

    common::close_bridge(operator).await;
    common::close_bridge(robot).await;
}
