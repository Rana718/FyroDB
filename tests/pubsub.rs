use fyro_db::pubsub::{PubSub, SubSlot, WorkerNotifier, encode_sub_reply};
use mio::{Poll, Token, Waker};
use std::sync::Arc;

fn make_notifier(poll: &Poll, worker_index: usize) -> Arc<WorkerNotifier> {
    let waker = Arc::new(Waker::new(poll.registry(), Token(usize::MAX)).unwrap());
    WorkerNotifier::new(waker, worker_index)
}

fn make_slot(notifier: &Arc<WorkerNotifier>, token: usize) -> Arc<SubSlot> {
    Arc::new(SubSlot::new(token, Arc::clone(notifier)))
}

/// A subscriber plus the notifier of the worker it would live on, so tests
/// can observe where publishing routed the frame.
fn make_sub(worker_index: usize, token: usize) -> (Arc<WorkerNotifier>, Arc<SubSlot>) {
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll, worker_index);
    let slot = make_slot(&notifier, token);
    (notifier, slot)
}

/// Drain every fanned-out frame queued for one worker.
fn fanout_frames(notifier: &Arc<WorkerNotifier>) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    notifier.drain_fanout(|entry| out.push((entry.channel.to_string(), entry.frame.to_vec())));
    out
}

fn drain(slot: &SubSlot) -> Vec<u8> {
    let mut buf = Vec::new();
    slot.drain_into(&mut buf);
    buf
}

#[test]
fn sub_reply_format() {
    let r = encode_sub_reply("subscribe", "news", 1);
    assert_eq!(r, b"*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
}

#[test]
fn sub_reply_zero_count() {
    let r = encode_sub_reply("unsubscribe", "", 0);
    assert_eq!(r, b"*3\r\n$11\r\nunsubscribe\r\n$0\r\n\r\n:0\r\n");
}

#[test]
fn publish_delivers_to_subscriber() {
    let pubsub = Arc::new(PubSub::new());
    let (notifier, slot) = make_sub(0, 1);

    pubsub.subscribe("news", Arc::clone(&slot));
    let count = pubsub.publish("news", "hello");
    assert_eq!(count, 1);

    // Small fan-out takes the per-subscriber queue path.
    assert!(fanout_frames(&notifier).is_empty());
    assert_eq!(
        drain(&slot),
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n".to_vec()
    );
}

#[test]
fn publish_no_subscribers_returns_zero() {
    let pubsub = Arc::new(PubSub::new());
    assert_eq!(pubsub.publish("empty", "msg"), 0);
}

#[test]
fn publish_to_multiple_subscribers() {
    let pubsub = Arc::new(PubSub::new());
    let (n1, s1) = make_sub(0, 1);
    let (n2, s2) = make_sub(1, 2);

    pubsub.subscribe("ch", Arc::clone(&s1));
    pubsub.subscribe("ch", Arc::clone(&s2));

    let count = pubsub.publish("ch", "hi");
    assert_eq!(count, 2);
    // Two subscribers across two workers stay below the grouping
    // threshold: each gets its own queue push.
    assert!(fanout_frames(&n1).is_empty());
    assert!(fanout_frames(&n2).is_empty());
    assert!(!drain(&s1).is_empty());
    assert!(!drain(&s2).is_empty());
}

#[test]
fn subscribers_on_one_worker_share_a_single_entry() {
    let pubsub = Arc::new(PubSub::new());
    let (notifier, first) = make_sub(3, 1);

    // Nine subscribers on one worker cross the grouping threshold
    // (8 per distinct worker): the publisher hands that worker one entry
    // instead of nine per-subscriber pushes.
    pubsub.subscribe("ch", Arc::clone(&first));
    for token in 2..=9 {
        let poll = Poll::new().unwrap();
        let extra = make_notifier(&poll, 3);
        let slot = make_slot(&extra, token);
        pubsub.subscribe("ch", slot);
    }

    assert_eq!(pubsub.publish("ch", "hi"), 9);
    assert_eq!(fanout_frames(&notifier).len(), 1);
}

#[test]
fn fanout_below_threshold_stays_per_subscriber() {
    let pubsub = Arc::new(PubSub::new());
    let (notifier, first) = make_sub(3, 1);

    // Exactly at the threshold: still per-subscriber pushes.
    pubsub.subscribe("ch", Arc::clone(&first));
    for token in 2..=8 {
        let poll = Poll::new().unwrap();
        let extra = make_notifier(&poll, 3);
        let slot = make_slot(&extra, token);
        pubsub.subscribe("ch", slot);
    }

    assert_eq!(pubsub.publish("ch", "hi"), 8);
    assert!(fanout_frames(&notifier).is_empty());
    assert!(!drain(&first).is_empty());
}

#[test]
fn unsubscribe_stops_delivery() {
    let pubsub = Arc::new(PubSub::new());
    let (notifier, slot) = make_sub(0, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.unsubscribe("ch", &slot);

    let count = pubsub.publish("ch", "msg");
    assert_eq!(count, 0);
    assert!(fanout_frames(&notifier).is_empty());
    assert!(drain(&slot).is_empty());
}

#[test]
fn publish_to_different_channel_not_delivered() {
    let pubsub = Arc::new(PubSub::new());
    let (notifier, slot) = make_sub(0, 1);

    pubsub.subscribe("sports", Arc::clone(&slot));
    pubsub.publish("news", "breaking");

    assert!(fanout_frames(&notifier).is_empty());
    assert!(drain(&slot).is_empty());
}

#[test]
fn message_content_identical_across_subscribers() {
    let pubsub = Arc::new(PubSub::new());
    let (_, s1) = make_sub(0, 1);
    let (_, s2) = make_sub(1, 2);

    pubsub.subscribe("ch", Arc::clone(&s1));
    pubsub.subscribe("ch", Arc::clone(&s2));
    pubsub.publish("ch", "payload");

    assert_eq!(drain(&s1), drain(&s2));
}

#[test]
fn psubscribe_wildcard_delivers() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.psubscribe("news.*", Arc::clone(&slot));
    let count = pubsub.publish("news.sports", "goal");
    assert_eq!(count, 1);

    // Pattern subscribers keep the per-slot path: their frames embed the
    // matched pattern, which only the publisher knows.
    let buf = drain(&slot);
    assert!(buf.starts_with(b"*4\r\n$8\r\npmessage\r\n"));
    assert!(buf.windows(4).any(|w| w == b"goal"));
}

#[test]
fn psubscribe_no_match_not_delivered() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.psubscribe("sports.*", Arc::clone(&slot));
    pubsub.publish("news.world", "update");

    assert!(drain(&slot).is_empty());
}

#[test]
fn punsubscribe_stops_pattern_delivery() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.psubscribe("ch.*", Arc::clone(&slot));
    pubsub.punsubscribe("ch.*", &slot);
    let count = pubsub.publish("ch.anything", "msg");
    assert_eq!(count, 0);
}

#[test]
fn pattern_and_exact_both_delivered() {
    let pubsub = Arc::new(PubSub::new());
    let (_, exact) = make_sub(0, 1);
    let (_, pattern) = make_sub(1, 2);

    pubsub.subscribe("ch", Arc::clone(&exact));
    pubsub.psubscribe("c*", Arc::clone(&pattern));

    let count = pubsub.publish("ch", "msg");
    assert_eq!(count, 2);
    assert!(!drain(&exact).is_empty());
    assert!(!drain(&pattern).is_empty());
}

#[test]
fn active_channels_lists_subscribed() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.subscribe("alpha", Arc::clone(&slot));
    pubsub.subscribe("beta", Arc::clone(&slot));

    let mut channels = pubsub.active_channels(None);
    channels.sort();
    assert_eq!(channels, vec!["alpha", "beta"]);
}

#[test]
fn active_channels_with_pattern_filter() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.subscribe("news.sports", Arc::clone(&slot));
    pubsub.subscribe("news.tech", Arc::clone(&slot));
    pubsub.subscribe("weather", Arc::clone(&slot));

    let mut channels = pubsub.active_channels(Some("news.*"));
    channels.sort();
    assert_eq!(channels, vec!["news.sports", "news.tech"]);
}

#[test]
fn active_channels_empty_after_unsubscribe() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.unsubscribe("ch", &slot);
    assert!(pubsub.active_channels(None).is_empty());
}

#[test]
fn numsub_counts_correctly() {
    let pubsub = Arc::new(PubSub::new());
    let (_, s1) = make_sub(0, 1);
    let (_, s2) = make_sub(1, 2);

    pubsub.subscribe("ch", Arc::clone(&s1));
    pubsub.subscribe("ch", Arc::clone(&s2));

    let result = pubsub.numsub(&["ch", "missing"]);
    assert_eq!(
        result,
        vec![("ch".to_string(), 2), ("missing".to_string(), 0)]
    );
}

#[test]
fn numpat_counts_pattern_subscriptions() {
    let pubsub = Arc::new(PubSub::new());
    let (_, s1) = make_sub(0, 1);
    let (_, s2) = make_sub(1, 2);

    pubsub.psubscribe("a*", Arc::clone(&s1));
    pubsub.psubscribe("b*", Arc::clone(&s2));
    assert_eq!(pubsub.numpat(), 2);

    pubsub.punsubscribe("a*", &s1);
    assert_eq!(pubsub.numpat(), 1);
}

#[test]
fn drain_twice_second_is_empty() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.publish("ch", "msg");

    assert!(!drain(&slot).is_empty());
    assert!(drain(&slot).is_empty());
}

#[test]
fn local_map_tracks_registered_subscribers() {
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll, 0);

    notifier.register_local("ch", 7);
    notifier.register_local("ch", 7); // duplicate register is idempotent
    notifier.register_local("ch", 9);
    notifier.register_local("other", 7);

    assert_eq!(
        notifier.local_subscribers("ch"),
        Some(vec![7, 9])
    );
    assert_eq!(notifier.local_subscribers("other"), Some(vec![7]));
    assert_eq!(notifier.local_subscribers("missing"), None);

    notifier.unregister_local("ch", 7);
    assert_eq!(notifier.local_subscribers("ch"), Some(vec![9]));
    notifier.unregister_local("ch", 9);
    // The channel key disappears with its last subscriber.
    assert_eq!(notifier.local_subscribers("ch"), None);
}

#[test]
fn concurrent_push_and_drain_never_wraps_queue_length() {
    let (_, slot) = make_sub(0, 1);
    let producers = 4;
    let per_producer = 25_000;

    std::thread::scope(|scope| {
        for _ in 0..producers {
            let slot = Arc::clone(&slot);
            scope.spawn(move || {
                for _ in 0..per_producer {
                    slot.push(Arc::from(&b"x"[..]));
                }
            });
        }

        let slot = Arc::clone(&slot);
        scope.spawn(move || {
            let mut out = Vec::new();
            while slot.queue_len() != 0 {
                slot.drain_into_limit(&mut out, 4096);
                out.clear();
                assert!(slot.queue_len() <= producers * per_producer);
                std::hint::spin_loop();
            }
        });
    });

    let mut out = Vec::new();
    slot.drain_into(&mut out);
    assert_eq!(slot.queue_len(), 0);
}

#[test]
fn multiple_publishes_queue_in_order() {
    let pubsub = Arc::new(PubSub::new());
    let (_, slot) = make_sub(0, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.publish("ch", "first");
    pubsub.publish("ch", "second");
    pubsub.publish("ch", "third");

    let buf = drain(&slot);
    let s = std::str::from_utf8(&buf).unwrap();
    assert!(s.find("first").unwrap() < s.find("second").unwrap());
    assert!(s.find("second").unwrap() < s.find("third").unwrap());
}

/// Full delivery path through a real worker event loop: SUBSCRIBE over TCP,
/// PUBLISH from a second connection, frame arriving on the subscriber's
/// socket. Before the per-worker fan-out this path was only ever exercised
/// by benchmarks.
#[test]
fn end_to_end_delivery_through_worker_loop() {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let store = Arc::new(fyro_db::storage::store::Store::with_config(2, 64));
    let pubsub = Arc::new(PubSub::new());
    let mut worker_handles: Vec<std::thread::JoinHandle<()>> = Vec::new();
    for idx in 0..2 {
        let store = Arc::clone(&store);
        let pubsub = Arc::clone(&pubsub);
        worker_handles.push(
            std::thread::Builder::new()
                .name(format!("test-worker-{idx}"))
                .stack_size(512 * 1024)
                .spawn(move || {
                    fyro_db::worker::run_worker(&store, &pubsub, port, "127.0.0.1", None, idx)
                })
                .unwrap(),
        );
    }
    // Wait for the listeners to come up.
    std::thread::sleep(Duration::from_millis(500));

    let mut sub = TcpStream::connect(("127.0.0.1", port)).unwrap();
    sub.set_nodelay(true).unwrap();
    sub.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut sub_r = BufReader::new(sub.try_clone().unwrap());
    sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nc1\r\n").unwrap();
    // Confirmation: *3\r\n$9\r\nsubscribe\r\n$2\r\nc1\r\n:1\r\n
    for _ in 0..6 {
        let mut line = String::new();
        sub_r.read_line(&mut line).unwrap();
        if line.starts_with(':') {
            break;
        }
    }

    let mut publisher = TcpStream::connect(("127.0.0.1", port)).unwrap();
    publisher.set_nodelay(true).unwrap();
    publisher
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut pub_r = BufReader::new(publisher.try_clone().unwrap());
    publisher
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$2\r\nc1\r\n$5\r\nhello\r\n")
        .unwrap();
    let mut reply = String::new();
    pub_r.read_line(&mut reply).unwrap();
    assert_eq!(reply.trim_end(), ":1", "publish must report one subscriber");

    // Delivery frame: *3\r\n$7\r\nmessage\r\n$2\r\nc1\r\n$5\r\nhello\r\n —
    // seven lines, the payload trailing its length header.
    let mut delivered = String::new();
    for _ in 0..7 {
        let mut line = String::new();
        sub_r.read_line(&mut line).unwrap();
        delivered.push_str(&line);
        if line.starts_with("hello") {
            break;
        }
    }
    assert!(delivered.contains("message"));
    assert!(delivered.contains("c1"));
    assert!(delivered.contains("hello"));

    // Unsubscribe, publish again: no further delivery.
    sub.write_all(b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$2\r\nc1\r\n")
        .unwrap();
    let mut line = String::new();
    for _ in 0..6 {
        sub_r.read_line(&mut line).unwrap();
        if line.starts_with(':') {
            break;
        }
    }
    publisher
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$2\r\nc1\r\n$5\r\nagain\r\n")
        .unwrap();
    let mut reply = String::new();
    pub_r.read_line(&mut reply).unwrap();
    assert_eq!(reply.trim_end(), ":0");

    sub.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
    let mut unexpected = String::new();
    match sub_r.read_line(&mut unexpected) {
        Ok(0) | Err(_) => {}
        Ok(_) => panic!("delivery after unsubscribe: {unexpected:?}"),
    }

    // High fan-out on few workers takes the grouped path: 30 subscribers
    // across 2 workers is 15 per worker, past the grouping threshold.
    let mut subs = Vec::new();
    for _ in 0..30 {
        let mut c = TcpStream::connect(("127.0.0.1", port)).unwrap();
        c.set_nodelay(true).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut r = BufReader::new(c.try_clone().unwrap());
        c.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nc2\r\n").unwrap();
        for _ in 0..6 {
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            if line.starts_with(':') {
                break;
            }
        }
        subs.push((c, r));
    }
    publisher
        .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$2\r\nc2\r\n$5\r\nburst\r\n")
        .unwrap();
    let mut reply = String::new();
    pub_r.read_line(&mut reply).unwrap();
    assert_eq!(reply.trim_end(), ":30");

    for (c, r) in &mut subs {
        let mut delivered = String::new();
        for _ in 0..7 {
            let mut line = String::new();
            r.read_line(&mut line).unwrap();
            delivered.push_str(&line);
            if line.starts_with("burst") {
                break;
            }
        }
        assert!(
            delivered.contains("message") && delivered.contains("burst"),
            "grouped delivery missing: {delivered:?}"
        );
        let _ = c;
    }

    // The workers block in poll() with no timeout when idle; nothing in this
    // test can wake them after shutdown is flagged, so joining would hang.
    // They are intentionally left running: the test binary's process exit
    // reclaims them.
    fyro_db::worker::initiate_shutdown();
}
