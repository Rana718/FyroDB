use fyro_db::pubsub::{PubSub, SubSlot, WorkerNotifier, encode_sub_reply};
use mio::{Poll, Token, Waker};
use std::sync::Arc;

fn make_notifier(poll: &Poll) -> Arc<WorkerNotifier> {
    let waker = Arc::new(Waker::new(poll.registry(), Token(usize::MAX)).unwrap());
    WorkerNotifier::new(waker)
}

fn make_slot(notifier: &Arc<WorkerNotifier>, token: usize) -> Arc<SubSlot> {
    Arc::new(SubSlot::new(token, Arc::clone(notifier)))
}

fn make_slot_own_poll(token: usize) -> Arc<SubSlot> {
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    Arc::new(SubSlot::new(token, notifier))
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
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.subscribe("news", Arc::clone(&slot));
    let count = pubsub.publish("news", "hello");
    assert_eq!(count, 1);

    let buf = drain(&slot);
    assert_eq!(buf, b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n");
}

#[test]
fn publish_no_subscribers_returns_zero() {
    let pubsub = Arc::new(PubSub::new());
    assert_eq!(pubsub.publish("empty", "msg"), 0);
}

#[test]
fn publish_to_multiple_subscribers() {
    let pubsub = Arc::new(PubSub::new());
    let s1 = make_slot_own_poll(1);
    let s2 = make_slot_own_poll(2);

    pubsub.subscribe("ch", Arc::clone(&s1));
    pubsub.subscribe("ch", Arc::clone(&s2));

    let count = pubsub.publish("ch", "hi");
    assert_eq!(count, 2);
    assert!(!drain(&s1).is_empty());
    assert!(!drain(&s2).is_empty());
}

#[test]
fn unsubscribe_stops_delivery() {
    let pubsub = Arc::new(PubSub::new());
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.unsubscribe("ch", &slot);

    let count = pubsub.publish("ch", "msg");
    assert_eq!(count, 0);
    assert!(drain(&slot).is_empty());
}

#[test]
fn publish_to_different_channel_not_delivered() {
    let pubsub = Arc::new(PubSub::new());
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.subscribe("sports", Arc::clone(&slot));
    pubsub.publish("news", "breaking");

    assert!(drain(&slot).is_empty());
}

#[test]
fn message_content_identical_across_subscribers() {
    let pubsub = Arc::new(PubSub::new());
    let s1 = make_slot_own_poll(1);
    let s2 = make_slot_own_poll(2);

    pubsub.subscribe("ch", Arc::clone(&s1));
    pubsub.subscribe("ch", Arc::clone(&s2));
    pubsub.publish("ch", "payload");

    assert_eq!(drain(&s1), drain(&s2));
}

#[test]
fn psubscribe_wildcard_delivers() {
    let pubsub = Arc::new(PubSub::new());
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.psubscribe("news.*", Arc::clone(&slot));
    let count = pubsub.publish("news.sports", "goal");
    assert_eq!(count, 1);

    let buf = drain(&slot);
    assert!(buf.starts_with(b"*4\r\n$8\r\npmessage\r\n"));
    assert!(buf.windows(4).any(|w| w == b"goal"));
}

#[test]
fn psubscribe_no_match_not_delivered() {
    let pubsub = Arc::new(PubSub::new());
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.psubscribe("sports.*", Arc::clone(&slot));
    pubsub.publish("news.world", "update");

    assert!(drain(&slot).is_empty());
}

#[test]
fn punsubscribe_stops_pattern_delivery() {
    let pubsub = Arc::new(PubSub::new());
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.psubscribe("ch.*", Arc::clone(&slot));
    pubsub.punsubscribe("ch.*", &slot);
    let count = pubsub.publish("ch.anything", "msg");
    assert_eq!(count, 0);
}

#[test]
fn pattern_and_exact_both_delivered() {
    let pubsub = Arc::new(PubSub::new());
    let exact = make_slot_own_poll(1);
    let pattern = make_slot_own_poll(2);

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
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.subscribe("alpha", Arc::clone(&slot));
    pubsub.subscribe("beta", Arc::clone(&slot));

    let mut channels = pubsub.active_channels(None);
    channels.sort();
    assert_eq!(channels, vec!["alpha", "beta"]);
}

#[test]
fn active_channels_with_pattern_filter() {
    let pubsub = Arc::new(PubSub::new());
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

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
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.unsubscribe("ch", &slot);
    assert!(pubsub.active_channels(None).is_empty());
}

#[test]
fn numsub_counts_correctly() {
    let pubsub = Arc::new(PubSub::new());
    let s1 = make_slot_own_poll(1);
    let s2 = make_slot_own_poll(2);

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
    let s1 = make_slot_own_poll(1);
    let s2 = make_slot_own_poll(2);

    pubsub.psubscribe("a*", Arc::clone(&s1));
    pubsub.psubscribe("b*", Arc::clone(&s2));
    assert_eq!(pubsub.numpat(), 2);

    pubsub.punsubscribe("a*", &s1);
    assert_eq!(pubsub.numpat(), 1);
}

#[test]
fn drain_twice_second_is_empty() {
    let pubsub = Arc::new(PubSub::new());
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.publish("ch", "msg");

    let first = drain(&slot);
    let second = drain(&slot);
    assert!(!first.is_empty());
    assert!(second.is_empty());
}

#[test]
fn concurrent_push_and_drain_never_wraps_queue_length() {
    let slot = make_slot_own_poll(1);
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
    let poll = Poll::new().unwrap();
    let notifier = make_notifier(&poll);
    let slot = make_slot(&notifier, 1);

    pubsub.subscribe("ch", Arc::clone(&slot));
    pubsub.publish("ch", "first");
    pubsub.publish("ch", "second");
    pubsub.publish("ch", "third");

    let buf = drain(&slot);
    let s = std::str::from_utf8(&buf).unwrap();
    assert!(s.find("first").unwrap() < s.find("second").unwrap());
    assert!(s.find("second").unwrap() < s.find("third").unwrap());
}
