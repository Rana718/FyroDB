use fyro_db::pubsub::{PubSub, SubSlot, WorkerNotifier};
use mio::{Poll, Token, Waker};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

fn notifier(waker: &Arc<Waker>, worker_index: usize) -> Arc<WorkerNotifier> {
   WorkerNotifier::new(Arc::clone(waker), worker_index)
}

fn bench_new_channels() {
   let poll = Poll::new().unwrap();
   let waker = Arc::new(Waker::new(poll.registry(), Token(0)).unwrap());

   for (count, label) in [(1_000, "1k"), (5_000, "5k"), (20_000, "20k")] {
      let notifier = notifier(&waker, 0);
      let slot = Arc::new(SubSlot::new(0, notifier));
      let channels: Vec<_> = (0..count).map(|i| format!("channel:{i}")).collect();
      let pubsub = PubSub::new();
      let start = Instant::now();
      for channel in &channels {
         pubsub.subscribe(black_box(channel), Arc::clone(&slot));
      }
      report(&format!("new channels ({label})"), count, start.elapsed());
   }
}

fn bench_additional_subscribers() {
   const SUBSCRIBERS: usize = 4_000;
   let poll = Poll::new().unwrap();
   let waker = Arc::new(Waker::new(poll.registry(), Token(0)).unwrap());
   let slots: Vec<_> = (0..SUBSCRIBERS)
      .map(|token| {
         let notifier = notifier(&waker, 0);
         Arc::new(SubSlot::new(token, notifier))
      })
      .collect();
   let pubsub = PubSub::new();

   let start = Instant::now();
   for slot in &slots {
      pubsub.subscribe("shared-channel", Arc::clone(slot));
   }
   report("additional subscribers", SUBSCRIBERS, start.elapsed());
}

fn bench_duplicate_subscriptions() {
   const SUBSCRIPTIONS: usize = 100_000;
   let poll = Poll::new().unwrap();
   let waker = Arc::new(Waker::new(poll.registry(), Token(0)).unwrap());
   let notifier = notifier(&waker, 0);
   let slot = Arc::new(SubSlot::new(0, notifier));
   let pubsub = PubSub::new();
   pubsub.subscribe("duplicate-channel", Arc::clone(&slot));

   let start = Instant::now();
   for _ in 0..SUBSCRIPTIONS {
      pubsub.subscribe("duplicate-channel", Arc::clone(&slot));
   }
   report("duplicate subscriptions", SUBSCRIPTIONS, start.elapsed());
}

fn bench_distinct_worker_subscribers() {
   const WORKERS: usize = 128;
   let poll = Poll::new().unwrap();
   let waker = Arc::new(Waker::new(poll.registry(), Token(0)).unwrap());
   let slots: Vec<_> = (0..WORKERS)
      .map(|worker_index| {
         let notifier = notifier(&waker, worker_index);
         Arc::new(SubSlot::new(worker_index, notifier))
      })
      .collect();
   let orders = [
      ("worker indexes ascending", (0..WORKERS).collect::<Vec<_>>()),
      ("worker indexes descending", (0..WORKERS).rev().collect()),
   ];

   for (label, order) in &orders {
      let pubsub = PubSub::new();
      let start = Instant::now();
      for &worker_index in order {
         pubsub.subscribe("shared-worker-channel", Arc::clone(&slots[worker_index]));
      }
      report(label, WORKERS, start.elapsed());
      assert_eq!(
         pubsub.numsub(&["shared-worker-channel"]),
         vec![("shared-worker-channel", WORKERS)]
      );
   }
}

fn bench_pubsub_channels(pubsub: &PubSub, channels: &[String], pattern: Option<&str>, label: &str) {
   let parts = match pattern {
      Some(pattern) => vec!["PUBSUB", "CHANNELS", pattern],
      None => vec!["PUBSUB", "CHANNELS"],
   };
   let mut out = Vec::new();
   fyro_db::handler::pubsub_cmds::pubsub_info(&parts, pubsub, &mut out);

   let mut actual = read_resp_bulk_array(&out);
   let mut expected: Vec<_> = channels
      .iter()
      .map(String::as_str)
      .filter(|channel| {
         pattern.is_none_or(|pattern| channel.starts_with(pattern.trim_end_matches('*')))
      })
      .collect();
   actual.sort_unstable();
   expected.sort_unstable();
   assert_eq!(actual, expected);

   bench_pubsub_handler(label, &parts, pubsub, &mut out, 50);
}

fn bench_pubsub_numsub(pubsub: &PubSub) {
   let requested = [
      "channel:0",
      "missing",
      "channel:19999",
      "channel:0",
      "channel:9999",
      "missing",
      "channel:19999",
      "channel:0",
   ];
   let parts: Vec<_> = ["PUBSUB", "NUMSUB"].into_iter().chain(requested).collect();
   let expected = [
      ("channel:0", 1),
      ("missing", 0),
      ("channel:19999", 1),
      ("channel:0", 1),
      ("channel:9999", 1),
      ("missing", 0),
      ("channel:19999", 1),
      ("channel:0", 1),
   ];
   let mut out = Vec::new();
   fyro_db::handler::pubsub_cmds::pubsub_info(&parts, pubsub, &mut out);
   assert_resp_numsub(&out, &expected);

   bench_pubsub_handler("PUBSUB NUMSUB", &parts, pubsub, &mut out, 100_000);
}

fn bench_pubsub_handler(
   label: &str,
   parts: &[&str],
   pubsub: &PubSub,
   out: &mut Vec<u8>,
   operations: usize,
) {
   out.clear();
   let start = Instant::now();
   for _ in 0..operations {
      out.clear();
      fyro_db::handler::pubsub_cmds::pubsub_info(black_box(parts), black_box(pubsub), out);
      black_box(out.as_slice());
   }
   report(label, operations, start.elapsed());
}

fn read_resp_line<'a>(out: &'a [u8], offset: &mut usize) -> &'a [u8] {
   let end = *offset
      + out[*offset..]
         .windows(2)
         .position(|window| window == b"\r\n")
         .expect("RESP line terminator");
   let line = &out[*offset..end];
   *offset = end + 2;
   line
}

fn read_resp_array_len(out: &[u8], offset: &mut usize) -> usize {
   let header = read_resp_line(out, offset);
   assert_eq!(header.first(), Some(&b'*'));
   std::str::from_utf8(&header[1..]).unwrap().parse().unwrap()
}

fn read_resp_bulk<'a>(out: &'a [u8], offset: &mut usize) -> &'a [u8] {
   let header = read_resp_line(out, offset);
   assert_eq!(header.first(), Some(&b'$'));
   let len: usize = std::str::from_utf8(&header[1..]).unwrap().parse().unwrap();
   let start = *offset;
   let end = start + len;
   assert_eq!(&out[end..end + 2], b"\r\n");
   *offset = end + 2;
   &out[start..end]
}

fn read_resp_bulk_array(out: &[u8]) -> Vec<&str> {
   let mut offset = 0;
   let count = read_resp_array_len(out, &mut offset);
   let values = (0..count)
      .map(|_| std::str::from_utf8(read_resp_bulk(out, &mut offset)).unwrap())
      .collect();
   assert_eq!(offset, out.len());
   values
}

fn assert_resp_numsub(out: &[u8], expected: &[(&str, usize)]) {
   let mut offset = 0;
   assert_eq!(read_resp_array_len(out, &mut offset), expected.len() * 2);
   for &(channel, count) in expected {
      assert_eq!(read_resp_bulk(out, &mut offset), channel.as_bytes());
      let integer = read_resp_line(out, &mut offset);
      assert_eq!(integer.first(), Some(&b':'));
      assert_eq!(
         std::str::from_utf8(&integer[1..])
            .unwrap()
            .parse::<usize>()
            .unwrap(),
         count
      );
   }
   assert_eq!(offset, out.len());
}

fn report(label: &str, operations: usize, elapsed: std::time::Duration) {
   println!(
      "{label:<26} {operations:>8} ops  {:>10.2} ms  {:>10.1} ops/sec",
      elapsed.as_secs_f64() * 1_000.0,
      operations as f64 / elapsed.as_secs_f64(),
   );
}

fn main() {
   bench_new_channels();
   bench_additional_subscribers();
   bench_duplicate_subscriptions();
   bench_distinct_worker_subscribers();

   const CHANNELS: usize = 20_000;
   let poll = Poll::new().unwrap();
   let waker = Arc::new(Waker::new(poll.registry(), Token(0)).unwrap());
   let notifier = notifier(&waker, 0);
   let slot = Arc::new(SubSlot::new(0, notifier));
   let channels: Vec<_> = (0..CHANNELS).map(|i| format!("channel:{i}")).collect();
   let pubsub = PubSub::new();
   for channel in &channels {
      pubsub.subscribe(channel, Arc::clone(&slot));
   }

   bench_pubsub_channels(&pubsub, &channels, None, "PUBSUB CHANNELS");
   bench_pubsub_channels(
      &pubsub,
      &channels,
      Some("channel:1*"),
      "PUBSUB CHANNELS filter",
   );
   bench_pubsub_numsub(&pubsub);
}
