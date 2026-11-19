use fyro_db::pubsub::{PubSub, SubSlot, WorkerNotifier, frame::encode_batch};
use std::sync::Arc;

fn expected(channel: &str, messages: &[&str]) -> Vec<u8> {
   let mut out = Vec::new();
   for message in messages {
      out.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n");
      for field in [channel, *message] {
         out.extend_from_slice(format!("${}\r\n", field.len()).as_bytes());
         out.extend_from_slice(field.as_bytes());
         out.extend_from_slice(b"\r\n");
      }
   }
   out
}

#[test]
fn long_channels_and_payload_boundaries_keep_complete_batch_frames() {
   #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
   {
      assert!(std::mem::size_of::<SubSlot>() <= 384);
      assert!(std::mem::size_of::<WorkerNotifier>() <= 896);
   }
   for length in [0, 1, 9, 10, 40, 41, 63, 64, 65, 99, 100, 1024] {
      let channel = "c".repeat(length);
      for bytes in [0, 5, 64, 1024, 10 * 1024] {
         let payload = "p".repeat(bytes);
         for count in [1, 2, 3, 65, 512] {
            let messages = vec![payload.as_str(); count];
            assert_eq!(
               &*encode_batch(&channel, &messages),
               expected(&channel, &messages)
            );
         }
      }
   }
}

#[test]
fn mixed_frames_and_cache_changes_preserve_every_byte() {
   for channel in ["", "news:\0\r\né", &"long".repeat(100)] {
      for messages in [
         vec!["", "a", "two", "\0\r\né"],
         vec!["same", "diff"],
         vec!["same", "same"],
      ] {
         assert_eq!(
            &*encode_batch(channel, &messages),
            expected(channel, &messages)
         );
      }
   }
   let first = encode_batch("a", &["payload"; 64]);
   let again = encode_batch("a", &["payload"; 64]);
   assert!(Arc::ptr_eq(&first, &again));
   assert_eq!(
      &*encode_batch("b", &["payload"; 64]),
      expected("b", &["payload"; 64])
   );
   assert_eq!(
      &*encode_batch("a", &["payloae"; 64]),
      expected("a", &["payloae"; 64])
   );
}

#[test]
fn empty_audience_needs_no_frame_even_with_a_long_channel() {
   let pubsub = PubSub::new();
   assert_eq!(
      pubsub.publish_batch(&"channel".repeat(1000), &["payload"; 512]),
      0
   );
}

#[test]
fn oversized_frames_do_not_remain_in_the_thread_cache() {
   let payload = "p".repeat(9 * 1024 * 1024);
   let frame = encode_batch("large", &[payload.as_str()]);
   assert_eq!(Arc::strong_count(&frame), 1);
   assert_eq!(&*frame, expected("large", &[payload.as_str()]));
   let payload = "p".repeat(10 * 1024);
   let frame = encode_batch("normal", &[payload.as_str(); 512]);
   let again = encode_batch("normal", &[payload.as_str(); 512]);
   assert!(Arc::ptr_eq(&frame, &again));
}
#[test]
fn public_empty_batches_keep_the_text_api_and_encode_no_messages() {
   use fyro_db::pubsub::frame::{encode_batch, encode_message, encode_pmessage};
   let message: fn(&str, &str) -> std::sync::Arc<[u8]> = encode_message;
   let pattern: fn(&str, &str, &str) -> std::sync::Arc<[u8]> = encode_pmessage;
   assert!(encode_batch("channel", &[]).is_empty());
   assert!(!message("channel", "value").is_empty());
   assert!(!pattern("*", "channel", "value").is_empty());
}
