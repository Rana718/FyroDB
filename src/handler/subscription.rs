use super::conn::{Conn, ConnMode};
use crate::pubsub::{SubSlot, encode_sub_reply};
use crate::utils::resp;
use foldhash::{HashSet, HashSetExt};
use std::sync::Arc;

pub fn handle_subscribe(conn: &mut Conn<'_>, parts: &[&str]) {
   if parts.len() < 2 {
      resp::write_wrong_args(&mut conn.parser.wbuf, "subscribe");
      return;
   }
   let slot = ensure_slot(conn);
   let pubsub = conn.pubsub;
   let token = conn.token;
   let notifier = conn.notifier.as_ref();

   for &ch in &parts[1..] {
      let total = {
         let (ch_set, pat_set) = sub_sets_mut(conn);
         if ch_set.insert(ch.to_string()) {
            pubsub.subscribe(ch, Arc::clone(&slot));
            notifier.register_local(ch, token);
         }
         ch_set.len() + pat_set.len()
      };

      encode_sub_reply("subscribe", ch, total, &mut conn.parser.wbuf);
   }
}

pub fn handle_unsubscribe(conn: &mut Conn<'_>, parts: &[&str]) {
   let pubsub = conn.pubsub;
   let ConnMode::Subscribed {
      slot,
      channels,
      patterns,
   } = &mut conn.mode
   else {
      encode_sub_reply("unsubscribe", "", 0, &mut conn.parser.wbuf);
      return;
   };

   let mut remaining = channels.len() + patterns.len();
   let mut unsubscribe_one = |ch: &str, removed: bool| {
      if removed {
         pubsub.unsubscribe(ch, &*slot);
         conn.notifier.unregister_local(ch, conn.token);
         remaining -= 1;
      }
      encode_sub_reply("unsubscribe", ch, remaining, &mut conn.parser.wbuf);
   };

   if parts.len() <= 1 {
      let targets = std::mem::take(channels);

      if targets.is_empty() {
         unsubscribe_one("", false);
      } else {
         for ch in &targets {
            unsubscribe_one(ch, true);
         }
      }
   } else {
      for &ch in &parts[1..] {
         unsubscribe_one(ch, channels.remove(ch));
      }
   }

   if channels.is_empty() && patterns.is_empty() {
      conn.mode = ConnMode::Normal;
   }
}

pub fn handle_psubscribe(conn: &mut Conn<'_>, parts: &[&str]) {
   if parts.len() < 2 {
      resp::write_wrong_args(&mut conn.parser.wbuf, "psubscribe");
      return;
   }
   let slot = ensure_slot(conn);
   let pubsub = conn.pubsub;

   for &pat in &parts[1..] {
      let total = {
         let (ch_set, pat_set) = sub_sets_mut(conn);
         if pat_set.insert(pat.to_string()) {
            pubsub.psubscribe(pat, Arc::clone(&slot));
         }
         ch_set.len() + pat_set.len()
      };

      encode_sub_reply("psubscribe", pat, total, &mut conn.parser.wbuf);
   }
}

pub fn handle_punsubscribe(conn: &mut Conn<'_>, parts: &[&str]) {
   let pubsub = conn.pubsub;
   let ConnMode::Subscribed {
      slot,
      channels,
      patterns,
   } = &mut conn.mode
   else {
      encode_sub_reply("punsubscribe", "", 0, &mut conn.parser.wbuf);
      return;
   };

   let mut remaining = channels.len() + patterns.len();
   let mut punsubscribe_one = |pat: &str, removed: bool| {
      if removed {
         pubsub.punsubscribe(pat, &*slot);
         remaining -= 1;
      }
      encode_sub_reply("punsubscribe", pat, remaining, &mut conn.parser.wbuf);
   };

   if parts.len() <= 1 {
      let targets = std::mem::take(patterns);

      if targets.is_empty() {
         punsubscribe_one("", false);
      } else {
         for pat in targets {
            punsubscribe_one(&pat, true);
         }
      }
   } else {
      for &pat in &parts[1..] {
         punsubscribe_one(pat, patterns.remove(pat));
      }
   }

   if channels.is_empty() && patterns.is_empty() {
      conn.mode = ConnMode::Normal;
   }
}

pub fn do_full_unsubscribe(conn: &mut Conn<'_>) {
   let pubsub = conn.pubsub;
   let (slot, channels, patterns) = match &mut conn.mode {
      ConnMode::Subscribed {
         slot,
         channels,
         patterns,
      } => (
         Arc::clone(&*slot),
         std::mem::take(channels),
         std::mem::take(patterns),
      ),
      ConnMode::Normal => return,
   };
   for ch in &channels {
      pubsub.unsubscribe(ch, &slot);
      conn.notifier.unregister_local(ch, conn.token);
   }
   for pat in &patterns {
      pubsub.punsubscribe(pat, &slot);
   }
   conn.mode = ConnMode::Normal;
}

pub fn ensure_slot(conn: &mut Conn<'_>) -> Arc<SubSlot> {
   if let ConnMode::Normal = conn.mode {
      let slot = Arc::new(SubSlot::new(conn.token, Arc::clone(conn.notifier)));
      conn.mode = ConnMode::Subscribed {
         slot,
         channels: HashSet::new(),
         patterns: HashSet::new(),
      };
   }
   match &conn.mode {
      ConnMode::Subscribed { slot, .. } => Arc::clone(slot),
      ConnMode::Normal => unreachable!(),
   }
}

fn sub_sets_mut<'c>(conn: &'c mut Conn<'_>) -> (&'c mut HashSet<String>, &'c mut HashSet<String>) {
   let ConnMode::Subscribed {
      channels, patterns, ..
   } = &mut conn.mode
   else {
      unreachable!()
   };
   (channels, patterns)
}
