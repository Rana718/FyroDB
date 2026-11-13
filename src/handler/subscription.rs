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

   for &ch in &parts[1..] {
      let total = {
         let (ch_set, pat_set) = sub_sets_mut(conn);
         if !ch_set.contains(ch) {
            ch_set.insert(pubsub.subscribe_shared(ch, Arc::clone(&slot)));
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
         remaining -= 1;
      }
      encode_sub_reply("unsubscribe", ch, remaining, &mut conn.parser.wbuf);
   };

   if parts.len() <= 1 {
      let targets = std::mem::take(&mut channels.keys);

      if targets.is_empty() {
         unsubscribe_one("", false);
      } else {
         for ch in targets.iter() {
            unsubscribe_one(ch, true);
         }
      }
   } else {
      for &ch in &parts[1..] {
         unsubscribe_one(ch, channels.remove(ch));
      }
   }

   if channels.is_empty() && patterns.is_empty() {
      conn.mode = channels
         .transaction
         .take()
         .map_or(ConnMode::Normal, ConnMode::Transaction);
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
         if !pat_set.contains(pat) {
            pat_set.insert(pubsub.psubscribe_shared(pat, Arc::clone(&slot)));
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
      conn.mode = channels
         .transaction
         .take()
         .map_or(ConnMode::Normal, ConnMode::Transaction);
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
         std::mem::take(&mut channels.keys),
         std::mem::take(patterns),
      ),
      ConnMode::Normal | ConnMode::Transaction(_) => return,
   };
   for ch in &channels {
      pubsub.unsubscribe(ch, &slot);
   }
   for pat in &patterns {
      pubsub.punsubscribe(pat, &slot);
   }
   conn.mode = ConnMode::Normal;
}

pub fn ensure_slot(conn: &mut Conn<'_>) -> Arc<SubSlot> {
   if !matches!(conn.mode, ConnMode::Subscribed { .. }) {
      let sock_ref = socket2::SockRef::from(&conn.stream);
      let _ = sock_ref.set_send_buffer_size(1024 * 1024);
      let slot = Arc::new(SubSlot::new(conn.token, Arc::clone(conn.notifier)));
      let transaction = match std::mem::replace(&mut conn.mode, ConnMode::Normal) {
         ConnMode::Transaction(state) => Some(state),
         _ => None,
      };
      conn.mode = ConnMode::Subscribed {
         slot,
         channels: Box::new(super::conn::Channels {
            keys: HashSet::new(),
            transaction,
         }),
         patterns: HashSet::new(),
      };
   }
   match &conn.mode {
      ConnMode::Subscribed { slot, .. } => Arc::clone(slot),
      ConnMode::Normal | ConnMode::Transaction(_) => unreachable!(),
   }
}

fn sub_sets_mut<'c>(
   conn: &'c mut Conn<'_>,
) -> (&'c mut HashSet<Arc<str>>, &'c mut HashSet<Arc<str>>) {
   let ConnMode::Subscribed {
      channels, patterns, ..
   } = &mut conn.mode
   else {
      unreachable!()
   };
   (channels, patterns)
}
