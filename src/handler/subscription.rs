use super::conn::{Conn, ConnMode};
use crate::pubsub::{SubSlot, encode_sub_reply};
use crate::utils::resp;
use crate::{write_sub_replies, write_unsub_replies};
use foldhash::{HashSet, HashSetExt};
use std::sync::Arc;

pub fn handle_subscribe(conn: &mut Conn, parts: &[&str]) {
    if parts.len() < 2 {
        resp::write_wrong_args(&mut conn.parser.wbuf, "subscribe");
        return;
    }
    let new_items: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
    let slot = ensure_slot(conn);

    let to_register: Vec<String> = {
        let (ch_set, _) = sub_sets_mut(conn);
        new_items
            .iter()
            .filter(|ch| ch_set.insert((*ch).clone()))
            .cloned()
            .collect()
    };
    for ch in &to_register {
        conn.pubsub.subscribe(ch, Arc::clone(&slot));
    }

    let total = sub_total(conn);
    write_sub_replies!(&mut conn.parser.wbuf, "subscribe", &new_items, total);
}

pub fn handle_unsubscribe(conn: &mut Conn, parts: &[&str]) {
    let (slot_ptr, ch_ptr, pat_ptr) = match &mut conn.mode {
        ConnMode::Subscribed {
            slot,
            channels,
            patterns,
        } => (
            slot as *const Arc<SubSlot>,
            channels as *mut HashSet<String>,
            patterns as *mut HashSet<String>,
        ),
        ConnMode::Normal => {
            conn.parser
                .wbuf
                .extend_from_slice(&encode_sub_reply("unsubscribe", "", 0));
            return;
        }
    };
    let slot = unsafe { &*slot_ptr };
    let channels = unsafe { &mut *ch_ptr };
    let patterns = unsafe { &mut *pat_ptr };

    let targets: Vec<String> = if parts.len() <= 1 {
        channels.iter().cloned().collect()
    } else {
        parts[1..].iter().map(|s| s.to_string()).collect()
    };

    let mut removed = HashSet::new();
    for ch in &targets {
        if channels.remove(ch) {
            conn.pubsub.unsubscribe(ch, slot);
            removed.insert(ch.clone());
        }
    }

    let remaining = channels.len() + patterns.len();
    write_unsub_replies!(
        &mut conn.parser.wbuf,
        "unsubscribe",
        &targets,
        &removed,
        remaining + removed.len()
    );

    if channels.is_empty() && patterns.is_empty() {
        conn.mode = ConnMode::Normal;
    }
}

pub fn handle_psubscribe(conn: &mut Conn, parts: &[&str]) {
    if parts.len() < 2 {
        resp::write_wrong_args(&mut conn.parser.wbuf, "psubscribe");
        return;
    }
    let new_items: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
    let slot = ensure_slot(conn);

    let to_register: Vec<String> = {
        let (_, pat_set) = sub_sets_mut(conn);
        new_items
            .iter()
            .filter(|p| pat_set.insert((*p).clone()))
            .cloned()
            .collect()
    };
    for pat in &to_register {
        conn.pubsub.psubscribe(pat, Arc::clone(&slot));
    }

    let total = sub_total(conn);
    write_sub_replies!(&mut conn.parser.wbuf, "psubscribe", &new_items, total);
}

pub fn handle_punsubscribe(conn: &mut Conn, parts: &[&str]) {
    let (slot_ptr, ch_ptr, pat_ptr) = match &mut conn.mode {
        ConnMode::Subscribed {
            slot,
            channels,
            patterns,
        } => (
            slot as *const Arc<SubSlot>,
            channels as *mut HashSet<String>,
            patterns as *mut HashSet<String>,
        ),
        ConnMode::Normal => {
            conn.parser
                .wbuf
                .extend_from_slice(&encode_sub_reply("punsubscribe", "", 0));
            return;
        }
    };
    let slot = unsafe { &*slot_ptr };
    let channels = unsafe { &mut *ch_ptr };
    let patterns = unsafe { &mut *pat_ptr };

    let targets: Vec<String> = if parts.len() <= 1 {
        patterns.iter().cloned().collect()
    } else {
        parts[1..].iter().map(|s| s.to_string()).collect()
    };

    let mut removed = HashSet::new();
    for pat in &targets {
        if patterns.remove(pat) {
            conn.pubsub.punsubscribe(pat, slot);
            removed.insert(pat.clone());
        }
    }

    let remaining = channels.len() + patterns.len();
    write_unsub_replies!(
        &mut conn.parser.wbuf,
        "punsubscribe",
        &targets,
        &removed,
        remaining + removed.len()
    );

    if channels.is_empty() && patterns.is_empty() {
        conn.mode = ConnMode::Normal;
    }
}

pub fn do_full_unsubscribe(conn: &mut Conn) {
    let (slot, channels, patterns) = match &mut conn.mode {
        ConnMode::Subscribed {
            slot,
            channels,
            patterns,
        } => (
            Arc::clone(slot),
            std::mem::take(channels),
            std::mem::take(patterns),
        ),
        ConnMode::Normal => return,
    };
    for ch in &channels {
        conn.pubsub.unsubscribe(ch, &slot);
    }
    for pat in &patterns {
        conn.pubsub.punsubscribe(pat, &slot);
    }
    conn.mode = ConnMode::Normal;
}

pub fn ensure_slot(conn: &mut Conn) -> Arc<SubSlot> {
    if let ConnMode::Normal = conn.mode {
        let slot = Arc::new(SubSlot::new(conn.token, Arc::clone(&conn.notifier)));
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

fn sub_sets_mut(conn: &mut Conn) -> (&mut HashSet<String>, &mut HashSet<String>) {
    match &mut conn.mode {
        ConnMode::Subscribed {
            channels, patterns, ..
        } => (channels, patterns),
        ConnMode::Normal => unreachable!(),
    }
}

fn sub_total(conn: &Conn) -> usize {
    match &conn.mode {
        ConnMode::Subscribed {
            channels, patterns, ..
        } => channels.len() + patterns.len(),
        ConnMode::Normal => 0,
    }
}
