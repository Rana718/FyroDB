#[macro_export]
macro_rules! write_sub_replies {
   ($out:expr, $kind:expr, $items:expr, $total:expr) => {{
      let items: &[String] = $items;
      let total: usize = $total;
      for (i, item) in items.iter().enumerate() {
         let count = total - items.len() + i + 1;
         $out.extend_from_slice(&encode_sub_reply($kind, item, count));
      }
   }};
}

#[macro_export]
macro_rules! write_unsub_replies {
   ($out:expr, $kind:expr, $targets:expr, $removed_set:expr, $remaining:expr) => {{
      let mut remaining: usize = $remaining;
      for item in ($targets).iter() {
         if ($removed_set).contains(item) {
            remaining -= 1;
            $out.extend_from_slice(&encode_sub_reply($kind, item, remaining));
         }
      }
      if ($targets).is_empty() && remaining == 0 {
         $out.extend_from_slice(&encode_sub_reply($kind, "", 0));
      }
   }};
}
