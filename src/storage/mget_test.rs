use super::store::Store;
use crate::commands::string::mget;
use std::sync::Barrier;

#[test]
fn replacements_preserve_one_mget_snapshot() {
   let store = Store::with_config_workers(16, usize::MAX, 6);
   let start = Barrier::new(5);
   for size in [7, 4096, 65536] {
      let values = ["a".repeat(size - 1), "b".repeat(size)];
      store.set_string("hot", &values[0], 0);
      store.set_string("marker", "x", 0);
      std::thread::scope(|scope| {
         for writer in 0..4 {
            let (store, start, values) = (&store, &start, &values);
            scope.spawn(move || {
               start.wait();
               for i in 0..4000 {
                  store.set_string("hot", &values[(writer + i) % 2], 0);
               }
            });
         }
         start.wait();
         for keys in [vec!["hot", "hot"], vec!["hot", "marker", "hot"], {
            let mut keys = vec!["marker"; 65];
            keys[0] = "hot";
            keys.push("hot");
            keys
         }] {
            let expected: Vec<_> = values
               .iter()
               .map(|value| {
                  let mut reply = Vec::new();
                  crate::utils::resp::write_array_header(&mut reply, keys.len());
                  for key in &keys {
                     let s = if *key == "hot" { value.as_str() } else { "x" };
                     reply.extend(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                  }
                  reply
               })
               .collect();
            let mut args = vec!["MGET"];
            args.extend(&keys);
            let mut reply = Vec::new();
            for _ in 0..128 {
               reply.clear();
               mget(&args, &store, &mut reply);
               assert!(
                  expected.contains(&reply),
                  "inconsistent {}-key MGET",
                  keys.len()
               );
            }
         }
      });
   }
}
