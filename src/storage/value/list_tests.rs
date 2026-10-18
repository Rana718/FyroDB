use super::*;

#[test]
fn promotes_and_demotes_across_the_threshold() {
   let mut l = ListValue::new();
   let mut model: VecDeque<String> = VecDeque::new();
   for i in 0..400 {
      let v = format!("v{i}");
      if i % 2 == 0 {
         l.push_back(&v);
         model.push_back(v);
      } else {
         l.push_front(&v);
         model.push_front(v);
      }
   }
   assert!(matches!(l, ListValue::Packed(_)));
   assert!(l.iter().eq(model.iter().map(String::as_str)));
   assert!(l.iter().rev().eq(model.iter().rev().map(String::as_str)));
   assert_eq!(
      l.range(10, 5).collect::<Vec<_>>(),
      model.range(10..15).map(String::as_str).collect::<Vec<_>>()
   );
   while l.len() > 20 {
      assert_eq!(l.pop_front(), model.pop_front());
      assert_eq!(l.pop_back(), model.pop_back());
   }
   assert!(matches!(l, ListValue::Small(_)));
   assert!(l.iter().eq(model.iter().map(String::as_str)));
   assert!(l.set(3, "x"));
   model[3] = "x".into();
   l.insert(2, "ins");
   model.insert(2, "ins".into());
   assert_eq!(l.position("ins"), Some(2));
   assert_eq!(l.remove_matching("x", 1, false), 1);
   model.retain(|v| v != "x");
   assert!(l.iter().eq(model.iter().map(String::as_str)));
   l.trim(1, 3);
   assert_eq!(l.len(), 3);
   assert_eq!(l.get(0), model.get(1).map(String::as_str));
}
