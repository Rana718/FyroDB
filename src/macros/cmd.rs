#[macro_export]
macro_rules! parse_int {
   ($out:expr, $s:expr) => {
      match $s.parse::<i64>() {
         Ok(n) => n,
         Err(_) => {
            $crate::utils::resp::write_err($out, "value is not an integer");
            return;
         }
      }
   };
   ($out:expr, $s:expr, $t:ty) => {
      match $s.parse::<$t>() {
         Ok(n) => n,
         Err(_) => {
            $crate::utils::resp::write_err($out, "value is not an integer");
            return;
         }
      }
   };
}

#[macro_export]
macro_rules! parse_float {
   ($out:expr, $s:expr) => {
      match $s.parse::<f64>() {
         Ok(n) => n,
         Err(_) => {
            $crate::utils::resp::write_err($out, "value is not a float");
            return;
         }
      }
   };
}

#[macro_export]
macro_rules! wt {
   ($out:expr, $expr:expr) => {
      match $expr {
         Ok(v) => v,
         Err(_) => {
            $crate::utils::resp::write_wrong_type($out);
            return;
         }
      }
   };
}

#[macro_export]
macro_rules! store_ok {
   ($out:expr, $expr:expr) => {
      match $expr {
         Ok(v) => v,
         Err(e) => {
            $crate::utils::resp::write_err($out, e);
            return;
         }
      }
   };
}
