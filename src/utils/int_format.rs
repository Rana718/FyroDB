pub(crate) const DIGITS100: &[u8; 200] = b"0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

#[inline(always)]
fn write_u64_digits(digits: &mut [u8; 20], mut n: u64) -> usize {
   let mut d_pos = digits.len();

   while n >= 100 {
      let rem = ((n % 100) * 2) as usize;
      n /= 100;
      d_pos -= 2;
      digits[d_pos] = DIGITS100[rem];
      digits[d_pos + 1] = DIGITS100[rem + 1];
   }

   if n >= 10 {
      let rem = (n * 2) as usize;
      d_pos -= 2;
      digits[d_pos] = DIGITS100[rem];
      digits[d_pos + 1] = DIGITS100[rem + 1];
   } else {
      d_pos -= 1;
      digits[d_pos] = b'0' + n as u8;
   }

   d_pos
}

#[inline(always)]
pub fn write_u64_to(digits: &mut [u8; 20], n: u64) -> &str {
   let d_pos = write_u64_digits(digits, n);
   unsafe { std::str::from_utf8_unchecked(&digits[d_pos..]) }
}

#[inline(always)]
pub fn write_usize_to(digits: &mut [u8; 20], n: usize) -> &str {
   write_u64_to(digits, n as u64)
}

#[inline(always)]
pub fn write_i64_to(digits: &mut [u8; 20], n: i64) -> &str {
   let mut d_pos = write_u64_digits(digits, n.unsigned_abs());

   if n < 0 {
      d_pos -= 1;
      digits[d_pos] = b'-';
   }

   unsafe { std::str::from_utf8_unchecked(&digits[d_pos..]) }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_write_u64_to() {
      let cases = [
         0,
         1,
         9,
         10,
         11,
         99,
         100,
         101,
         999,
         1000,
         12345,
         999999,
         1000000,
         u64::MAX - 1,
         u64::MAX,
      ];
      let mut buf = [0u8; 20];
      for &c in &cases {
         assert_eq!(write_u64_to(&mut buf, c), c.to_string());
      }
   }

   #[test]
   fn test_write_i64_to() {
      let cases = [
         i64::MIN,
         i64::MIN + 1,
         -1000000,
         -99999,
         -100,
         -99,
         -10,
         -9,
         -1,
         0,
         1,
         9,
         10,
         99,
         100,
         99999,
         1000000,
         i64::MAX - 1,
         i64::MAX,
      ];
      let mut buf = [0u8; 20];
      for &c in &cases {
         assert_eq!(write_i64_to(&mut buf, c), c.to_string());
      }
   }
}
