pub fn glob_match(pattern: &str, input: &str) -> bool {
   glob_match_bytes(pattern.as_bytes(), input.as_bytes())
}

pub fn glob_match_bytes(p: &[u8], s: &[u8]) -> bool {
   let (mut pi, mut si) = (0, 0);
   let (mut star, mut retry_si) = (None, 0);
   while si < s.len() {
      if pi < p.len() && (p[pi] == b'?' || p[pi] == s[si]) {
         pi += 1;
         si += 1;
      } else if pi < p.len() && p[pi] == b'*' {
         star = Some(pi);
         pi += 1;
         retry_si = si;
      } else if let Some(star_pi) = star {
         retry_si += 1;
         si = retry_si;
         pi = star_pi + 1;
      } else {
         return false;
      }
   }
   while pi < p.len() && p[pi] == b'*' {
      pi += 1;
   }
   pi == p.len()
}
