use crate::protocol::FrameReader;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use md5::Md5;
use sha2::{Digest, Sha256};
use std::io::{self, Read, Write};

pub const SCRAM_MECHANISM: &str = "SCRAM-SHA-256";

const SCRAM_ITERATIONS: u32 = 4096;
const SCRAM_SALT_LEN: usize = 16;
const SCRAM_NONCE_LEN: usize = 18;

#[derive(Debug)]
pub enum ClientAuthError {
   Io(io::Error),
   // The password or the SASL exchange was invalid. The client must receive a
   // 28P01 error, never a generic I/O failure.
   Rejected,
}

impl From<io::Error> for ClientAuthError {
   fn from(error: io::Error) -> Self {
      Self::Io(error)
   }
}

// Authenticates frontend with SCRAM-SHA-256 using stored password.
pub fn authenticate_client<R: Read, W: Write>(
   reader: &mut FrameReader<R>,
   writer: &mut W,
   password: &str,
) -> Result<(), ClientAuthError> {
   let mut offer = Vec::new();
   offer.extend_from_slice(&10i32.to_be_bytes());
   offer.extend_from_slice(SCRAM_MECHANISM.as_bytes());
   offer.push(0);
   offer.push(0);
   write_msg(writer, b'R', &offer)?;

   let initial = read_sasl_response(reader)?;
   let (mechanism, client_first) = split_sasl_initial(&initial)?;
   if mechanism != SCRAM_MECHANISM {
      return Err(ClientAuthError::Rejected);
   }
   let (bare, client_nonce) = split_client_first(client_first)?;
   if client_nonce.is_empty() {
      return Err(ClientAuthError::Rejected);
   }

   let mut salt = [0u8; SCRAM_SALT_LEN];
   getrandom::fill(&mut salt).map_err(random_error)?;
   let server_nonce = format!("{client_nonce}{}", random_b64(SCRAM_NONCE_LEN)?);
   let server_first = format!(
      "r={server_nonce},s={},i={SCRAM_ITERATIONS}",
      STANDARD.encode(salt)
   );
   let mut continue_msg = Vec::new();
   continue_msg.extend_from_slice(&11i32.to_be_bytes());
   continue_msg.extend_from_slice(server_first.as_bytes());
   write_msg(writer, b'R', &continue_msg)?;

   let client_final = read_sasl_response(reader)?;
   let final_text = std::str::from_utf8(&client_final).map_err(|_| ClientAuthError::Rejected)?;
   let (without_proof, proof) = final_text
      .rsplit_once(",p=")
      .ok_or(ClientAuthError::Rejected)?;
   if attr(without_proof, "c") != Some("biws")
      || attr(without_proof, "r") != Some(server_nonce.as_str())
   {
      return Err(ClientAuthError::Rejected);
   }

   let auth_message = format!("{bare},{server_first},{without_proof}");
   let salted = pbkdf2_sha256(password.as_bytes(), &salt, SCRAM_ITERATIONS);
   let client_key = hmac_sha256(&salted, &[b"Client Key"]);
   let stored_key = sha256(&client_key);
   let client_signature = hmac_sha256(&stored_key, &[auth_message.as_bytes()]);
   let expected = xor32(&client_key, &client_signature);
   let proof = STANDARD
      .decode(proof)
      .map_err(|_| ClientAuthError::Rejected)?;
   if !constant_time_eq(&proof, &expected) {
      return Err(ClientAuthError::Rejected);
   }

   let server_key = hmac_sha256(&salted, &[b"Server Key"]);
   let server_signature = hmac_sha256(&server_key, &[auth_message.as_bytes()]);
   let mut final_msg = Vec::new();
   final_msg.extend_from_slice(&12i32.to_be_bytes());
   final_msg.extend_from_slice(format!("v={}", STANDARD.encode(server_signature)).as_bytes());
   write_msg(writer, b'R', &final_msg)?;
   write_msg(writer, b'R', &0i32.to_be_bytes())?;
   writer.flush()?;
   Ok(())
}

// Client half of SCRAM-SHA-256, used when the proxy authenticates to
// PostgreSQL with credentials from its own auth map.
pub struct ScramClient {
   client_nonce: String,
   client_first_bare: String,
   expected_server_signature: Option<[u8; 32]>,
}

impl ScramClient {
   pub fn new() -> io::Result<Self> {
      let client_nonce = random_b64(SCRAM_NONCE_LEN)?;
      Ok(Self {
         client_first_bare: format!("n=,r={client_nonce}"),
         client_nonce,
         expected_server_signature: None,
      })
   }

   pub fn mechanism_supported(mechanisms: &[u8]) -> bool {
      mechanisms
         .split(|byte| *byte == 0)
         .any(|name| name == SCRAM_MECHANISM.as_bytes())
   }

   pub fn initial_response(&self) -> String {
      format!("n,,{}", self.client_first_bare)
   }

   // Consumes the server-first message and returns the client-final message.
   pub fn finish(&mut self, server_first: &str, password: &str) -> io::Result<String> {
      let server_nonce =
         attr(server_first, "r").ok_or_else(|| invalid("SCRAM server-first is missing a nonce"))?;
      if !server_nonce.starts_with(&self.client_nonce) {
         return Err(invalid(
            "SCRAM server nonce does not extend the client nonce",
         ));
      }
      let salt = STANDARD
         .decode(
            attr(server_first, "s")
               .ok_or_else(|| invalid("SCRAM server-first is missing a salt"))?,
         )
         .map_err(|_| invalid("SCRAM salt is not valid base64"))?;
      let iterations = attr(server_first, "i")
         .and_then(|value| value.parse::<u32>().ok())
         .filter(|value| *value > 0)
         .ok_or_else(|| invalid("SCRAM iteration count is invalid"))?;

      let without_proof = format!("c=biws,r={server_nonce}");
      let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
      let salted = pbkdf2_sha256(password.as_bytes(), &salt, iterations);
      let client_key = hmac_sha256(&salted, &[b"Client Key"]);
      let stored_key = sha256(&client_key);
      let client_signature = hmac_sha256(&stored_key, &[auth_message.as_bytes()]);
      let proof = xor32(&client_key, &client_signature);
      let server_key = hmac_sha256(&salted, &[b"Server Key"]);
      self.expected_server_signature = Some(hmac_sha256(&server_key, &[auth_message.as_bytes()]));
      Ok(format!("{without_proof},p={}", STANDARD.encode(proof)))
   }

   pub fn verify_server_final(&self, server_final: &str) -> bool {
      let Some(expected) = self.expected_server_signature else {
         return false;
      };
      attr(server_final, "v")
         .and_then(|value| STANDARD.decode(value).ok())
         .is_some_and(|value| constant_time_eq(&value, &expected))
   }
}

// MD5 password response: "md5" + md5(md5(password + user) + salt).
pub fn md5_response(password: &str, user: &str, salt: &[u8]) -> String {
   let inner = md5_hex(format!("{password}{user}").as_bytes());
   let mut outer_input = inner.into_bytes();
   outer_input.extend_from_slice(salt);
   format!("md5{}", md5_hex(&outer_input))
}

fn read_sasl_response<R: Read>(reader: &mut FrameReader<R>) -> Result<Vec<u8>, ClientAuthError> {
   let tag = reader.read_raw()?;
   if tag != b'p' {
      return Err(ClientAuthError::Rejected);
   }
   Ok(reader.last_body().to_vec())
}

fn split_sasl_initial(body: &[u8]) -> Result<(&str, &str), ClientAuthError> {
   let end = body
      .iter()
      .position(|byte| *byte == 0)
      .ok_or(ClientAuthError::Rejected)?;
   let mechanism = std::str::from_utf8(&body[..end]).map_err(|_| ClientAuthError::Rejected)?;
   if body.len() < end + 5 {
      return Err(ClientAuthError::Rejected);
   }
   let len = i32::from_be_bytes([body[end + 1], body[end + 2], body[end + 3], body[end + 4]]);
   if len < 0 || body.len() < end + 5 + len as usize {
      return Err(ClientAuthError::Rejected);
   }
   let data = std::str::from_utf8(&body[end + 5..end + 5 + len as usize])
      .map_err(|_| ClientAuthError::Rejected)?;
   Ok((mechanism, data))
}

// Returns the client-first-bare text and its nonce.
fn split_client_first(message: &str) -> Result<(&str, &str), ClientAuthError> {
   let (gs2, rest) = message.split_once(',').ok_or(ClientAuthError::Rejected)?;
   let (_, bare) = rest.split_once(',').ok_or(ClientAuthError::Rejected)?;
   if gs2 != "n" && gs2 != "y" {
      return Err(ClientAuthError::Rejected);
   }
   let nonce = attr(bare, "r").ok_or(ClientAuthError::Rejected)?;
   Ok((bare, nonce))
}

fn attr<'a>(message: &'a str, name: &str) -> Option<&'a str> {
   message.split(',').find_map(|part| {
      let (key, value) = part.split_once('=')?;
      (key == name).then_some(value)
   })
}

fn random_b64(len: usize) -> io::Result<String> {
   let mut bytes = vec![0u8; len];
   getrandom::fill(&mut bytes).map_err(random_error)?;
   Ok(STANDARD.encode(bytes))
}

fn random_error(error: getrandom::Error) -> io::Error {
   io::Error::other(error.to_string())
}

fn invalid(message: &str) -> io::Error {
   io::Error::new(io::ErrorKind::InvalidData, message)
}

fn write_msg<W: Write>(writer: &mut W, tag: u8, body: &[u8]) -> io::Result<()> {
   writer.write_all(&[tag])?;
   writer.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
   writer.write_all(body)?;
   writer.flush()
}

fn sha256(data: &[u8]) -> [u8; 32] {
   let digest = Sha256::digest(data);
   let mut out = [0u8; 32];
   out.copy_from_slice(&digest);
   out
}

fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
   const BLOCK: usize = 64;
   let mut normalized = [0u8; BLOCK];
   if key.len() > BLOCK {
      normalized[..32].copy_from_slice(&sha256(key));
   } else {
      normalized[..key.len()].copy_from_slice(key);
   }

   let mut inner_pad = [0x36u8; BLOCK];
   let mut outer_pad = [0x5cu8; BLOCK];
   for index in 0..BLOCK {
      inner_pad[index] ^= normalized[index];
      outer_pad[index] ^= normalized[index];
   }

   let mut inner = Sha256::new();
   inner.update(inner_pad);
   for part in parts {
      inner.update(part);
   }
   let inner = inner.finalize();

   let mut outer = Sha256::new();
   outer.update(outer_pad);
   outer.update(inner);
   let digest = outer.finalize();
   let mut out = [0u8; 32];
   out.copy_from_slice(&digest);
   out
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
   // SHA-256 output equals the derived key length, so exactly one block runs.
   let mut first = Vec::with_capacity(salt.len() + 4);
   first.extend_from_slice(salt);
   first.extend_from_slice(&1u32.to_be_bytes());
   let mut previous = hmac_sha256(password, &[&first]);
   let mut derived = previous;
   for _ in 1..iterations {
      previous = hmac_sha256(password, &[&previous]);
      for index in 0..32 {
         derived[index] ^= previous[index];
      }
   }
   derived
}

fn xor32(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
   let mut out = [0u8; 32];
   for index in 0..32 {
      out[index] = left[index] ^ right[index];
   }
   out
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
   if left.len() != right.len() {
      return false;
   }
   let mut diff = 0u8;
   for (a, b) in left.iter().zip(right) {
      diff |= a ^ b;
   }
   diff == 0
}

fn md5_hex(data: &[u8]) -> String {
   let digest = Md5::digest(data);
   let mut out = String::with_capacity(32);
   for byte in digest {
      out.push_str(&format!("{byte:02x}"));
   }
   out
}

#[cfg(test)]
mod tests {
   use super::*;

   // RFC 7677 test vector, which pins the whole SCRAM pipeline.
   #[test]
   fn scram_client_matches_rfc_vector() {
      let mut client = ScramClient {
         client_nonce: "rOprNGfwEbeRWgbNEkqO".to_string(),
         client_first_bare: "n=user,r=rOprNGfwEbeRWgbNEkqO".to_string(),
         expected_server_signature: None,
      };
      let server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
         s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
      let final_msg = client.finish(server_first, "pencil").expect("scram final");
      assert_eq!(
         final_msg,
         "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
          p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
      );
      assert!(client.verify_server_final("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="));
   }

   #[test]
   fn md5_response_matches_postgres_formula() {
      let salt = [1u8, 2, 3, 4];
      let inner = md5_hex(b"secretpostgres");
      let mut outer_input = inner.into_bytes();
      outer_input.extend_from_slice(&salt);
      assert_eq!(
         md5_response("secret", "postgres", &salt),
         format!("md5{}", md5_hex(&outer_input))
      );
   }
}
