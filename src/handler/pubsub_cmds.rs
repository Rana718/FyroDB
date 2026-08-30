use crate::pubsub::PubSub;
use crate::utils::resp;

pub fn pubsub_info(parts: &[&str], pubsub: &PubSub, out: &mut Vec<u8>) {
    let sub = match parts.get(1) {
        Some(s) => *s,
        None => {
            resp::write_err(out, "wrong number of arguments for 'pubsub' command");
            return;
        }
    };

    match sub.to_ascii_uppercase().as_str() {
        "CHANNELS" => {
            let pattern = parts.get(2).copied();
            let mut channels = pubsub.active_channels(pattern);
            channels.sort();
            resp::write_array(out, &channels);
        }
        "NUMSUB" => {
            let keys: Vec<&str> = parts[2..].to_vec();
            let pairs = pubsub.numsub(&keys);
            resp::write_array_header(out, pairs.len() * 2);
            for (ch, n) in pairs {
                resp::write_bulk(out, &ch);
                resp::write_integer(out, n as i64);
            }
        }
        "NUMPAT" => {
            resp::write_integer(out, pubsub.numpat() as i64);
        }
        _ => resp::write_err(out, "unknown pubsub subcommand"),
    }
}
