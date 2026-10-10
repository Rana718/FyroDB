use crate::storage::store::Store;
use crate::store_ok;
use crate::utils::resp;

pub fn pfadd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key, elements @ ..] => {
            let changed = store_ok!(out, store.pfadd(key, elements));
            resp::write_boolean(out, changed);
        }
        _ => resp::write_wrong_args(out, "pfadd"),
    }
}

pub fn pfcount(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, keys @ ..] if !keys.is_empty() => {
            resp::write_integer(out, store_ok!(out, store.pfcount(keys)) as i64);
        }
        _ => resp::write_wrong_args(out, "pfcount"),
    }
}

pub fn pfmerge(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, dest, sources @ ..] => {
            store_ok!(out, store.pfmerge(dest, sources));
            resp::write_ok(out);
        }
        _ => resp::write_wrong_args(out, "pfmerge"),
    }
}
