use crate::storage::bitmap::BitOp;
use crate::storage::store::Store;
use crate::utils::resp;
use crate::{parse_int, store_ok, wt};

pub fn setbit(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, offset, value] = parts else {
        return resp::write_wrong_args(out, "setbit");
    };
    let off = parse_int!(out, offset, u64);
    let val = parse_int!(out, value, u8);
    if val > 1 {
        return resp::write_err(out, "bit is not an integer or out of range");
    }
    resp::write_integer(out, store_ok!(out, store.setbit(key, off, val == 1)) as i64);
}

pub fn getbit(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, offset] = parts else {
        return resp::write_wrong_args(out, "getbit");
    };
    let off = parse_int!(out, offset, u64);
    resp::write_integer(out, wt!(out, store.getbit(key, off)) as i64);
}

pub fn bitcount(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let (key, start, end, use_bit) = match parts {
        [_, key] => (*key, 0i64, -1i64, false),
        [_, key, start, end] => {
            let s = parse_int!(out, start);
            let e = parse_int!(out, end);
            (*key, s, e, false)
        }
        [_, key, start, end, mode] => {
            let s = parse_int!(out, start);
            let e = parse_int!(out, end);
            let use_bit = mode.eq_ignore_ascii_case("BIT");
            (*key, s, e, use_bit)
        }
        _ => return resp::write_wrong_args(out, "bitcount"),
    };
    resp::write_integer(
        out,
        wt!(out, store.bitcount(key, start, end, use_bit)) as i64,
    );
}

pub fn bitpos(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, bit_str, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "bitpos");
    };
    let bit = parse_int!(out, bit_str, u8);
    if bit > 1 {
        return resp::write_err(out, "bit is not an integer or out of range");
    }
    let (start, end, has_end, use_bit) = match rest {
        [] => (0i64, -1i64, false, false),
        [s] => (parse_int!(out, s), -1i64, false, false),
        [s, e] => (parse_int!(out, s), parse_int!(out, e), true, false),
        [s, e, mode] => {
            let use_bit = mode.eq_ignore_ascii_case("BIT");
            (parse_int!(out, s), parse_int!(out, e), true, use_bit)
        }
        _ => return resp::write_wrong_args(out, "bitpos"),
    };
    resp::write_integer(
        out,
        store_ok!(out, store.bitpos(key, bit, start, end, has_end, use_bit)),
    );
}

pub fn bitop(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, op_str, dest, keys @ ..] = parts else {
        return resp::write_wrong_args(out, "bitop");
    };
    if keys.is_empty() {
        return resp::write_wrong_args(out, "bitop");
    }
    let op = if op_str.eq_ignore_ascii_case("AND") {
        BitOp::And
    } else if op_str.eq_ignore_ascii_case("OR") {
        BitOp::Or
    } else if op_str.eq_ignore_ascii_case("XOR") {
        BitOp::Xor
    } else if op_str.eq_ignore_ascii_case("NOT") {
        BitOp::Not
    } else {
        return resp::write_err(out, "syntax error");
    };
    resp::write_integer(out, store_ok!(out, store.bitop(op, dest, keys)) as i64);
}

pub fn bitfield(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "bitfield");
    };
    if rest.is_empty() {
        return resp::write_array_header(out, 0);
    }

    let mut results: Vec<Option<i64>> = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut i = 0;

    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("OVERFLOW") {
            i += 1;
            if i >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            if rest[i].eq_ignore_ascii_case("WRAP") {
                overflow = Overflow::Wrap;
            } else if rest[i].eq_ignore_ascii_case("SAT") {
                overflow = Overflow::Sat;
            } else if rest[i].eq_ignore_ascii_case("FAIL") {
                overflow = Overflow::Fail;
            } else {
                return resp::write_err(out, "syntax error");
            }
            i += 1;
        } else if rest[i].eq_ignore_ascii_case("GET") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let (signed, bits) = parse_bitfield_type(rest[i + 1]);
            let offset = parse_bitfield_offset(rest[i + 2], bits);
            if bits == 0 || bits > 64 {
                return resp::write_err(out, "invalid bitfield type");
            }
            let val = bf_get(store, key, offset, bits, signed);
            results.push(Some(val));
            i += 3;
        } else if rest[i].eq_ignore_ascii_case("SET") {
            if i + 3 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let (signed, bits) = parse_bitfield_type(rest[i + 1]);
            let offset = parse_bitfield_offset(rest[i + 2], bits);
            let value = match rest[i + 3].parse::<i64>() {
                Ok(v) => v,
                Err(_) => return resp::write_err(out, "value is not an integer"),
            };
            if bits == 0 || bits > 64 {
                return resp::write_err(out, "invalid bitfield type");
            }
            let old = bf_get(store, key, offset, bits, signed);
            bf_set(store, key, offset, bits, value);
            results.push(Some(old));
            i += 4;
        } else if rest[i].eq_ignore_ascii_case("INCRBY") {
            if i + 3 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            let (signed, bits) = parse_bitfield_type(rest[i + 1]);
            let offset = parse_bitfield_offset(rest[i + 2], bits);
            let increment = match rest[i + 3].parse::<i64>() {
                Ok(v) => v,
                Err(_) => return resp::write_err(out, "value is not an integer"),
            };
            if bits == 0 || bits > 64 {
                return resp::write_err(out, "invalid bitfield type");
            }
            let old = bf_get(store, key, offset, bits, signed);
            let new_val = old.wrapping_add(increment);
            let clamped = if signed {
                clamp_signed(new_val, bits)
            } else {
                clamp_unsigned(new_val, bits)
            };
            match overflow {
                Overflow::Wrap => {
                    bf_set(store, key, offset, bits, clamped);
                    results.push(Some(clamped));
                }
                Overflow::Sat => {
                    let sat = if signed {
                        saturate_signed(old, increment, bits)
                    } else {
                        saturate_unsigned(old, increment, bits)
                    };
                    bf_set(store, key, offset, bits, sat);
                    results.push(Some(sat));
                }
                Overflow::Fail => {
                    let check = if signed {
                        let min = -(1i64 << (bits - 1));
                        let max = (1i64 << (bits - 1)) - 1;
                        new_val >= min && new_val <= max
                    } else {
                        let max = if bits >= 64 {
                            i64::MAX
                        } else {
                            (1i64 << bits) - 1
                        };
                        new_val >= 0 && new_val <= max
                    };
                    if check {
                        bf_set(store, key, offset, bits, new_val);
                        results.push(Some(new_val));
                    } else {
                        results.push(None);
                    }
                }
            }
            i += 4;
        } else {
            return resp::write_err(out, "syntax error");
        }
    }

    resp::write_array_header(out, results.len());
    for r in results {
        match r {
            Some(v) => resp::write_integer(out, v),
            None => resp::write_nil(out),
        }
    }
}

#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}

fn parse_bitfield_type(s: &str) -> (bool, u32) {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return (false, 0);
    }
    let signed = bytes[0] == b'i' || bytes[0] == b'I';
    let bits = s[1..].parse::<u32>().unwrap_or(0);
    (signed, bits)
}

fn parse_bitfield_offset(s: &str, bits: u32) -> u64 {
    if let Some(stripped) = s.strip_prefix('#') {
        stripped.parse::<u64>().unwrap_or(0) * bits as u64
    } else {
        s.parse::<u64>().unwrap_or(0)
    }
}

fn bf_get(store: &Store, key: &str, offset: u64, bits: u32, signed: bool) -> i64 {
    let mut val: u64 = 0;
    for i in 0..bits {
        let bit = store.getbit(key, offset + i as u64).unwrap_or(0);
        val |= (bit as u64) << (bits - 1 - i);
    }
    if signed && bits < 64 && (val >> (bits - 1)) & 1 == 1 {
        (val | (!0u64 << bits)) as i64
    } else {
        val as i64
    }
}

fn bf_set(store: &Store, key: &str, offset: u64, bits: u32, value: i64) {
    let val = value as u64;
    for i in 0..bits {
        let bit = (val >> (bits - 1 - i)) & 1 == 1;
        let _ = store.setbit(key, offset + i as u64, bit);
    }
}

fn clamp_signed(val: i64, bits: u32) -> i64 {
    if bits >= 64 {
        return val;
    }
    let mask = (1i64 << bits) - 1;
    let mut v = val & mask;
    if (v >> (bits - 1)) & 1 == 1 {
        v |= !mask;
    }
    v
}

fn clamp_unsigned(val: i64, bits: u32) -> i64 {
    if bits >= 64 {
        return val;
    }
    val & ((1i64 << bits) - 1)
}

fn saturate_signed(old: i64, incr: i64, bits: u32) -> i64 {
    let min = -(1i64 << (bits - 1));
    let max = (1i64 << (bits - 1)) - 1;
    let result = old.saturating_add(incr);
    result.max(min).min(max)
}

fn saturate_unsigned(old: i64, incr: i64, bits: u32) -> i64 {
    let max = if bits >= 64 {
        i64::MAX
    } else {
        (1i64 << bits) - 1
    };
    let result = old.saturating_add(incr);
    result.max(0).min(max)
}
