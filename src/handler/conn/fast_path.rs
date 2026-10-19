use super::*;

#[inline(always)]
pub(super) fn dispatch_raw(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   if raw.is_empty() {
      return;
   }

   let cmd_len = raw[0].1;
   let cmd: &[u8] = unsafe { part_bytes(raw[0]) };

   if !conn.authenticated {
      handle_unauth(conn, raw, cmd, cmd_len);
      return;
   }

   if !conn.store.cluster.enabled && !conn.store.at_key_capacity() {
      if cmd_len == 3 {
         if cmd.eq_ignore_ascii_case(b"GET") && raw.len() == 2 {
            let hint = conn.hint_for(raw[1]);
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            if !conn.store.get_to_buf_hinted_pinned(key, hint, out) {
               out.extend_from_slice(b"$-1\r\n");
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"SET") && raw.len() == 3 {
            let hint = conn.hint_for(raw[1]);
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(value) = part_str(out, raw[2]) else {
               return;
            };
            conn.store.set_string_hinted_pinned(key, hint, value, 0);
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            out.extend_from_slice(b"+OK\r\n");
            return;
         } else if cmd.eq_ignore_ascii_case(b"DEL") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            if conn.store.del(key) {
               out.extend_from_slice(b":1\r\n");
            } else {
               out.extend_from_slice(b":0\r\n");
            }
            return;
         }
      } else if cmd_len == 4 {
         if cmd.eq_ignore_ascii_case(b"HGET") && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(field) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.hget_to_buf(key, field, out) {
               Ok(true) => {}
               Ok(false) => crate::utils::resp::write_nil(out),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"HSET") && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let pair = [(unsafe { part_bytes(raw[2]) }, unsafe { part_bytes(raw[3]) })];
            crate::commands::hash::hset_raw(key, &pair, conn.store, out);
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"PING") {
            if raw.len() == 1 {
               conn.parser.wbuf.extend_from_slice(crate::utils::resp::PONG);
               return;
            } else if raw.len() == 2 {
               let msg = unsafe { part_bytes(raw[1]) };
               crate::utils::resp::write_bulk_bytes(&mut conn.parser.wbuf, msg);
               return;
            }
         } else if cmd.eq_ignore_ascii_case(b"INCR") && raw.len() == 2 {
            let hint = conn.hint_for(raw[1]);
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.incr_hinted(key, hint) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n)
               }
               Err(e) => crate::utils::resp::write_err(out, e),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"RPOP") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.pop_one_to_buf(key, true, out) {
               Ok(true) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
               }
               Ok(false) => out.extend_from_slice(b"$-1\r\n"),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"LPOP") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.pop_one_to_buf(key, false, out) {
               Ok(true) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
               }
               Ok(false) => out.extend_from_slice(b"$-1\r\n"),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"SADD") && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(member) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.sadd(key, &[member]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"ZADD") && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(score_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(member) = part_str(out, raw[3]) else {
               return;
            };
            if let Ok(score) = score_str.parse::<f64>() {
               match conn.store.zadd(key, &[(score, member)], Default::default()) {
                  Ok(n) => {
                     if conn.store.has_replication() {
                        conn.store.record_current_value(key);
                     }
                     crate::utils::resp::write_integer(out, n as i64)
                  }
                  Err(_) => crate::utils::resp::write_wrong_type(out),
               }
            } else {
               out.extend_from_slice(b"-ERR value is not a valid float\r\n");
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"XADD") && raw.len() == 5 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(id_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(f) = part_str(out, raw[3]) else {
               return;
            };
            let Some(v) = part_str(out, raw[4]) else {
               return;
            };
            match conn.store.xadd(key, id_str, &[(f, v)], None, false) {
               Ok(Some(id)) => crate::utils::resp::write_bulk(out, &id),
               Ok(None) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            return;
         }
      } else if cmd_len == 5 {
         if cmd.eq_ignore_ascii_case(b"LPUSH") && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(value) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.lpush(key, &[value]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"RPUSH") && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(value) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.rpush(key, &[value]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(out, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"PFADD") && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(elem) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.pfadd(key, &[elem]) {
               Ok(changed) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_boolean(out, changed);
               }
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
      } else if cmd_len == 6 {
         if cmd.eq_ignore_ascii_case(b"EXPIRE") && raw.len() == 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(secs) = part_str(out, raw[2]) else {
               return;
            };
            let reply = match secs
               .parse::<u64>()
               .ok()
               .and_then(crate::storage::value::expiry_from_secs)
            {
               Some(exp) => {
                  let ok = conn.store.expire_ms(key, exp);
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  if ok {
                     crate::utils::resp::ONE
                  } else {
                     crate::utils::resp::ZERO
                  }
               }
               None => b"-ERR invalid expire time in 'expire' command\r\n" as &[u8],
            };
            out.extend_from_slice(reply);
            return;
         } else if cmd.eq_ignore_ascii_case(b"SETBIT") && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(offset_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(value_str) = part_str(out, raw[3]) else {
               return;
            };
            if let (Ok(off), Ok(val)) = (offset_str.parse::<u64>(), value_str.parse::<u8>()) {
               if val <= 1 {
                  match conn.store.setbit(key, off, val == 1) {
                     Ok(prev) => {
                        if conn.store.has_replication() {
                           conn.store.record_current_value(key);
                        }
                        crate::utils::resp::write_integer(out, prev as i64);
                     }
                     Err(e) => crate::utils::resp::write_store_err(out, e),
                  }
               } else {
                  out.extend_from_slice(b"-ERR bit is not an integer or out of range\r\n");
               }
            } else {
               out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"GEOADD") && raw.len() == 5 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(lon_str) = part_str(out, raw[2]) else {
               return;
            };
            let Some(lat_str) = part_str(out, raw[3]) else {
               return;
            };
            let Some(member) = part_str(out, raw[4]) else {
               return;
            };
            if let (Ok(lon), Ok(lat)) = (lon_str.parse::<f64>(), lat_str.parse::<f64>()) {
               if (-180.0..=180.0).contains(&lon) && (-85.05112878..=85.05112878).contains(&lat) {
                  match conn
                     .store
                     .geoadd(key, &[(lon, lat, member)], false, false, false)
                  {
                     Ok(n) => {
                        if conn.store.has_replication() {
                           conn.store.record_current_value(key);
                        }
                        crate::utils::resp::write_integer(out, n as i64);
                     }
                     Err(e) => crate::utils::resp::write_store_err(out, e),
                  }
               } else {
                  out.extend_from_slice(b"-ERR invalid longitude,latitude pair\r\n");
               }
            } else {
               out.extend_from_slice(b"-ERR value is not a valid float\r\n");
            }
            return;
         }
      } else if cmd_len == 7 {
         if cmd.eq_ignore_ascii_case(b"PFCOUNT") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.pfcount(&[key]) {
               Ok(n) => crate::utils::resp::write_integer(out, n as i64),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"GEODIST") && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(m1) = part_str(out, raw[2]) else {
               return;
            };
            let Some(m2) = part_str(out, raw[3]) else {
               return;
            };
            match conn
               .store
               .geodist(key, m1, m2, crate::storage::geo::GeoUnit::M)
            {
               Ok(Some(d)) => {
                  crate::utils::resp::write_bulk(out, &crate::utils::util::format_float(d))
               }
               Ok(None) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
      } else if cmd_len == 8 {
         if cmd.eq_ignore_ascii_case(b"BITCOUNT") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            match conn.store.bitcount(key, 0, -1, false) {
               Ok(c) => crate::utils::resp::write_integer(out, c as i64),
               Err(_) => crate::utils::resp::write_wrong_type(out),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"JSON.SET") && raw.len() == 4 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let Some(path) = part_str(out, raw[2]) else {
               return;
            };
            let Some(value) = part_str(out, raw[3]) else {
               return;
            };
            match conn.store.json_set(key, path, value, false, false) {
               Ok(true) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_ok(out);
               }
               Ok(false) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         } else if cmd.eq_ignore_ascii_case(b"JSON.GET") {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
               return;
            };
            let res = if raw.len() == 2 {
               conn.store.json_get_to_buf(key, &[], out)
            } else if raw.len() == 3 {
               let Some(path) = part_str(out, raw[2]) else {
                  return;
               };
               conn.store.json_get_to_buf(key, &[path], out)
            } else {
               crate::utils::resp::write_wrong_args(out, "json.get");
               return;
            };
            match res {
               Ok(true) => {}
               Ok(false) => crate::utils::resp::write_nil(out),
               Err(e) => crate::utils::resp::write_store_err(out, e),
            }
            return;
         }
      }
   }

   // Only pay for the write gate when cluster is actually enabled and the
   // command is a write.
   let cluster_is_write = conn.store.cluster.enabled && crate::cluster::is_write_command(cmd);

   if conn.store.at_key_capacity() && crate::storage::capacity::is_denyoom_command(cmd) {
      conn
         .parser
         .wbuf
         .extend_from_slice(crate::storage::capacity::OOM_REPLY);
      return;
   }

   let store = conn.store;
   let _cluster_write_guard = if cluster_is_write {
      Some(store.begin_write(conn.worker))
   } else {
      None
   };

   if conn.store.cluster.enabled {
      if conn.store.replica_installing() {
         conn
            .parser
            .wbuf
            .extend_from_slice(b"-CLUSTERDOWN Replica snapshot is installing\r\n");
         return;
      }
      if conn.store.cluster.is_replica && cluster_is_write {
         conn
            .parser
            .wbuf
            .extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
         return;
      }

      conn.refresh_topology();
      let asking = std::mem::take(&mut conn.asking);

      // The argument slice is materialized only for multi-key commands;
      // keyless and single-key commands skip it.
      let decision = match crate::cluster::routing_scope(cmd) {
         crate::cluster::RoutingScope::Keyless => crate::cluster::RouteDecision::Local,
         crate::cluster::RoutingScope::FirstKey => match raw.get(1) {
            Some(&part) => {
               let (topology, routing) = conn.cached_topology();
               crate::cluster::route_single_key(
                  &conn.store.cluster,
                  conn.store.cluster_state_ref(),
                  topology,
                  routing,
                  unsafe { part_bytes(part) },
                  asking,
               )
            }
            None => crate::cluster::RouteDecision::Local,
         },
         crate::cluster::RoutingScope::ManyKeys => {
            const STACK_ARGS: usize = 32;
            let mut stack_args = [&[][..]; STACK_ARGS];
            if raw.len().saturating_sub(1) > STACK_ARGS {
               let args: Vec<&[u8]> = raw[1..]
                  .iter()
                  .map(|&part| unsafe { part_bytes(part) })
                  .collect();
               route_cached(conn, cmd, &args, asking)
            } else {
               for (index, part) in raw[1..].iter().enumerate() {
                  stack_args[index] = unsafe { part_bytes(*part) };
               }
               let args = &stack_args[..raw.len() - 1];
               route_cached(conn, cmd, args, asking)
            }
         }
      };
      match decision {
         crate::cluster::RouteDecision::Local => {}
         other => {
            write_route_decision(&mut conn.parser.wbuf, other);
            return;
         }
      }
   }

   if park::is_blocking_cmd(cmd) {
      park::dispatch_blocking(conn, raw);
      return;
   }
   if park::is_blocking_stream_read(cmd, raw) {
      park::dispatch_stream_read(conn, raw);
      return;
   }

   if cmd_len == 3 {
      if cmd.eq_ignore_ascii_case(b"SET") && raw.len() >= 3 {
         let hint = conn.hint_for(raw[1]);
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         let Some(value) = part_str(out, raw[2]) else {
            return;
         };
         if raw.len() == 3 {
            conn.store.set_string_hinted_pinned(key, hint, value, 0);
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
            return;
         }
      } else if cmd.eq_ignore_ascii_case(b"GET") && raw.len() == 2 {
         let hint = conn.hint_for(raw[1]);
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if !conn.store.get_to_buf_hinted_pinned(key, hint, out) {
            out.extend_from_slice(b"$-1\r\n");
         }
         return;
      } else if cmd.eq_ignore_ascii_case(b"DEL") && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if conn.store.del(key) {
            conn.parser.wbuf.extend_from_slice(b":1\r\n");
         } else {
            conn.parser.wbuf.extend_from_slice(b":0\r\n");
         }
         return;
      }
   } else if cmd_len == 4 {
      if cmd.eq_ignore_ascii_case(b"HGET") && raw.len() == 3 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         let Some(field) = part_str(out, raw[2]) else {
            return;
         };
         match conn.store.hget_to_buf(key, field, out) {
            Ok(true) => {}
            Ok(false) => crate::utils::resp::write_nil(out),
            Err(_) => crate::utils::resp::write_wrong_type(out),
         }
         return;
      } else if cmd.eq_ignore_ascii_case(b"HSET")
         && raw.len() >= 4
         && (raw.len() - 2).is_multiple_of(2)
      {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if raw.len() == 4 {
            let pair = [(unsafe { part_bytes(raw[2]) }, unsafe { part_bytes(raw[3]) })];
            crate::commands::hash::hset_raw(key, &pair, conn.store, &mut conn.parser.wbuf);
         } else {
            let n_pairs = (raw.len() - 2) / 2;
            const STACK_PAIRS: usize = 16;
            if n_pairs <= STACK_PAIRS {
               let mut stack_pairs = [(&[][..], &[][..]); STACK_PAIRS];
               for i in 0..n_pairs {
                  stack_pairs[i] = (unsafe { part_bytes(raw[2 + i * 2]) }, unsafe {
                     part_bytes(raw[3 + i * 2])
                  });
               }
               crate::commands::hash::hset_raw(
                  key,
                  &stack_pairs[..n_pairs],
                  conn.store,
                  &mut conn.parser.wbuf,
               );
            } else {
               let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity(n_pairs);
               for i in 0..n_pairs {
                  pairs.push((unsafe { part_bytes(raw[2 + i * 2]) }, unsafe {
                     part_bytes(raw[3 + i * 2])
                  }));
               }
               crate::commands::hash::hset_raw(key, &pairs, conn.store, &mut conn.parser.wbuf);
            }
         }
         if conn.store.has_replication() {
            conn.store.record_current_value(key);
         }
         return;
      } else if cmd.eq_ignore_ascii_case(b"PING") {
         if raw.len() == 1 {
            conn.parser.wbuf.extend_from_slice(crate::utils::resp::PONG);
            return;
         } else if raw.len() == 2 {
            let msg = unsafe { part_bytes(raw[1]) };
            crate::utils::resp::write_bulk_bytes(&mut conn.parser.wbuf, msg);
            return;
         }
      } else if cmd.eq_ignore_ascii_case(b"INCR") && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         match conn.store.incr(key) {
            Ok(n) => {
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
               crate::utils::resp::write_integer(&mut conn.parser.wbuf, n)
            }
            Err(e) => crate::utils::resp::write_err(&mut conn.parser.wbuf, e),
         }
         return;
      } else if cmd.eq_ignore_ascii_case(b"RPOP") && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         match conn.store.pop_one_to_buf(key, true, &mut conn.parser.wbuf) {
            Ok(true) => {
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
            }
            Ok(false) => conn.parser.wbuf.extend_from_slice(b"$-1\r\n"),
            Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
         }
         return;
      } else if cmd.eq_ignore_ascii_case(b"LPOP") && raw.len() == 2 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         match conn.store.pop_one_to_buf(key, false, &mut conn.parser.wbuf) {
            Ok(true) => {
               if conn.store.has_replication() {
                  conn.store.record_current_value(key);
               }
            }
            Ok(false) => conn.parser.wbuf.extend_from_slice(b"$-1\r\n"),
            Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
         }
         return;
      } else if cmd.eq_ignore_ascii_case(b"SADD") && raw.len() >= 3 {
         let out = &mut conn.parser.wbuf;
         let Some(key) = part_str(out, raw[1]) else {
            return;
         };
         if raw.len() == 3 {
            let Some(member) = part_str(out, raw[2]) else {
               return;
            };
            match conn.store.sadd(key, &[member]) {
               Ok(n) => {
                  if conn.store.has_replication() {
                     conn.store.record_current_value(key);
                  }
                  crate::utils::resp::write_integer(&mut conn.parser.wbuf, n as i64)
               }
               Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
            }
            return;
         }
      }
   } else if cmd_len == 6 && cmd.eq_ignore_ascii_case(b"EXPIRE") && raw.len() == 3 {
      let out = &mut conn.parser.wbuf;
      let Some(key) = part_str(out, raw[1]) else {
         return;
      };
      let Some(secs) = part_str(out, raw[2]) else {
         return;
      };
      // Bad input and out-of-range overflow produce the same error the
      // generic path reports, so the fast arm stays protocol-identical.
      let reply = match secs
         .parse::<u64>()
         .ok()
         .and_then(crate::storage::value::expiry_from_secs)
      {
         Some(exp) => {
            let ok = conn.store.expire_ms(key, exp);
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            if ok {
               crate::utils::resp::ONE
            } else {
               crate::utils::resp::ZERO
            }
         }
         None => b"-ERR invalid expire time in 'expire' command\r\n" as &[u8],
      };
      conn.parser.wbuf.extend_from_slice(reply);
      return;
   } else if cmd_len == 5 && cmd.eq_ignore_ascii_case(b"LPUSH") && raw.len() == 3 {
      let out = &mut conn.parser.wbuf;
      let Some(key) = part_str(out, raw[1]) else {
         return;
      };
      let Some(value) = part_str(out, raw[2]) else {
         return;
      };
      match conn.store.lpush(key, &[value]) {
         Ok(n) => {
            if conn.store.has_replication() {
               conn.store.record_current_value(key);
            }
            crate::utils::resp::write_integer(&mut conn.parser.wbuf, n as i64)
         }
         Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
      }
      return;
   }
   if cmd_len == 9
      && (cmd.eq_ignore_ascii_case(b"FT.SEARCH") || cmd.eq_ignore_ascii_case(b"FT.HYBRID"))
      && raw.len() >= 3
   {
      return ft_search_raw(conn, raw);
   }
   if is_offloaded_ft(cmd) {
      return ft_offload_generic(conn, raw);
   }

   const STACK_CAP: usize = 32;
   if raw.len() <= STACK_CAP {
      let mut arr = [""; STACK_CAP];
      for (i, &part) in raw.iter().enumerate() {
         let Some(s) = part_str(&mut conn.parser.wbuf, part) else {
            return;
         };
         arr[i] = s;
      }
      dispatch(conn, &arr[..raw.len()]);
   } else {
      let mut parts: Vec<&str> = Vec::with_capacity(raw.len());
      for &part in raw.iter() {
         let Some(s) = part_str(&mut conn.parser.wbuf, part) else {
            return;
         };
         parts.push(s);
      }
      dispatch(conn, &parts);
   }
}

/// Route one command against this connection's cached topology snapshot.
#[inline(always)]
pub(super) fn route_cached(
   conn: &Conn<'_>,
   cmd: &[u8],
   args: &[&[u8]],
   asking: bool,
) -> crate::cluster::RouteDecision<'static> {
   let (topology, routing) = conn.cached_topology();
   crate::cluster::route_command_with_snapshot(
      &conn.store.cluster,
      conn.store.cluster_state_ref(),
      topology,
      routing,
      cmd,
      args,
      asking,
   )
}

/// Write a non-Local routing decision into the output buffer.
#[cold]
#[inline(never)]
pub(super) fn write_route_decision(out: &mut Vec<u8>, decision: crate::cluster::RouteDecision<'_>) {
   match decision {
      crate::cluster::RouteDecision::Moved { slot, address } => {
         out.extend_from_slice(b"-MOVED ");
         crate::utils::resp::write_usize(out, slot.value() as usize);
         out.push(b' ');
         out.extend_from_slice(address.as_bytes());
         out.extend_from_slice(b"\r\n");
      }
      crate::cluster::RouteDecision::MovedOwned { slot, address } => {
         out.extend_from_slice(b"-MOVED ");
         crate::utils::resp::write_usize(out, slot.value() as usize);
         out.push(b' ');
         out.extend_from_slice(address.as_bytes());
         out.extend_from_slice(b"\r\n");
      }
      crate::cluster::RouteDecision::Ask { slot, address } => {
         out.extend_from_slice(b"-ASK ");
         crate::utils::resp::write_usize(out, slot.value() as usize);
         out.push(b' ');
         out.extend_from_slice(address.as_bytes());
         out.extend_from_slice(b"\r\n");
      }
      crate::cluster::RouteDecision::CrossSlot => {
         out.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
      }
      crate::cluster::RouteDecision::Unassigned(_) => {
         out.extend_from_slice(b"-CLUSTERDOWN Hash slot not served\r\n");
      }
      crate::cluster::RouteDecision::Local => {}
   }
}

#[cold]
#[inline(never)]
pub(super) fn handle_unauth(
   conn: &mut Conn<'_>,
   raw: &[(*const u8, usize)],
   cmd: &[u8],
   cmd_len: usize,
) {
   if cmd_len == 4 && cmd.eq_ignore_ascii_case(b"AUTH") {
      if raw.len() >= 2 {
         let Some(pass) = part_str(&mut conn.parser.wbuf, raw[1]) else {
            return;
         };
         if let Some(expected) = conn.auth_required {
            if constant_time_eq(pass.as_bytes(), expected.as_bytes()) {
               conn.authenticated = true;
               conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
            } else {
               conn
                  .parser
                  .wbuf
                  .extend_from_slice(b"-WRONGPASS invalid username-password pair\r\n");
            }
         } else {
            conn.authenticated = true;
            conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
         }
      } else {
         crate::utils::resp::write_wrong_args(&mut conn.parser.wbuf, "auth");
      }
   } else if cmd_len == 4 && cmd.eq_ignore_ascii_case(b"PING") {
      conn.parser.wbuf.extend_from_slice(b"+PONG\r\n");
   } else if cmd_len == 4 && cmd.eq_ignore_ascii_case(b"QUIT") {
      conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
   } else {
      conn
         .parser
         .wbuf
         .extend_from_slice(b"-NOAUTH Authentication required.\r\n");
   }
}

#[inline(never)]
pub(super) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
   if a.len() != b.len() {
      return false;
   }
   let mut diff = 0u8;
   for (x, y) in a.iter().zip(b.iter()) {
      diff |= x ^ y;
   }
   diff == 0
}

/// Offloads search commands to the vector pool to avoid stalling workers;
/// `FYRODB_SEARCH_INLINE=1` overrides to keep them inline.
fn search_offloaded() -> bool {
   static INLINE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
   !*INLINE.get_or_init(|| std::env::var("FYRODB_SEARCH_INLINE").is_ok_and(|v| v == "1"))
}

pub(super) fn is_offloaded_ft(cmd: &[u8]) -> bool {
   cmd.len() > 3
      && cmd[..3].eq_ignore_ascii_case(b"FT.")
      && (cmd.eq_ignore_ascii_case(b"FT.AGGREGATE")
         || cmd.eq_ignore_ascii_case(b"FT.CURSOR")
         || cmd.eq_ignore_ascii_case(b"FT.PROFILE"))
}

/// FT.AGGREGATE / FT.CURSOR / FT.PROFILE: owned copies of the arguments go
/// to the pool with the store handle.
pub(super) fn ft_offload_generic(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   let mut parts: Vec<String> = Vec::with_capacity(raw.len());
   for &p in raw {
      match std::str::from_utf8(unsafe { part_bytes(p) }) {
         Ok(s) => parts.push(s.to_string()),
         Err(_) => {
            conn
               .parser
               .wbuf
               .extend_from_slice(b"-ERR invalid UTF-8 in request\r\n");
            return;
         }
      }
   }
   let store = Arc::clone(conn.store);
   let run = move |out: &mut Vec<u8>| {
      let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
      crate::commands::ft::execute_ft(&refs, &store, out);
   };
   if search_offloaded() {
      conn.submit(run);
   } else {
      run(&mut conn.parser.wbuf);
   }
}

pub(super) fn ft_search_raw(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
   let inline = !search_offloaded()
      || (raw.len() >= 2 && {
         let idx = unsafe { std::str::from_utf8_unchecked(part_bytes(raw[1])) };
         conn.store.search_state().hnsw_graph_is_clean(idx)
      });

   if inline {
      const STACK_PARTS: usize = 32;
      if raw.len() <= STACK_PARTS {
         let mut stack_parts = [""; STACK_PARTS];
         let mut binaries: Vec<(usize, Vec<u8>)> = Vec::new();
         for (i, &part) in raw.iter().enumerate() {
            let bytes = unsafe { part_bytes(part) };
            match std::str::from_utf8(bytes) {
               Ok(s) => stack_parts[i] = s,
               Err(_) if i >= 3 => {
                  binaries.push((i, bytes.to_vec()));
                  stack_parts[i] = "";
               }
               Err(_) => {
                  conn
                     .parser
                     .wbuf
                     .extend_from_slice(b"-ERR invalid UTF-8 in request\r\n");
                  return;
               }
            }
         }
         crate::commands::ft::search_dispatch(
            &stack_parts[..raw.len()],
            &binaries,
            conn.store,
            &mut conn.parser.wbuf,
         );
         return;
      }
   }

   // Owned copies: the job outlives this read buffer when submitted to pool.
   let mut parts: Vec<String> = Vec::with_capacity(raw.len());
   let mut binaries: Vec<(usize, Vec<u8>)> = Vec::new();
   for (i, &part) in raw.iter().enumerate() {
      let bytes = unsafe { part_bytes(part) };
      match std::str::from_utf8(bytes) {
         Ok(s) => parts.push(s.to_string()),
         Err(_) if i >= 3 => {
            binaries.push((i, bytes.to_vec()));
            parts.push(String::new());
         }
         Err(_) => {
            conn
               .parser
               .wbuf
               .extend_from_slice(b"-ERR invalid UTF-8 in request\r\n");
            return;
         }
      }
   }
   let store = Arc::clone(conn.store);
   let run = move |out: &mut Vec<u8>| {
      let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
      crate::commands::ft::search_dispatch(&refs, &binaries, &store, out);
   };
   if search_offloaded() && !inline {
      conn.submit(run);
   } else {
      run(&mut conn.parser.wbuf);
   }
}
