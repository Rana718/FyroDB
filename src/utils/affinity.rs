pub fn parse_cpu_list(s: &str) -> Vec<usize> {
   let mut out = Vec::new();
   for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
      match part.split_once('-') {
         Some((a, b)) => {
            if let (Ok(a), Ok(b)) = (a.trim().parse::<usize>(), b.trim().parse::<usize>()) {
               out.extend(a.min(b)..=a.max(b));
            }
         }
         None => {
            if let Ok(c) = part.parse() {
               out.push(c);
            }
         }
      }
   }
   out
}

pub fn pin_current(core: usize) -> bool {
   #[cfg(target_os = "linux")]
   {
      if core >= libc::CPU_SETSIZE as usize {
         return false;
      }
      unsafe {
         let mut set: libc::cpu_set_t = std::mem::zeroed();
         libc::CPU_ZERO(&mut set);
         libc::CPU_SET(core, &mut set);
         libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
      }
   }
   #[cfg(not(target_os = "linux"))]
   {
      let _ = core;
      false
   }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Plan {
   pub workers: Option<Vec<usize>>,
   pub search: Option<Vec<usize>>,
}

fn plan_on_cores(
   workers: usize,
   cores: impl ExactSizeIterator<Item = usize> + Clone,
   env: impl Fn(&str) -> Option<String>,
) -> Plan {
   let list = |k: &str| env(k).map(|v| parse_cpu_list(&v)).filter(|v| !v.is_empty());
   let auto = env("FYRODB_AFFINITY").is_some_and(|v| v.eq_ignore_ascii_case("auto"));
   let workers_cores =
      list("FYRODB_WORKER_CORES").or_else(|| auto.then(|| cores.clone().take(workers).collect()));
   let search_cores = list("FYRODB_SEARCH_CORES")
      .or_else(|| (auto && workers < cores.len()).then(|| cores.skip(workers).collect()));
   Plan {
      workers: workers_cores,
      search: search_cores,
   }
}

pub fn plan(workers: usize, ncpu: usize, env: impl Fn(&str) -> Option<String>) -> Plan {
   plan_on_cores(workers, 0..ncpu, env)
}

#[cfg(target_os = "linux")]
fn allowed_cores() -> Option<Vec<usize>> {
   let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
   if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&set), &mut set) } != 0 {
      return None;
   }
   let cores: Vec<_> = (0..libc::CPU_SETSIZE as usize)
      .filter(|&core| unsafe { libc::CPU_ISSET(core, &set) })
      .collect();
   (!cores.is_empty()).then_some(cores)
}

#[cfg(not(target_os = "linux"))]
fn allowed_cores() -> Option<Vec<usize>> {
   None
}

pub fn from_env(workers: usize) -> Plan {
   let env = |k: &str| std::env::var(k).ok();
   if env("FYRODB_AFFINITY").is_some_and(|v| v.eq_ignore_ascii_case("auto"))
      && let Some(cores) = allowed_cores()
   {
      plan_on_cores(workers, cores.iter().copied(), env)
   } else {
      plan(workers, num_cpus::get(), env)
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn parses_ranges_and_lists() {
      assert_eq!(parse_cpu_list("0-3, 8,10-9"), vec![0, 1, 2, 3, 8, 9, 10]);
      assert_eq!(parse_cpu_list("x,,2"), vec![2]);
   }

   #[test]
   fn auto_splits_workers_and_search_disjointly() {
      let env = |k: &str| (k == "FYRODB_AFFINITY").then(|| "auto".to_string());
      let p = plan(4, 12, env);
      assert_eq!(p.workers, Some(vec![0, 1, 2, 3]));
      assert_eq!(p.search, Some((4..12).collect()));
      let p = plan(12, 12, env);
      assert_eq!(p.search, None);
      let explicit = |k: &str| match k {
         "FYRODB_WORKER_CORES" => Some("2-3".to_string()),
         _ => None,
      };
      assert_eq!(
         plan(2, 8, explicit),
         Plan {
            workers: Some(vec![2, 3]),
            search: None
         }
      );
   }

   #[test]
   fn auto_respects_sparse_allowed_cpus_and_explicit_overrides() {
      let cores = [2, 6, 9, 11];
      let auto = |k: &str| (k == "FYRODB_AFFINITY").then(|| "auto".to_string());
      assert_eq!(
         plan_on_cores(2, cores.into_iter(), auto),
         Plan {
            workers: Some(vec![2, 6]),
            search: Some(vec![9, 11]),
         }
      );
      assert_eq!(
         plan_on_cores(8, cores.into_iter(), auto).workers,
         Some(cores.to_vec())
      );
      assert_eq!(plan_on_cores(8, cores.into_iter(), auto).search, None);
      let explicit = |k: &str| match k {
         "FYRODB_AFFINITY" => Some("auto".to_string()),
         "FYRODB_WORKER_CORES" => Some("11,2".to_string()),
         "FYRODB_SEARCH_CORES" => Some("6".to_string()),
         _ => None,
      };
      assert_eq!(
         plan_on_cores(2, cores.into_iter(), explicit),
         Plan {
            workers: Some(vec![11, 2]),
            search: Some(vec![6]),
         }
      );
   }

   #[test]
   fn invalid_cpu_index_does_not_panic_or_change_affinity() {
      assert!(!pin_current(usize::MAX));
   }
}
