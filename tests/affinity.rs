#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server {
   child: Child,
   directory: PathBuf,
}

impl Drop for Server {
   fn drop(&mut self) {
      let _ = self.child.kill();
      let _ = self.child.wait();
      let _ = std::fs::remove_dir_all(&self.directory);
   }
}

#[test]
fn automatic_affinity_pins_each_worker_within_a_restricted_cpu_set() {
   let mut allowed: libc::cpu_set_t = unsafe { std::mem::zeroed() };
   if unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&allowed), &mut allowed) } != 0 {
      return;
   }
   let cores: Vec<_> = (0..libc::CPU_SETSIZE as usize)
      .filter(|&core| unsafe { libc::CPU_ISSET(core, &allowed) })
      .collect();
   if cores.len() < 2 || cores == [0, 1] {
      return;
   }
   let pair = [cores[0], *cores.last().unwrap()];
   let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
   let port = reservation.local_addr().unwrap().port();
   let directory =
      std::env::temp_dir().join(format!("fyro-affinity-{}-{port}", std::process::id()));
   std::fs::create_dir_all(&directory).unwrap();
   let mut command = Command::new(env!("CARGO_BIN_EXE_fyro_db"));
   for (name, _) in std::env::vars().filter(|(name, _)| name.starts_with("FYRODB_")) {
      command.env_remove(name);
   }
   command
      .env("FYRODB_BIND", "127.0.0.1")
      .env("FYRODB_PORT", port.to_string())
      .env("FYRODB_WORKERS", "2")
      .env("FYRODB_AFFINITY", "auto")
      .env("FYRODB_MAX_KEYS", "0")
      .env("FYRODB_RDB_INTERVAL", "0")
      .env("FYRODB_RDB_PATH", directory.join("snapshot.rdb"))
      .stdout(std::fs::File::create(directory.join("stdout.log")).unwrap())
      .stderr(Stdio::null());
   unsafe {
      command.pre_exec(move || {
         let mut set: libc::cpu_set_t = std::mem::zeroed();
         libc::CPU_ZERO(&mut set);
         for core in pair {
            libc::CPU_SET(core, &mut set);
         }
         if libc::sched_setaffinity(0, std::mem::size_of_val(&set), &set) == 0 {
            Ok(())
         } else {
            Err(std::io::Error::last_os_error())
         }
      });
   }
   drop(reservation);
   let mut server = Server {
      child: command.spawn().unwrap(),
      directory,
   };
   let deadline = Instant::now() + Duration::from_secs(5);
   let mut stream = loop {
      if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
         break stream;
      }
      assert!(server.child.try_wait().unwrap().is_none());
      assert!(Instant::now() < deadline, "server startup timed out");
      std::thread::sleep(Duration::from_millis(10));
   };
   stream
      .set_read_timeout(Some(Duration::from_secs(5)))
      .unwrap();
   stream.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
   let mut reply = [0; 7];
   stream.read_exact(&mut reply).unwrap();
   assert_eq!(&reply, b"+PONG\r\n");
   let mut expected = pair.map(|core| core.to_string()).to_vec();
   expected.sort();
   loop {
      let mut actual = Vec::new();
      for task in std::fs::read_dir(format!("/proc/{}/task", server.child.id())).unwrap() {
         let path = task.unwrap().path();
         let Ok(name) = std::fs::read_to_string(path.join("comm")) else {
            continue;
         };
         if name.trim() != "fyrodb-worker" {
            continue;
         }
         let status = std::fs::read_to_string(path.join("status")).unwrap();
         actual.push(
            status
               .lines()
               .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
               .unwrap()
               .trim()
               .to_string(),
         );
      }
      actual.sort();
      if actual == expected {
         break;
      }
      assert!(
         Instant::now() < deadline,
         "worker CPU masks: {actual:?}, expected: {expected:?}"
      );
      std::thread::sleep(Duration::from_millis(10));
   }
   let output = std::fs::read_to_string(server.directory.join("stdout.log")).unwrap();
   assert!(output.contains(&format!("worker_cores=[{}, {}]", pair[0], pair[1])));
}
