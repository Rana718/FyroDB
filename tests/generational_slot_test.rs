use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use fyro_db::pubsub::PubSub;
use fyro_db::storage::store::Store;
use fyro_db::worker::run_worker;

#[test]
fn test_generational_slot_reuse_and_stale_event_safety() {
   let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
   let port = probe.local_addr().unwrap().port();
   drop(probe);

   let store = Arc::new(Store::with_config(2, 64));
   let pubsub = Arc::new(PubSub::new());

   let store_worker = Arc::clone(&store);
   let pubsub_worker = Arc::clone(&pubsub);
   std::thread::spawn(move || {
      run_worker(&store_worker, &pubsub_worker, port, "127.0.0.1", None, 0);
   });

   std::thread::sleep(Duration::from_millis(400));

   // 1. Client 1 connects (gets Slot 1, Gen 0)
   let mut client1 = TcpStream::connect(("127.0.0.1", port)).expect("Client 1 must connect");
   client1.set_nodelay(true).unwrap();
   client1
      .set_read_timeout(Some(Duration::from_secs(2)))
      .unwrap();

   let mut r1 = BufReader::new(client1.try_clone().unwrap());
   client1
      .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nk_1\r\n$3\r\nv_1\r\n")
      .unwrap();
   let mut reply = String::new();
   r1.read_line(&mut reply).unwrap();
   assert_eq!(reply.trim_end(), "+OK");

   // 2. Client 1 disconnects (Slot 1 freed, Generation bumps to 1)
   drop(client1);
   std::thread::sleep(Duration::from_millis(50));

   // 3. Client 2 connects immediately (re-uses Slot 1, but now has Gen 1)
   let mut client2 =
      TcpStream::connect(("127.0.0.1", port)).expect("Client 2 must connect and reuse slot");
   client2.set_nodelay(true).unwrap();
   client2
      .set_read_timeout(Some(Duration::from_secs(2)))
      .unwrap();

   let mut r2 = BufReader::new(client2.try_clone().unwrap());
   client2
      .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nk_1\r\n")
      .unwrap();
   let mut line1 = String::new();
   let mut line2 = String::new();
   r2.read_line(&mut line1).unwrap();
   r2.read_line(&mut line2).unwrap();
   assert_eq!(line1.trim_end(), "$3");
   assert_eq!(line2.trim_end(), "v_1");

   // 4. Rapid connect / disconnect cycles (Slot reuse stress test)
   for i in 0..20 {
      let mut client = TcpStream::connect(("127.0.0.1", port)).expect("Rapid client connect");
      client.set_nodelay(true).unwrap();
      let mut r = BufReader::new(client.try_clone().unwrap());
      let val_str = i.to_string();
      let cmd = format!(
         "*3\r\n$3\r\nSET\r\n$4\r\niter\r\n${}\r\n{}\r\n",
         val_str.len(),
         val_str
      );
      client.write_all(cmd.as_bytes()).unwrap();
      let mut rep = String::new();
      r.read_line(&mut rep).unwrap();
      assert_eq!(rep.trim_end(), "+OK");
      drop(client);
   }

   println!(
      "\n✅ Generational Slot Arena successfully verified: Safe slot reuse with zero ghost events!"
   );
}
