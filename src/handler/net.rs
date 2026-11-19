use mio::event::Source;
use mio::net::{TcpListener, TcpStream};
#[cfg(feature = "unix-sockets")]
use mio::net::{UnixListener, UnixStream};
use mio::{Interest, Registry, Token};
use std::io::{self, Read, Write};

#[derive(Debug)]
pub enum Listener {
   Tcp(TcpListener),
   #[cfg(feature = "unix-sockets")]
   Unix(UnixListener),
}

impl Listener {
   pub fn accept(&self) -> io::Result<Stream> {
      match self {
         Listener::Tcp(l) => l.accept().map(|(stream, _)| Stream::Tcp(stream)),
         #[cfg(feature = "unix-sockets")]
         Listener::Unix(l) => l.accept().map(|(stream, _)| Stream::Unix(stream)),
      }
   }
}

impl Source for Listener {
   fn register(
      &mut self,
      registry: &Registry,
      token: Token,
      interests: Interest,
   ) -> io::Result<()> {
      match self {
         Listener::Tcp(l) => l.register(registry, token, interests),
         #[cfg(feature = "unix-sockets")]
         Listener::Unix(l) => l.register(registry, token, interests),
      }
   }

   fn reregister(
      &mut self,
      registry: &Registry,
      token: Token,
      interests: Interest,
   ) -> io::Result<()> {
      match self {
         Listener::Tcp(l) => l.reregister(registry, token, interests),
         #[cfg(feature = "unix-sockets")]
         Listener::Unix(l) => l.reregister(registry, token, interests),
      }
   }

   fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
      match self {
         Listener::Tcp(l) => l.deregister(registry),
         #[cfg(feature = "unix-sockets")]
         Listener::Unix(l) => l.deregister(registry),
      }
   }
}

#[cfg(unix)]
impl std::os::unix::io::AsRawFd for Listener {
   fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
      match self {
         Listener::Tcp(l) => l.as_raw_fd(),
         #[cfg(feature = "unix-sockets")]
         Listener::Unix(l) => l.as_raw_fd(),
      }
   }
}

#[cfg(unix)]
impl std::os::fd::AsFd for Listener {
   fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
      match self {
         Listener::Tcp(l) => l.as_fd(),
         #[cfg(feature = "unix-sockets")]
         Listener::Unix(l) => l.as_fd(),
      }
   }
}

#[derive(Debug)]
pub enum Stream {
   Tcp(TcpStream),
   #[cfg(feature = "unix-sockets")]
   Unix(UnixStream),
}

impl Read for Stream {
   fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      match self {
         Stream::Tcp(s) => s.read(buf),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.read(buf),
      }
   }

   fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
      match self {
         Stream::Tcp(s) => s.read_vectored(bufs),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.read_vectored(bufs),
      }
   }
}

impl Write for Stream {
   fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
      match self {
         Stream::Tcp(s) => s.write(buf),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.write(buf),
      }
   }

   fn flush(&mut self) -> io::Result<()> {
      match self {
         Stream::Tcp(s) => s.flush(),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.flush(),
      }
   }

   fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
      match self {
         Stream::Tcp(s) => s.write_vectored(bufs),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.write_vectored(bufs),
      }
   }
}

impl Source for Stream {
   fn register(
      &mut self,
      registry: &Registry,
      token: Token,
      interests: Interest,
   ) -> io::Result<()> {
      match self {
         Stream::Tcp(s) => s.register(registry, token, interests),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.register(registry, token, interests),
      }
   }

   fn reregister(
      &mut self,
      registry: &Registry,
      token: Token,
      interests: Interest,
   ) -> io::Result<()> {
      match self {
         Stream::Tcp(s) => s.reregister(registry, token, interests),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.reregister(registry, token, interests),
      }
   }

   fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
      match self {
         Stream::Tcp(s) => s.deregister(registry),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.deregister(registry),
      }
   }
}

#[cfg(unix)]
impl std::os::unix::io::AsRawFd for Stream {
   fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
      match self {
         Stream::Tcp(s) => s.as_raw_fd(),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.as_raw_fd(),
      }
   }
}

#[cfg(unix)]
impl std::os::fd::AsFd for Stream {
   fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
      match self {
         Stream::Tcp(s) => s.as_fd(),
         #[cfg(feature = "unix-sockets")]
         Stream::Unix(s) => s.as_fd(),
      }
   }
}

impl From<TcpStream> for Stream {
   fn from(s: TcpStream) -> Self {
      Stream::Tcp(s)
   }
}

#[cfg(feature = "unix-sockets")]
impl From<UnixStream> for Stream {
   fn from(s: UnixStream) -> Self {
      Stream::Unix(s)
   }
}
