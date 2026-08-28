use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use super::{Frame, FrameCodec, ProtocolError};

pub struct PeerConnection {
    stream: TcpStream,
    codec: FrameCodec,
}

impl PeerConnection {
    pub fn connect(
        address: SocketAddr,
        timeout: Duration,
        codec: FrameCodec,
    ) -> Result<Self, ProtocolError> {
        let stream = TcpStream::connect_timeout(&address, timeout)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Self { stream, codec })
    }

    pub fn from_stream(stream: TcpStream, codec: FrameCodec) -> Result<Self, ProtocolError> {
        stream.set_nodelay(true)?;
        Ok(Self { stream, codec })
    }

    pub fn send(&mut self, frame: &Frame) -> Result<(), ProtocolError> {
        self.codec.write_frame(&mut self.stream, frame)
    }

    pub fn receive(&mut self) -> Result<Frame, ProtocolError> {
        self.codec.read_frame(&mut self.stream)
    }

    pub fn peer_addr(&self) -> Result<SocketAddr, ProtocolError> {
        Ok(self.stream.peer_addr()?)
    }

    pub fn try_clone(&self) -> Result<Self, ProtocolError> {
        Ok(Self {
            stream: self.stream.try_clone()?,
            codec: self.codec.clone(),
        })
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<(), ProtocolError> {
        Ok(self.stream.set_read_timeout(timeout)?)
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> Result<(), ProtocolError> {
        Ok(self.stream.set_write_timeout(timeout)?)
    }
}
