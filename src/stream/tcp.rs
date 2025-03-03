/*
 * Copyright (C) 2021 Evtech Solutions, Ltd., dba 3D-P
 * Copyright (C) 2021 Neil Tallim <neiltallim@3d-p.com>
 *
 * This file is part of rperf.
 *
 * rperf is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * rperf is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with rperf.  If not, see <https://www.gnu.org/licenses/>.
 */

use crate::protocol::results::{get_unix_timestamp, TcpReceiveResult, TcpSendResult};
use crate::stream::{parse_port_spec, TestStream, INTERVAL};
use crate::BoxResult;

pub const TEST_HEADER_SIZE: usize = 16;

#[cfg(unix)]
const KEEPALIVE_DURATION: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
pub struct TcpTestDefinition {
    //a UUID used to identify packets associated with this test
    pub test_id: [u8; 16],
    //bandwidth target, in bytes/sec
    pub bandwidth: u64,
    //the length of the buffer to exchange
    pub length: usize,
}
impl TcpTestDefinition {
    pub fn new(details: &serde_json::Value) -> BoxResult<TcpTestDefinition> {
        let mut test_id_bytes = [0_u8; 16];
        for (i, v) in details
            .get("test_id")
            .unwrap_or(&serde_json::json!([]))
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            if i >= 16 {
                //avoid out-of-bounds if given malicious data
                break;
            }
            test_id_bytes[i] = v.as_i64().unwrap_or(0) as u8;
        }

        let length = details
            .get("length")
            .unwrap_or(&serde_json::json!(TEST_HEADER_SIZE))
            .as_i64()
            .unwrap() as usize;
        if length < TEST_HEADER_SIZE {
            return Err(Box::new(simple_error::simple_error!(std::format!(
                "{} is too short of a length to satisfy testing requirements",
                length
            ))));
        }

        Ok(TcpTestDefinition {
            test_id: test_id_bytes,
            bandwidth: details.get("bandwidth").unwrap_or(&serde_json::json!(0.0)).as_f64().unwrap() as u64,
            length,
        })
    }
}

pub mod receiver {
    use mio::net::{TcpListener, TcpStream};
    use std::io::Read;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use crate::{protocol::results::IntervalResultBox, BoxResult};

    const POLL_TIMEOUT: Duration = Duration::from_millis(250);
    const CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);
    const RECEIVE_TIMEOUT: Duration = Duration::from_secs(3);

    pub struct TcpPortPool {
        pub ports_ip4: Vec<u16>,
        pub ports_ip6: Vec<u16>,
        pos_ip4: usize,
        pos_ip6: usize,
        lock_ip4: Mutex<u8>,
        lock_ip6: Mutex<u8>,
    }
    impl TcpPortPool {
        pub fn new(port_spec: &str, port_spec6: &str) -> TcpPortPool {
            let ports = super::parse_port_spec(port_spec);
            if !ports.is_empty() {
                log::debug!("configured IPv4 TCP port pool: {:?}", ports);
            } else {
                log::debug!("using OS assignment for IPv4 TCP ports");
            }

            let ports6 = super::parse_port_spec(port_spec6);
            if !ports.is_empty() {
                log::debug!("configured IPv6 TCP port pool: {:?}", ports6);
            } else {
                log::debug!("using OS assignment for IPv6 TCP ports");
            }

            TcpPortPool {
                ports_ip4: ports,
                pos_ip4: 0,
                lock_ip4: Mutex::new(0),

                ports_ip6: ports6,
                pos_ip6: 0,
                lock_ip6: Mutex::new(0),
            }
        }

        pub fn bind(&mut self, peer_ip: &IpAddr) -> BoxResult<TcpListener> {
            match peer_ip {
                IpAddr::V6(_) => {
                    if self.ports_ip6.is_empty() {
                        return Ok(TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))
                            .expect("failed to bind OS-assigned IPv6 TCP socket"));
                    } else {
                        let _guard = self.lock_ip6.lock().unwrap();

                        for port_idx in (self.pos_ip6 + 1)..self.ports_ip6.len() {
                            //iterate to the end of the pool; this will skip the first element in the pool initially, but that's fine
                            let listener_result =
                                TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), self.ports_ip6[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip6 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv6 TCP port {}", self.ports_ip6[port_idx]);
                            }
                        }
                        for port_idx in 0..=self.pos_ip6 {
                            //circle back to where the search started
                            let listener_result =
                                TcpListener::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), self.ports_ip6[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip6 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv6 TCP port {}", self.ports_ip6[port_idx]);
                            }
                        }
                    }
                    Err(Box::new(simple_error::simple_error!("unable to allocate IPv6 TCP port")))
                }
                IpAddr::V4(_) => {
                    if self.ports_ip4.is_empty() {
                        return Ok(TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
                            .expect("failed to bind OS-assigned IPv4 TCP socket"));
                    } else {
                        let _guard = self.lock_ip4.lock().unwrap();

                        for port_idx in (self.pos_ip4 + 1)..self.ports_ip4.len() {
                            //iterate to the end of the pool; this will skip the first element in the pool initially, but that's fine
                            let listener_result =
                                TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.ports_ip4[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip4 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv4 TCP port {}", self.ports_ip4[port_idx]);
                            }
                        }
                        for port_idx in 0..=self.pos_ip4 {
                            //circle back to where the search started
                            let listener_result =
                                TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.ports_ip4[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip4 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv4 TCP port {}", self.ports_ip4[port_idx]);
                            }
                        }
                    }
                    Err(Box::new(simple_error::simple_error!("unable to allocate IPv4 TCP port")))
                }
            }
        }
    }

    pub struct TcpReceiver {
        active: AtomicBool,
        test_definition: super::TcpTestDefinition,
        stream_idx: u8,

        listener: Option<TcpListener>,
        stream: Option<TcpStream>,

        mio_events: mio::Events,
        mio_poll: mio::Poll,
        mio_token: mio::Token,
    }

    impl TcpReceiver {
        pub fn new(
            test_definition: super::TcpTestDefinition,
            stream_idx: &u8,
            port_pool: &mut TcpPortPool,
            peer_ip: &IpAddr,
        ) -> BoxResult<TcpReceiver> {
            log::debug!("binding TCP listener for stream {}...", stream_idx);
            let mut listener: TcpListener = port_pool.bind(peer_ip).expect("failed to bind TCP socket");
            log::debug!("bound TCP listener for stream {}: {}", stream_idx, listener.local_addr()?);

            let _port = listener.local_addr()?.port();

            let mio_events = mio::Events::with_capacity(1);
            let mio_poll = mio::Poll::new()?;
            let mio_token = crate::get_global_token();
            mio_poll.registry().register(&mut listener, mio_token, mio::Interest::READABLE)?;

            Ok(TcpReceiver {
                active: AtomicBool::new(true),
                test_definition,
                stream_idx: stream_idx.to_owned(),

                listener: Some(listener),
                stream: None,
                mio_events,
                mio_token,
                mio_poll,
            })
        }

        fn process_connection(&mut self) -> BoxResult<(TcpStream, u64, f32)> {
            log::debug!("preparing to receive TCP stream {} connection...", self.stream_idx);

            let listener = self.listener.as_mut().unwrap();
            let mio_token = self.mio_token;

            let start = Instant::now();

            loop {
                if !self.active.load(Relaxed) {
                    return Err(Box::new(simple_error::simple_error!("local shutdown requested")));
                }
                if start.elapsed() >= RECEIVE_TIMEOUT {
                    let err = simple_error::simple_error!("TCP listening for stream {} timed out", self.stream_idx);
                    return Err(Box::new(err));
                }

                // assigned upon establishing a connection
                let mut stream: Option<TcpStream> = None;

                self.mio_poll.poll(&mut self.mio_events, Some(POLL_TIMEOUT))?;
                for event in self.mio_events.iter() {
                    if event.token() != mio_token {
                        log::warn!("got event for unbound token: {:?}", event);
                        continue;
                    }

                    let (mut new_stream, address) = match listener.accept() {
                        Ok((new_stream, address)) => (new_stream, address),
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            // no pending connections available
                            break;
                        }
                        Err(e) => {
                            return Err(Box::new(e));
                        }
                    };

                    log::debug!("received TCP stream {} connection from {}", self.stream_idx, address);

                    // hand over flow to the new connection
                    self.mio_poll.registry().deregister(listener)?;
                    self.mio_poll
                        .registry()
                        .register(&mut new_stream, mio_token, mio::Interest::READABLE)?;
                    stream = Some(new_stream);
                    break;
                }

                if stream.is_none() {
                    // no pending connections available
                    continue;
                }
                let mut unwrapped_stream = stream.unwrap();

                // process the stream
                let mut buf = vec![0_u8; self.test_definition.length];
                let mut validated: bool = false;
                let mut bytes_received: u64 = 0;

                let start_validation = Instant::now();

                self.mio_poll.poll(&mut self.mio_events, Some(CONNECTION_TIMEOUT))?;
                for event in self.mio_events.iter() {
                    if event.token() == mio_token {
                        loop {
                            let packet_size = match unwrapped_stream.read(&mut buf) {
                                Ok(packet_size) => packet_size,
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                                    // end of input reached
                                    break;
                                }
                                Err(e) => {
                                    let _ = unwrapped_stream.shutdown(std::net::Shutdown::Both);
                                    self.mio_poll.registry().deregister(&mut unwrapped_stream)?;
                                    return Err(Box::new(e));
                                }
                            };

                            if !validated {
                                if buf[..16] == self.test_definition.test_id {
                                    log::debug!(
                                        "validated TCP stream {} connection from {}",
                                        self.stream_idx,
                                        unwrapped_stream.peer_addr()?
                                    );
                                    validated = true;
                                } else {
                                    log::warn!(
                                        "unexpected ID in stream {} connection from {}",
                                        self.stream_idx,
                                        unwrapped_stream.peer_addr()?
                                    );
                                    break;
                                }
                            }

                            if validated {
                                bytes_received += packet_size as u64;
                            }
                        }
                    }
                }
                if validated {
                    return Ok((unwrapped_stream, bytes_received, start_validation.elapsed().as_secs_f32()));
                }

                let _ = unwrapped_stream.shutdown(std::net::Shutdown::Both);
                self.mio_poll.registry().deregister(&mut unwrapped_stream)?;
                log::warn!(
                    "could not validate TCP stream {} connection from {}",
                    self.stream_idx,
                    unwrapped_stream.peer_addr()?
                );
            }
        }
    }

    impl super::TestStream for TcpReceiver {
        fn run_interval(&mut self) -> Option<BoxResult<IntervalResultBox>> {
            let mut bytes_received: u64 = 0;

            let mut additional_time_elapsed: f32 = 0.0;
            if self.stream.is_none() {
                //if still in the setup phase, receive the sender
                match self.process_connection() {
                    Ok((stream, bytes_received_in_validation, time_spent_in_validation)) => {
                        self.stream = Some(stream);
                        // NOTE: the connection process consumes packets; account for those bytes
                        bytes_received += bytes_received_in_validation;
                        additional_time_elapsed += time_spent_in_validation;
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
                self.listener = None; //drop it, closing the socket
            }
            let stream = self.stream.as_mut().unwrap();

            let mio_token = self.mio_token;
            let mut buf = vec![0_u8; self.test_definition.length];

            let peer_addr = match stream.peer_addr() {
                Ok(pa) => pa,
                Err(e) => return Some(Err(Box::new(e))),
            };
            let start = Instant::now();

            while self.active.load(Relaxed) && start.elapsed() < super::INTERVAL {
                log::trace!("awaiting TCP stream {} from {}...", self.stream_idx, peer_addr);
                self.mio_poll.poll(&mut self.mio_events, Some(POLL_TIMEOUT)).ok()?;
                for event in self.mio_events.iter() {
                    if event.token() == mio_token {
                        loop {
                            let packet_size = match stream.read(&mut buf) {
                                Ok(packet_size) => packet_size,
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                                    // receive timeout
                                    break;
                                }
                                Err(e) => {
                                    return Some(Err(Box::new(e)));
                                }
                            };

                            log::trace!(
                                "received {} bytes in TCP stream {} from {}",
                                packet_size,
                                self.stream_idx,
                                peer_addr
                            );
                            if packet_size == 0 {
                                // test's over
                                // HACK: can't call self.stop() because it's a double-borrow due to the unwrapped stream
                                self.active.store(false, Relaxed);
                                break;
                            }

                            bytes_received += packet_size as u64;
                        }
                    } else {
                        log::warn!("got event for unbound token: {:?}", event);
                    }
                }
            }
            if bytes_received > 0 {
                log::debug!(
                    "{} bytes received via TCP stream {} from {} in this interval; reporting...",
                    bytes_received,
                    self.stream_idx,
                    peer_addr
                );
                Some(Ok(Box::new(super::TcpReceiveResult {
                    timestamp: super::get_unix_timestamp(),

                    stream_idx: self.stream_idx,

                    duration: start.elapsed().as_secs_f32() + additional_time_elapsed,

                    bytes_received,
                })))
            } else {
                log::debug!(
                    "no bytes received via TCP stream {} from {} in this interval",
                    self.stream_idx,
                    peer_addr
                );
                None
            }
        }

        fn get_port(&self) -> BoxResult<u16> {
            match &self.listener {
                Some(listener) => Ok(listener.local_addr()?.port()),
                None => match &self.stream {
                    Some(stream) => Ok(stream.local_addr()?.port()),
                    None => Err(Box::new(simple_error::simple_error!("no port currently bound"))),
                },
            }
        }

        fn get_idx(&self) -> u8 {
            self.stream_idx.to_owned()
        }

        fn stop(&mut self) {
            self.active.store(false, Relaxed);
        }
    }
}

pub mod sender {
    use std::io::Write;
    use std::net::{IpAddr, SocketAddr, TcpStream};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use crate::{protocol::results::IntervalResultBox, BoxResult};

    const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
    const WRITE_TIMEOUT: Duration = Duration::from_millis(50);
    const BUFFER_FULL_TIMEOUT: Duration = Duration::from_millis(1);

    #[allow(dead_code)]
    pub struct TcpSender {
        active: bool,
        test_definition: super::TcpTestDefinition,
        stream_idx: u8,

        socket_addr: SocketAddr,
        stream: Option<TcpStream>,

        //the interval, in seconds, at which to send data
        send_interval: f32,

        remaining_duration: f32,
        staged_buffer: Vec<u8>,

        send_buffer: usize,
        no_delay: bool,
    }
    impl TcpSender {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            test_definition: super::TcpTestDefinition,
            stream_idx: &u8,
            receiver_ip: &IpAddr,
            receiver_port: &u16,
            send_duration: &f32,
            send_interval: &f32,
            send_buffer: &usize,
            no_delay: &bool,
        ) -> BoxResult<TcpSender> {
            let mut staged_buffer = vec![0_u8; test_definition.length];
            for (i, staged_buffer_i) in staged_buffer.iter_mut().enumerate().skip(super::TEST_HEADER_SIZE) {
                //fill the packet with a fixed sequence
                *staged_buffer_i = (i % 256) as u8;
            }
            //embed the test ID
            staged_buffer[0..16].copy_from_slice(&test_definition.test_id);

            Ok(TcpSender {
                active: true,
                test_definition,
                stream_idx: stream_idx.to_owned(),

                socket_addr: SocketAddr::new(*receiver_ip, *receiver_port),
                stream: None,

                send_interval: send_interval.to_owned(),

                remaining_duration: send_duration.to_owned(),
                staged_buffer,

                send_buffer: send_buffer.to_owned(),
                no_delay: no_delay.to_owned(),
            })
        }

        fn process_connection(&mut self) -> BoxResult<TcpStream> {
            log::debug!("preparing to connect TCP stream {} to {} ...", self.stream_idx, self.socket_addr);

            let stream = match TcpStream::connect_timeout(&self.socket_addr, CONNECT_TIMEOUT) {
                Ok(s) => s,
                Err(e) => {
                    let err = simple_error::simple_error!("unable to connect stream {}: {}", self.stream_idx, e);
                    return Err(Box::new(err));
                }
            };

            stream.set_nonblocking(true)?;
            stream.set_write_timeout(Some(WRITE_TIMEOUT))?;

            // NOTE: features unsupported on Windows
            #[cfg(unix)]
            {
                use crate::stream::tcp::KEEPALIVE_DURATION;
                let keepalive_parameters = socket2::TcpKeepalive::new().with_time(KEEPALIVE_DURATION);
                let raw_socket = socket2::SockRef::from(&stream);
                raw_socket.set_tcp_keepalive(&keepalive_parameters)?;
            }

            log::debug!("connected TCP stream {} to {}", self.stream_idx, stream.peer_addr()?);

            if self.no_delay {
                log::debug!("setting no-delay...");
                stream.set_nodelay(true)?;
            }
            #[cfg(unix)]
            if self.send_buffer != 0 {
                log::debug!("setting send-buffer to {}...", self.send_buffer);
                let raw_socket = socket2::SockRef::from(&stream);
                raw_socket.set_send_buffer_size(self.send_buffer)?;
            }
            Ok(stream)
        }
    }
    impl super::TestStream for TcpSender {
        fn run_interval(&mut self) -> Option<BoxResult<IntervalResultBox>> {
            if self.stream.is_none() {
                //if still in the setup phase, connect to the receiver
                match self.process_connection() {
                    Ok(stream) => {
                        self.stream = Some(stream);
                    }
                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }
            let stream = self.stream.as_mut().unwrap();

            let interval_duration = Duration::from_secs_f32(self.send_interval);
            let mut interval_iteration = 0;
            let bytes_to_send = ((self.test_definition.bandwidth as f32) * super::INTERVAL.as_secs_f32()) as i64;
            let mut bytes_to_send_remaining = bytes_to_send;
            let bytes_to_send_per_interval_slice = ((bytes_to_send as f32) * self.send_interval) as i64;
            let mut bytes_to_send_per_interval_slice_remaining = bytes_to_send_per_interval_slice;

            let mut sends_blocked: u64 = 0;
            let mut bytes_sent: u64 = 0;

            let peer_addr = match stream.peer_addr() {
                Ok(pa) => pa,
                Err(e) => return Some(Err(Box::new(e))),
            };
            let cycle_start = Instant::now();

            while self.active && self.remaining_duration > 0.0 && bytes_to_send_remaining > 0 {
                log::trace!(
                    "writing {} bytes in TCP stream {} to {}...",
                    self.staged_buffer.len(),
                    self.stream_idx,
                    peer_addr
                );
                let packet_start = Instant::now();

                let packet_size = match stream.write(&self.staged_buffer) {
                    // it doesn't matter if the whole thing couldn't be written, since it's just garbage data
                    Ok(packet_size) => packet_size,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // send-buffer is full
                        // nothing to do, but avoid burning CPU cycles
                        sleep(BUFFER_FULL_TIMEOUT);
                        sends_blocked += 1;
                        continue;
                    }
                    Err(e) => {
                        return Some(Err(Box::new(e)));
                    }
                };
                log::trace!("wrote {} bytes in TCP stream {} to {}", packet_size, self.stream_idx, peer_addr);

                let bytes_written = packet_size as i64;
                bytes_sent += bytes_written as u64;
                bytes_to_send_remaining -= bytes_written;
                bytes_to_send_per_interval_slice_remaining -= bytes_written;

                let elapsed_time = cycle_start.elapsed();
                if elapsed_time >= super::INTERVAL {
                    self.remaining_duration -= packet_start.elapsed().as_secs_f32();

                    log::debug!(
                        "{} bytes sent via TCP stream {} to {} in this interval; reporting...",
                        bytes_sent,
                        self.stream_idx,
                        peer_addr
                    );
                    return Some(Ok(Box::new(super::TcpSendResult {
                        timestamp: super::get_unix_timestamp(),

                        stream_idx: self.stream_idx,

                        duration: elapsed_time.as_secs_f32(),

                        bytes_sent,
                        sends_blocked,
                    })));
                }

                if bytes_to_send_remaining <= 0 {
                    //interval's target is exhausted, so sleep until the end
                    let elapsed_time = cycle_start.elapsed();
                    if super::INTERVAL > elapsed_time {
                        sleep(super::INTERVAL - elapsed_time);
                    }
                } else if bytes_to_send_per_interval_slice_remaining <= 0 {
                    // interval subsection exhausted
                    interval_iteration += 1;
                    bytes_to_send_per_interval_slice_remaining = bytes_to_send_per_interval_slice;
                    let elapsed_time = cycle_start.elapsed();
                    let interval_endtime = interval_iteration * interval_duration;
                    if interval_endtime > elapsed_time {
                        sleep(interval_endtime - elapsed_time);
                    }
                }
                self.remaining_duration -= packet_start.elapsed().as_secs_f32();
            }
            if bytes_sent > 0 {
                log::debug!(
                    "{} bytes sent via TCP stream {} to {} in this interval; reporting...",
                    bytes_sent,
                    self.stream_idx,
                    peer_addr
                );
                Some(Ok(Box::new(super::TcpSendResult {
                    timestamp: super::get_unix_timestamp(),

                    stream_idx: self.stream_idx,

                    duration: cycle_start.elapsed().as_secs_f32(),

                    bytes_sent,
                    sends_blocked,
                })))
            } else {
                log::debug!(
                    "no bytes sent via TCP stream {} to {} in this interval; shutting down...",
                    self.stream_idx,
                    peer_addr
                );
                //indicate that the test is over by dropping the stream
                self.stream = None;
                None
            }
        }

        fn get_port(&self) -> BoxResult<u16> {
            match &self.stream {
                Some(stream) => Ok(stream.local_addr()?.port()),
                None => Err(Box::new(simple_error::simple_error!("no stream currently exists"))),
            }
        }

        fn get_idx(&self) -> u8 {
            self.stream_idx.to_owned()
        }

        fn stop(&mut self) {
            self.active = false;
        }
    }
}
