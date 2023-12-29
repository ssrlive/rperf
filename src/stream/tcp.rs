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

pub const TEST_HEADER_SIZE: usize = 16;

#[cfg(unix)]
const KEEPALIVE_DURATION: std::time::Duration = std::time::Duration::from_secs(5);

fn mio_stream_to_std_stream(stream: mio::net::TcpStream) -> std::net::TcpStream {
    #[cfg(unix)]
    {
        use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
        let fd = unsafe { OwnedFd::from_raw_fd(stream.into_raw_fd()) };
        let socket: socket2::Socket = socket2::Socket::from(fd);
        socket.into()
    }

    #[cfg(windows)]
    {
        use std::os::windows::io::{FromRawSocket, IntoRawSocket, OwnedSocket};
        let socket = unsafe { OwnedSocket::from_raw_socket(stream.into_raw_socket()) };
        let socket: socket2::Socket = socket2::Socket::from(socket);
        socket.into()
    }
}

pub mod receiver {
    use mio::net::{TcpListener, TcpStream};
    use std::io::Read;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use crate::{
        error_gen,
        protocol::{
            messaging::{Configuration, Message, TransmitState},
            results::{get_unix_timestamp, IntervalResultBox, TcpReceiveResult},
        },
        stream::{parse_port_spec, tcp::sender::TcpSender, TestRunner, INTERVAL},
        BoxResult,
    };

    const POLL_TIMEOUT: Duration = Duration::from_millis(250);
    const CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);
    const RECEIVE_TIMEOUT: Duration = Duration::from_secs(3);

    /// a global token generator
    pub fn get_global_token() -> mio::Token {
        mio::Token(TOKEN_SEED.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
    }
    static TOKEN_SEED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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
            let ports = parse_port_spec(port_spec);
            if !ports.is_empty() {
                log::debug!("configured IPv4 TCP port pool: {:?}", ports);
            } else {
                log::debug!("using OS assignment for IPv4 TCP ports");
            }

            let ports6 = parse_port_spec(port_spec6);
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

        pub fn bind(&mut self, peer_ip: IpAddr) -> BoxResult<TcpListener> {
            let ipv6addr_unspec = IpAddr::V6(Ipv6Addr::UNSPECIFIED);
            let ipv4addr_unspec = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
            match peer_ip {
                IpAddr::V6(_) => {
                    if self.ports_ip6.is_empty() {
                        return Ok(TcpListener::bind(SocketAddr::new(ipv6addr_unspec, 0))?);
                    } else {
                        let _guard = self.lock_ip6.lock().unwrap();

                        for port_idx in (self.pos_ip6 + 1)..self.ports_ip6.len() {
                            //iterate to the end of the pool; this will skip the first element in the pool initially, but that's fine
                            let listener_result = TcpListener::bind(SocketAddr::new(ipv6addr_unspec, self.ports_ip6[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip6 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv6 TCP port {}", self.ports_ip6[port_idx]);
                            }
                        }
                        for port_idx in 0..=self.pos_ip6 {
                            //circle back to where the search started
                            let listener_result = TcpListener::bind(SocketAddr::new(ipv6addr_unspec, self.ports_ip6[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip6 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv6 TCP port {}", self.ports_ip6[port_idx]);
                            }
                        }
                    }
                    Err(Box::new(error_gen!("unable to allocate IPv6 TCP port")))
                }
                IpAddr::V4(_) => {
                    if self.ports_ip4.is_empty() {
                        return Ok(TcpListener::bind(SocketAddr::new(ipv4addr_unspec, 0))?);
                    } else {
                        let _guard = self.lock_ip4.lock().unwrap();

                        for port_idx in (self.pos_ip4 + 1)..self.ports_ip4.len() {
                            //iterate to the end of the pool; this will skip the first element in the pool initially, but that's fine
                            let listener_result = TcpListener::bind(SocketAddr::new(ipv4addr_unspec, self.ports_ip4[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip4 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv4 TCP port {}", self.ports_ip4[port_idx]);
                            }
                        }
                        for port_idx in 0..=self.pos_ip4 {
                            //circle back to where the search started
                            let listener_result = TcpListener::bind(SocketAddr::new(ipv4addr_unspec, self.ports_ip4[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip4 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv4 TCP port {}", self.ports_ip4[port_idx]);
                            }
                        }
                    }
                    Err(Box::new(error_gen!("unable to allocate IPv4 TCP port")))
                }
            }
        }
    }

    pub struct TcpReceiver {
        active: AtomicBool,
        cfg: Configuration,
        stream_idx: usize,

        listener: Option<TcpListener>,
        stream: Option<TcpStream>,

        mio_events: mio::Events,
        mio_poll: mio::Poll,
        mio_token: mio::Token,

        sender: Option<Box<TcpSender>>,
    }

    impl TcpReceiver {
        pub fn new(cfg: &Configuration, stream_idx: usize, port_pool: &mut TcpPortPool, peer_ip: IpAddr) -> BoxResult<TcpReceiver> {
            log::debug!("binding TCP listener for stream {}...", stream_idx);
            let mut listener: TcpListener = port_pool.bind(peer_ip)?;
            log::debug!("bound TCP listener for stream {}: {}", stream_idx, listener.local_addr()?);

            let mio_events = mio::Events::with_capacity(1);
            let mio_poll = mio::Poll::new()?;
            let mio_token = get_global_token();
            mio_poll.registry().register(&mut listener, mio_token, mio::Interest::READABLE)?;

            Ok(TcpReceiver {
                active: AtomicBool::new(true),
                cfg: cfg.clone(),
                stream_idx,

                listener: Some(listener),
                stream: None,
                mio_events,
                mio_token,
                mio_poll,

                sender: None,
            })
        }

        fn process_connection(&mut self) -> BoxResult<(TcpStream, u64, f32)> {
            log::debug!("preparing to receive TCP stream {} connection...", self.stream_idx);

            let listener = self.listener.as_mut().unwrap();
            let mio_token = self.mio_token;

            let start = Instant::now();

            loop {
                if !self.active.load(Relaxed) {
                    return Err(Box::new(error_gen!("local shutdown requested")));
                }
                if start.elapsed() >= RECEIVE_TIMEOUT {
                    return Err(Box::new(error_gen!("TCP listening for stream {} timed out", self.stream_idx)));
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
                    let interests = mio::Interest::READABLE;
                    self.mio_poll.registry().register(&mut new_stream, mio_token, interests)?;
                    stream = Some(new_stream);
                    break;
                }

                if stream.is_none() {
                    // no pending connections available
                    continue;
                }
                let mut unwrapped_stream = stream.unwrap();

                // process the stream
                let mut buf = vec![0_u8; self.cfg.length as usize];
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
                                if uuid::Uuid::from_bytes((&buf[..16]).try_into()?) == self.cfg.test_id {
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
                    self.listener = None; // drop it, closing the socket
                    return Ok((unwrapped_stream, bytes_received, start_validation.elapsed().as_secs_f32()));
                }

                let _ = unwrapped_stream.shutdown(std::net::Shutdown::Both);
                self.mio_poll.registry().deregister(&mut unwrapped_stream)?;
                self.mio_poll.registry().register(listener, mio_token, mio::Interest::READABLE)?;
                log::warn!(
                    "could not validate TCP stream {} connection from {}",
                    self.stream_idx,
                    unwrapped_stream.peer_addr()?
                );
            }
        }

        fn process_stream_receive(&mut self, _bytes_received: u64, _additional_time_elapsed: f32) -> BoxResult<Option<IntervalResultBox>> {
            let mut bytes_received: u64 = _bytes_received;

            let additional_time_elapsed: f32 = _additional_time_elapsed;

            let stream = self.stream.as_mut().ok_or(Box::new(error_gen!("no stream currently exists")))?;

            let mio_token = self.mio_token;
            let mut buf = vec![0_u8; self.cfg.length as usize];

            let peer_addr = stream.peer_addr()?;
            let start = Instant::now();

            while self.active.load(Relaxed) && start.elapsed() < INTERVAL {
                log::trace!("awaiting TCP stream {} from {}...", self.stream_idx, peer_addr);
                self.mio_poll.poll(&mut self.mio_events, Some(POLL_TIMEOUT))?;
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
                                    return Err(Box::new(e));
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
                let receive_result = TransmitState {
                    timestamp: get_unix_timestamp(),
                    stream_idx: Some(self.stream_idx),
                    duration: start.elapsed().as_secs_f32() + additional_time_elapsed,
                    bytes_received: Some(bytes_received),
                    family: Some("tcp".to_string()),
                    ..TransmitState::default()
                };
                let receive_result = Message::Receive(receive_result);
                Ok(Some(Box::new(TcpReceiveResult::try_from(&receive_result)?)))
            } else {
                log::debug!(
                    "no bytes received via TCP stream {} from {} in this interval",
                    self.stream_idx,
                    peer_addr
                );
                Ok(None)
            }
        }
    }

    impl TestRunner for TcpReceiver {
        fn run_interval(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            let mut bytes_received = 0;
            let mut additional_time_elapsed = 0.0;
            if self.stream.is_none() && self.sender.is_none() {
                // if still in the setup phase, receive the sender
                let (stream, bytes_received_in_validation, time_spent_in_validation) = self.process_connection()?;
                self.stream = Some(stream);
                // NOTE: the connection process consumes packets; account for those bytes
                bytes_received += bytes_received_in_validation;
                additional_time_elapsed += time_spent_in_validation;
            }
            let res = if self.cfg.reverse_nat.unwrap_or(false) {
                if self.sender.is_none() {
                    let stream = self.stream.take().ok_or(Box::new(error_gen!("no stream currently exists")))?;
                    let stream = super::mio_stream_to_std_stream(stream);
                    let sender = TcpSender::new_from_tcp_stream(&self.cfg, self.stream_idx, stream)?;
                    self.sender = Some(Box::new(sender));
                }
                let sender = self.sender.as_mut().ok_or(Box::new(error_gen!("no sender currently exists")))?;
                sender.process_stream_send()?
            } else {
                self.process_stream_receive(bytes_received, additional_time_elapsed)?
            };
            Ok(res)
        }

        fn get_port(&self) -> BoxResult<u16> {
            match (&self.listener, &self.stream, &self.sender) {
                (Some(listener), _, _) => Ok(listener.local_addr()?.port()),
                (_, Some(stream), _) => Ok(stream.local_addr()?.port()),
                (_, _, Some(sender)) => Ok(sender.get_port()?),
                _ => Err(Box::new(error_gen!("no port currently bound"))),
            }
        }

        fn get_idx(&self) -> usize {
            self.stream_idx
        }

        fn stop(&mut self) {
            self.active.store(false, Relaxed);
        }
    }
}

pub mod sender {
    use std::io::{Read, Write};
    use std::net::{IpAddr, SocketAddr, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use crate::{
        error_gen,
        protocol::{
            messaging::{Configuration, Message, TransmitState},
            results::{get_unix_timestamp, IntervalResultBox, TcpReceiveResult, TcpSendResult},
        },
        stream::{tcp::receiver::TcpReceiver, TestRunner, INTERVAL},
        BoxResult,
    };

    const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
    const WRITE_TIMEOUT: Duration = Duration::from_millis(50);
    const BUFFER_FULL_TIMEOUT: Duration = Duration::from_millis(1);
    const RECEIVE_TIMEOUT: Duration = Duration::from_secs(3);

    #[allow(dead_code)]
    pub struct TcpSender {
        active: AtomicBool,
        cfg: Configuration,
        stream_idx: usize,

        socket_addr: SocketAddr,
        stream: Option<TcpStream>,

        //the interval, in seconds, at which to send data
        send_interval: f32,

        remaining_duration: f32,
        staged_buffer: Vec<u8>,

        no_delay: bool,

        test_id_sent: bool,

        receiver: Option<Box<TcpReceiver>>,
    }
    impl TcpSender {
        pub fn new(cfg: &Configuration, stream_idx: usize, receiver_ip: IpAddr, receiver_port: u16) -> BoxResult<TcpSender> {
            Ok(TcpSender {
                active: AtomicBool::new(true),
                cfg: cfg.clone(),
                stream_idx,

                socket_addr: SocketAddr::new(receiver_ip, receiver_port),
                stream: None,

                send_interval: cfg.send_interval.unwrap_or(1.0),

                remaining_duration: cfg.duration.unwrap_or(0.0),
                staged_buffer: Self::build_staged_buffer(cfg.length as usize, cfg.test_id),

                no_delay: cfg.no_delay.unwrap_or(false),
                test_id_sent: false,

                receiver: None,
            })
        }

        pub(crate) fn new_from_tcp_stream(cfg: &Configuration, stream_idx: usize, stream: TcpStream) -> BoxResult<TcpSender> {
            Ok(TcpSender {
                active: AtomicBool::new(true),
                cfg: cfg.clone(),
                stream_idx,
                socket_addr: stream.peer_addr()?,
                stream: Some(stream),
                send_interval: cfg.send_interval.unwrap_or(1.0),
                remaining_duration: cfg.duration.unwrap_or(0.0),
                staged_buffer: Self::build_staged_buffer(cfg.length as usize, cfg.test_id),
                no_delay: cfg.no_delay.unwrap_or(false),
                test_id_sent: false,
                receiver: None,
            })
        }

        fn build_staged_buffer(length: usize, test_id: uuid::Uuid) -> Vec<u8> {
            let mut staged_buffer = vec![0_u8; length];
            for (i, staged_buffer_i) in staged_buffer.iter_mut().enumerate().skip(super::TEST_HEADER_SIZE) {
                //fill the packet with a fixed sequence
                *staged_buffer_i = (i % 256) as u8;
            }
            //embed the test ID
            staged_buffer[0..16].copy_from_slice(test_id.as_bytes());
            staged_buffer
        }

        fn process_connection(&mut self) -> BoxResult<TcpStream> {
            log::debug!("preparing to connect TCP stream {} to {} ...", self.stream_idx, self.socket_addr);

            let stream = match TcpStream::connect_timeout(&self.socket_addr, CONNECT_TIMEOUT) {
                Ok(s) => s,
                Err(e) => {
                    return Err(Box::new(error_gen!("unable to connect stream {}: {}", self.stream_idx, e)));
                }
            };

            stream.set_nonblocking(true)?;
            stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
            stream.set_read_timeout(Some(RECEIVE_TIMEOUT))?;

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

            let _send_buffer = self.cfg.send_buffer.unwrap_or(0) as usize;

            #[cfg(unix)]
            if _send_buffer != 0 {
                log::debug!("setting send-buffer to {}...", _send_buffer);
                let raw_socket = socket2::SockRef::from(&stream);
                raw_socket.set_send_buffer_size(_send_buffer)?;
            }
            Ok(stream)
        }

        pub(crate) fn process_stream_send(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            let stream = self.stream.as_mut().ok_or(Box::new(error_gen!("no stream currently exists")))?;

            let interval_duration = Duration::from_secs_f32(self.send_interval);
            let mut interval_iteration = 0;
            let bytes_to_send = ((self.cfg.bandwidth.unwrap() as f32) * INTERVAL.as_secs_f32()) as i64;
            let mut bytes_to_send_remaining = bytes_to_send;
            let bytes_to_send_per_interval_slice = ((bytes_to_send as f32) * self.send_interval) as i64;
            let mut bytes_to_send_per_interval_slice_remaining = bytes_to_send_per_interval_slice;

            let mut sends_blocked: u64 = 0;
            let mut bytes_sent: u64 = 0;

            let peer_addr = stream.peer_addr()?;
            let cycle_start = Instant::now();

            while self.active.load(Relaxed) && self.remaining_duration > 0.0 && bytes_to_send_remaining > 0 {
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
                        return Err(Box::new(e));
                    }
                };
                log::trace!("wrote {} bytes in TCP stream {} to {}", packet_size, self.stream_idx, peer_addr);

                let bytes_written = packet_size as i64;
                bytes_sent += bytes_written as u64;
                bytes_to_send_remaining -= bytes_written;
                bytes_to_send_per_interval_slice_remaining -= bytes_written;

                let elapsed_time = cycle_start.elapsed();
                if elapsed_time >= INTERVAL {
                    self.remaining_duration -= packet_start.elapsed().as_secs_f32();

                    log::debug!(
                        "{} bytes sent via TCP stream {} to {} in this interval; reporting...",
                        bytes_sent,
                        self.stream_idx,
                        peer_addr
                    );

                    let send_result = TransmitState {
                        timestamp: get_unix_timestamp(),
                        stream_idx: Some(self.stream_idx),
                        duration: elapsed_time.as_secs_f32(),
                        bytes_sent: Some(bytes_sent),
                        sends_blocked: Some(sends_blocked),
                        family: Some("tcp".to_string()),
                        ..TransmitState::default()
                    };
                    let send_result = Message::Send(send_result);
                    return Ok(Some(Box::new(TcpSendResult::try_from(&send_result)?)));
                }

                if bytes_to_send_remaining <= 0 {
                    //interval's target is exhausted, so sleep until the end
                    let elapsed_time = cycle_start.elapsed();
                    if INTERVAL > elapsed_time {
                        sleep(INTERVAL - elapsed_time);
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

                let send_result = TransmitState {
                    timestamp: get_unix_timestamp(),
                    stream_idx: Some(self.stream_idx),
                    duration: cycle_start.elapsed().as_secs_f32(),
                    bytes_sent: Some(bytes_sent),
                    sends_blocked: Some(sends_blocked),
                    family: Some("tcp".to_string()),
                    ..TransmitState::default()
                };
                let send_result = Message::Send(send_result);
                Ok(Some(Box::new(TcpSendResult::try_from(&send_result)?)))
            } else {
                log::debug!(
                    "no bytes sent via TCP stream {} to {} in this interval; shutting down...",
                    self.stream_idx,
                    peer_addr
                );
                //indicate that the test is over by dropping the stream
                self.stream = None;
                Ok(None)
            }
        }

        fn process_stream_receive(&mut self, _bytes_received: u64, _additional_time_elapsed: f32) -> BoxResult<Option<IntervalResultBox>> {
            let mut bytes_received: u64 = _bytes_received;

            let additional_time_elapsed: f32 = _additional_time_elapsed;

            let stream = self.stream.as_mut().unwrap();

            let mut buf = vec![0_u8; self.cfg.length as usize];

            let peer_addr = stream.peer_addr()?;
            let start = Instant::now();

            while self.active.load(Relaxed) && start.elapsed() < INTERVAL {
                let packet_size = match stream.read(&mut buf) {
                    Ok(packet_size) => packet_size,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                        // receive timeout
                        sleep(WRITE_TIMEOUT);
                        continue;
                    }
                    Err(e) => {
                        return Err(Box::new(e));
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
            if bytes_received > 0 {
                log::debug!(
                    "{} bytes received via TCP stream {} from {} in this interval; reporting...",
                    bytes_received,
                    self.stream_idx,
                    peer_addr
                );
                let receive_result = TransmitState {
                    timestamp: get_unix_timestamp(),
                    stream_idx: Some(self.stream_idx),
                    duration: start.elapsed().as_secs_f32() + additional_time_elapsed,
                    bytes_received: Some(bytes_received),
                    family: Some("tcp".to_string()),
                    ..TransmitState::default()
                };
                let receive_result = Message::Receive(receive_result);
                Ok(Some(Box::new(TcpReceiveResult::try_from(&receive_result).unwrap())))
            } else {
                log::debug!(
                    "no bytes received via TCP stream {} from {} in this interval",
                    self.stream_idx,
                    peer_addr
                );
                Ok(None)
            }
        }

        fn send_test_uuid(&mut self) -> BoxResult<()> {
            if !self.test_id_sent {
                let stream = self.stream.as_mut().ok_or(Box::new(error_gen!("no stream currently exists")))?;

                let peer_addr = stream.peer_addr()?;

                log::trace!(
                    "sending TCP stream {} [{}] with test ID {}...",
                    self.stream_idx,
                    peer_addr,
                    self.cfg.test_id,
                );

                let test_uuid = self.cfg.test_id.as_bytes();
                let len = test_uuid.len();
                let mut written = 0;
                loop {
                    // send the test ID
                    let packet_size = match stream.write(&test_uuid[written..]) {
                        Ok(packet_size) => packet_size,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                            // nothing to do, but avoid burning CPU cycles
                            sleep(WRITE_TIMEOUT);
                            continue;
                        }
                        Err(e) => {
                            return Err(Box::new(e));
                        }
                    };
                    written += packet_size;
                    if written >= len {
                        break;
                    }
                }
                self.test_id_sent = true;
            }
            Ok(())
        }
    }

    impl TestRunner for TcpSender {
        fn run_interval(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            if self.stream.is_none() {
                // if still in the setup phase, connect to the receiver
                self.stream = Some(self.process_connection()?);
            }
            let res = if self.cfg.reverse_nat.unwrap_or(false) {
                self.send_test_uuid()?;
                self.process_stream_receive(0, 0.0)?
            } else {
                self.process_stream_send()?
            };
            Ok(res)
        }

        fn get_port(&self) -> BoxResult<u16> {
            match &self.stream {
                Some(stream) => Ok(stream.local_addr()?.port()),
                None => Err(Box::new(error_gen!("no stream currently exists"))),
            }
        }

        fn get_idx(&self) -> usize {
            self.stream_idx
        }

        fn stop(&mut self) {
            self.active.store(false, Relaxed);
        }
    }
}
