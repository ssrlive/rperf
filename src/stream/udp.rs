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

pub const TEST_HEADER_SIZE: u16 = 36;
const UDP_HEADER_SIZE: u16 = 8;

pub mod receiver {
    use crate::{
        error_gen,
        protocol::{
            messaging::{Configuration, Message, TransmitState},
            results::{get_unix_timestamp, IntervalResultBox, UdpReceiveResult},
        },
        stream::{parse_port_spec, udp::sender::UdpSender, TestRunner, INTERVAL},
        BoxResult,
    };
    use chrono::NaiveDateTime;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
    use std::sync::Mutex;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use std::{convert::TryInto, thread::sleep};

    const READ_TIMEOUT: Duration = Duration::from_millis(50);

    #[derive(Default)]
    pub struct UdpPortPool {
        pub ports_ip4: Vec<u16>,
        pos_ip4: usize,
        lock_ip4: Mutex<u8>,

        pub ports_ip6: Vec<u16>,
        pos_ip6: usize,
        lock_ip6: Mutex<u8>,
    }
    impl UdpPortPool {
        pub fn new(port_spec: &str, port_spec6: &str) -> UdpPortPool {
            let ports = parse_port_spec(port_spec);
            if !ports.is_empty() {
                log::debug!("configured IPv4 UDP port pool: {:?}", ports);
            } else {
                log::debug!("using OS assignment for IPv4 UDP ports");
            }

            let ports6 = parse_port_spec(port_spec6);
            if !ports.is_empty() {
                log::debug!("configured IPv6 UDP port pool: {:?}", ports6);
            } else {
                log::debug!("using OS assignment for IPv6 UDP ports");
            }

            UdpPortPool {
                ports_ip4: ports,
                ports_ip6: ports6,
                ..UdpPortPool::default()
            }
        }

        pub fn bind(&mut self, peer_ip: IpAddr) -> BoxResult<UdpSocket> {
            let ipv6addr_unspec = IpAddr::V6(Ipv6Addr::UNSPECIFIED);
            let ipv4addr_unspec = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
            match peer_ip {
                IpAddr::V6(_) => {
                    if self.ports_ip6.is_empty() {
                        return Ok(UdpSocket::bind(SocketAddr::new(ipv6addr_unspec, 0))?);
                    } else {
                        let _guard = self.lock_ip6.lock().unwrap();

                        for port_idx in (self.pos_ip6 + 1)..self.ports_ip6.len() {
                            //iterate to the end of the pool; this will skip the first element in the pool initially, but that's fine
                            let listener_result = UdpSocket::bind(SocketAddr::new(ipv6addr_unspec, self.ports_ip6[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip6 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv6 UDP port {}", self.ports_ip6[port_idx]);
                            }
                        }
                        for port_idx in 0..=self.pos_ip6 {
                            //circle back to where the search started
                            let listener_result = UdpSocket::bind(SocketAddr::new(ipv6addr_unspec, self.ports_ip6[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip6 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv6 UDP port {}", self.ports_ip6[port_idx]);
                            }
                        }
                    }
                    Err(Box::new(error_gen!("unable to allocate IPv6 UDP port")))
                }
                IpAddr::V4(_) => {
                    if self.ports_ip4.is_empty() {
                        return Ok(UdpSocket::bind(SocketAddr::new(ipv4addr_unspec, 0))?);
                    } else {
                        let _guard = self.lock_ip4.lock().unwrap();

                        for port_idx in (self.pos_ip4 + 1)..self.ports_ip4.len() {
                            //iterate to the end of the pool; this will skip the first element in the pool initially, but that's fine
                            let listener_result = UdpSocket::bind(SocketAddr::new(ipv4addr_unspec, self.ports_ip4[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip4 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv4 UDP port {}", self.ports_ip4[port_idx]);
                            }
                        }
                        for port_idx in 0..=self.pos_ip4 {
                            //circle back to where the search started
                            let listener_result = UdpSocket::bind(SocketAddr::new(ipv4addr_unspec, self.ports_ip4[port_idx]));
                            if let Ok(listener_result) = listener_result {
                                self.pos_ip4 = port_idx;
                                return Ok(listener_result);
                            } else {
                                log::warn!("unable to bind IPv4 UDP port {}", self.ports_ip4[port_idx]);
                            }
                        }
                    }
                    Err(Box::new(error_gen!("unable to allocate IPv4 UDP port")))
                }
            }
        }
    }

    #[derive(Default)]
    struct UdpReceiverIntervalHistory {
        packets_received: u64,

        packets_lost: i64,
        packets_out_of_order: u64,
        packets_duplicated: u64,

        unbroken_sequence: u64,
        jitter_seconds: Option<f32>,
        longest_unbroken_sequence: u64,
        longest_jitter_seconds: Option<f32>,
        previous_time_delta_nanoseconds: i64,
    }

    #[derive(Default)]
    pub struct UdpReceiver {
        active: bool,
        cfg: Configuration,
        stream_idx: usize,
        next_packet_id: u64,
        socket: Option<UdpSocket>,
        sender: Option<Box<UdpSender>>,
    }
    impl UdpReceiver {
        pub fn new(cfg: &Configuration, stream_idx: usize, port_pool: &mut UdpPortPool, peer_ip: IpAddr) -> BoxResult<UdpReceiver> {
            log::debug!("binding UDP receive socket for stream {}...", stream_idx);
            let socket: UdpSocket = port_pool.bind(peer_ip)?;

            let _receive_buffer = cfg.receive_buffer.unwrap_or(0) as usize;

            // NOTE: features unsupported on Windows
            #[cfg(unix)]
            if _receive_buffer != 0 {
                log::debug!("setting receive-buffer to {}...", _receive_buffer);
                let raw_socket = socket2::SockRef::from(&socket);
                raw_socket.set_recv_buffer_size(_receive_buffer)?;
            }
            log::debug!("bound UDP receive socket for stream {}: {}", stream_idx, socket.local_addr()?);

            Self::new_from_udp_socket(cfg, stream_idx, socket)
        }

        pub(crate) fn new_from_udp_socket(cfg: &Configuration, stream_idx: usize, socket: UdpSocket) -> BoxResult<UdpReceiver> {
            socket.set_read_timeout(Some(READ_TIMEOUT))?;
            Ok(UdpReceiver {
                active: true,
                cfg: cfg.clone(),
                stream_idx,
                socket: Some(socket),
                ..UdpReceiver::default()
            })
        }

        fn process_packets_ordering(&mut self, packet_id: u64, history: &mut UdpReceiverIntervalHistory) -> bool {
            /* the algorithm from iperf3 provides a pretty decent approximation
             * for tracking lost and out-of-order packets efficiently, so it's
             * been minimally reimplemented here, with corrections.
             *
             * returns true if packet-ordering is as-expected
             */
            match packet_id.cmp(&self.next_packet_id) {
                std::cmp::Ordering::Equal => {
                    //expected sequential-ordering case
                    self.next_packet_id += 1;
                    return true;
                }
                std::cmp::Ordering::Greater => {
                    //something was either lost or there's an ordering problem
                    let lost_packet_count = (packet_id - self.next_packet_id) as i64;
                    log::debug!(
                        "UDP reception for stream {} observed a gap of {} packets",
                        self.stream_idx,
                        lost_packet_count
                    );
                    history.packets_lost += lost_packet_count; //assume everything in-between has been lost
                    self.next_packet_id = packet_id + 1; //anticipate that ordered receipt will resume
                }
                std::cmp::Ordering::Less => {
                    //a packet with a previous ID was received; this is either a duplicate or an ordering issue
                    //CAUTION: this is where the approximation part of the algorithm comes into play
                    if history.packets_lost > 0 {
                        //assume it's an ordering issue in the common case
                        history.packets_lost -= 1;
                        history.packets_out_of_order += 1;
                    } else {
                        //the only other thing it could be is a duplicate; in practice, duplicates don't tend to show up alongside losses; non-zero is always bad, though
                        history.packets_duplicated += 1;
                    }
                }
            }
            false
        }

        fn process_jitter(&mut self, timestamp: &NaiveDateTime, history: &mut UdpReceiverIntervalHistory) -> BoxResult<()> {
            /* this is a pretty straightforward implementation of RFC 1889, Appendix 8
             * it works on an assumption that the timestamp delta between sender and receiver
             * will remain effectively constant during the testing window
             */
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
            let err = Box::new(error_gen!("unable to convert current timestamp to NaiveDateTime"));
            let current_timestamp = NaiveDateTime::from_timestamp_opt(now.as_secs() as i64, now.subsec_nanos()).ok_or(err)?;

            let time_delta = current_timestamp - *timestamp;

            let time_delta_nanoseconds = match time_delta.num_nanoseconds() {
                Some(ns) => ns,
                None => {
                    log::warn!("sender and receiver clocks are too out-of-sync to calculate jitter");
                    return Ok(());
                }
            };

            if history.unbroken_sequence > 1 {
                //do jitter calculation
                let delta_seconds = (time_delta_nanoseconds - history.previous_time_delta_nanoseconds).abs() as f32 / 1_000_000_000.00;

                if history.unbroken_sequence > 2 {
                    //normal jitter-calculation, per the RFC
                    //since we have a chain, this won't be None
                    let mut jitter_seconds = history.jitter_seconds.ok_or(Box::new(error_gen!("unable to calculate jitter")))?;
                    jitter_seconds += (delta_seconds - jitter_seconds) / 16.0;
                    history.jitter_seconds = Some(jitter_seconds);
                } else {
                    //first observed transition; use this as the calibration baseline
                    history.jitter_seconds = Some(delta_seconds);
                }
            }
            //update time-delta
            history.previous_time_delta_nanoseconds = time_delta_nanoseconds;
            Ok(())
        }

        fn process_packet(&mut self, packet: &[u8], history: &mut UdpReceiverIntervalHistory) -> BoxResult<bool> {
            //the first sixteen bytes are the test's ID
            if uuid::Uuid::from_bytes((&packet[..16]).try_into()?) != self.cfg.test_id {
                return Ok(false);
            }

            //the next eight bytes are the packet's ID, in big-endian order
            let packet_id = u64::from_be_bytes(packet[16..24].try_into()?);

            //except for the timestamp, nothing else in the packet actually matters

            history.packets_received += 1;
            if self.process_packets_ordering(packet_id, history) {
                //the second eight are the number of seconds since the UNIX epoch, in big-endian order
                let origin_seconds = i64::from_be_bytes(packet[24..32].try_into()?);
                //and the following four are the number of nanoseconds since the UNIX epoch
                let origin_nanoseconds = u32::from_be_bytes(packet[32..36].try_into()?);
                let err = Box::new(error_gen!("unable to convert packet timestamp to NaiveDateTime"));
                let source_timestamp = NaiveDateTime::from_timestamp_opt(origin_seconds, origin_nanoseconds).ok_or(err)?;

                history.unbroken_sequence += 1;
                self.process_jitter(&source_timestamp, history)?;

                if history.unbroken_sequence > history.longest_unbroken_sequence {
                    history.longest_unbroken_sequence = history.unbroken_sequence;
                    history.longest_jitter_seconds = history.jitter_seconds;
                }
            } else {
                history.unbroken_sequence = 0;
                history.jitter_seconds = None;
            }
            Ok(true)
        }

        pub(crate) fn interval_process_packet_receive(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            let mut buf = vec![0_u8; self.cfg.length as usize];

            let mut bytes_received: u64 = 0;

            let mut history = UdpReceiverIntervalHistory::default();

            let start = Instant::now();

            while self.active && start.elapsed() < INTERVAL {
                let (packet_size, peer_addr) = match self.socket.as_ref().unwrap().recv_from(&mut buf) {
                    Ok((packet_size, peer_addr)) => (packet_size, peer_addr),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                        // receive timeout
                        sleep(READ_TIMEOUT);
                        continue;
                    }
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };

                log::trace!(
                    "received {} bytes in UDP packet {} from {}",
                    packet_size,
                    self.stream_idx,
                    peer_addr
                );
                if packet_size == 16 {
                    // possible end-of-test message
                    if uuid::Uuid::from_bytes((&buf[..16]).try_into()?) == self.cfg.test_id {
                        // test's over
                        self.stop();
                        break;
                    }
                }
                if packet_size < super::TEST_HEADER_SIZE as usize {
                    log::warn!(
                        "received malformed packet with size {} for UDP stream {} from {}",
                        packet_size,
                        self.stream_idx,
                        peer_addr
                    );
                    continue;
                }

                if self.process_packet(&buf[..packet_size], &mut history)? {
                    // NOTE: duplicate packets increase this count; this is intentional because the stack still processed data
                    bytes_received += packet_size as u64 + super::UDP_HEADER_SIZE as u64;
                } else {
                    log::warn!("received packet unrelated to UDP stream {} from {}", self.stream_idx, peer_addr);
                    continue;
                }
            }
            if bytes_received > 0 {
                log::debug!(
                    "{} bytes received via UDP stream {} in this interval; reporting...",
                    bytes_received,
                    self.stream_idx
                );

                let receive_result = TransmitState {
                    family: Some("udp".to_string()),
                    timestamp: get_unix_timestamp(),
                    stream_idx: Some(self.stream_idx),
                    duration: start.elapsed().as_secs_f32(),
                    bytes_received: Some(bytes_received),
                    packets_received: Some(history.packets_received),
                    packets_lost: Some(history.packets_lost as u64),
                    packets_out_of_order: Some(history.packets_out_of_order),
                    packets_duplicated: Some(history.packets_duplicated),
                    unbroken_sequence: Some(history.longest_unbroken_sequence),
                    jitter_seconds: history.longest_jitter_seconds,
                    ..TransmitState::default()
                };
                let receive_result = Message::Receive(receive_result);
                Ok(Some(Box::new(UdpReceiveResult::try_from(&receive_result)?)))
            } else {
                log::debug!("no bytes received via UDP stream {} in this interval", self.stream_idx);
                Ok(None)
            }
        }

        fn get_incoming_socket_addr(&mut self) -> BoxResult<SocketAddr> {
            let mut buf = vec![0_u8; self.cfg.length as usize];
            let start = Instant::now();
            while self.active && start.elapsed() < INTERVAL {
                let (packet_size, peer_addr) = match self.socket.as_ref().unwrap().recv_from(&mut buf) {
                    Ok((packet_size, peer_addr)) => (packet_size, peer_addr),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                        // receive timeout
                        sleep(READ_TIMEOUT);
                        continue;
                    }
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };
                if packet_size > 16 && uuid::Uuid::from_bytes((&buf[..16]).try_into()?) == self.cfg.test_id {
                    return Ok(peer_addr);
                }
            }
            let idx = self.stream_idx;
            Err(Box::new(error_gen!("unable to retrieve incoming socket of stream {}", idx)))
        }

        fn interval_process_packet_send(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            if self.sender.is_none() {
                let incoming_addr = self.get_incoming_socket_addr()?;
                // remove the ownership of the socket from the receiver and give it to the sender
                let socket = self.socket.take().ok_or(Box::new(error_gen!("unable to get socket")))?;
                let sender = UdpSender::new_from_udp_socket(&self.cfg, self.stream_idx, socket, Some(incoming_addr))?;
                self.sender = Some(Box::new(sender));
            }
            let sender = self.sender.as_mut().unwrap();
            let res = sender.interval_process_packet_send()?;
            Ok(res)
        }
    }
    impl TestRunner for UdpReceiver {
        fn run_interval(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            if self.cfg.reverse_nat.unwrap_or(false) {
                self.interval_process_packet_send()
            } else {
                self.interval_process_packet_receive()
            }
        }

        fn get_port(&self) -> BoxResult<u16> {
            match (&self.socket, &self.sender) {
                (Some(s), _) => Ok(s.local_addr()?.port()),
                (_, Some(r)) => r.get_port(),
                _ => Err(Box::new(error_gen!("unable to get port"))),
            }
        }

        fn get_idx(&self) -> usize {
            self.stream_idx
        }

        fn stop(&mut self) {
            self.active = false;
            if let Some(s) = self.sender.as_mut() {
                s.stop();
            }
        }
    }
}

pub mod sender {
    use crate::{
        error_gen,
        protocol::{
            messaging::{Configuration, Message, TransmitState},
            results::{get_unix_timestamp, IntervalResultBox, UdpSendResult},
        },
        stream::{udp::receiver::UdpReceiver, TestRunner, INTERVAL},
        BoxResult,
    };
    use std::net::UdpSocket;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::thread::sleep;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const WRITE_TIMEOUT: Duration = Duration::from_millis(50);
    const BUFFER_FULL_TIMEOUT: Duration = Duration::from_millis(1);

    #[derive(Default)]
    pub struct UdpSender {
        active: bool,
        cfg: Configuration,
        stream_idx: usize,

        socket: Option<UdpSocket>,
        target: Option<SocketAddr>,

        //the interval, in seconds, at which to send data
        send_interval: f32,

        remaining_duration: f32,
        next_packet_id: u64,
        staged_packet: Vec<u8>,

        receiver: Option<Box<UdpReceiver>>,
    }
    impl UdpSender {
        pub fn new(cfg: &Configuration, stream_idx: usize, port: u16, receiver_ip: IpAddr, receiver_port: u16) -> BoxResult<UdpSender> {
            log::debug!("preparing to connect UDP stream {}...", stream_idx);
            let socket_addr_receiver = SocketAddr::new(receiver_ip, receiver_port);
            let socket = match receiver_ip {
                IpAddr::V6(_) => UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port))?,
                IpAddr::V4(_) => UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))?,
            };

            let _send_buffer: usize = cfg.send_buffer.unwrap_or(0) as usize;

            // NOTE: features unsupported on Windows
            #[cfg(unix)]
            if _send_buffer != 0 {
                log::debug!("setting send-buffer to {}...", _send_buffer);
                let raw_socket = socket2::SockRef::from(&socket);
                raw_socket.set_send_buffer_size(_send_buffer)?;
            }
            log::debug!("connected UDP stream {} to {}", stream_idx, socket_addr_receiver);

            Self::new_from_udp_socket(cfg, stream_idx, socket, Some(socket_addr_receiver))
        }

        pub fn new_from_udp_socket(cfg: &Configuration, idx: usize, socket: UdpSocket, target: Option<SocketAddr>) -> BoxResult<UdpSender> {
            socket.set_write_timeout(Some(WRITE_TIMEOUT))?;
            let mut staged_packet = vec![0_u8; cfg.length as usize];
            for i in super::TEST_HEADER_SIZE..(staged_packet.len() as u16) {
                //fill the packet with a fixed sequence
                staged_packet[i as usize] = (i % 256) as u8;
            }
            //embed the test ID
            staged_packet[0..16].copy_from_slice(cfg.test_id.as_bytes());

            Ok(UdpSender {
                active: true,
                cfg: cfg.clone(),
                stream_idx: idx,

                socket: Some(socket),
                target,

                send_interval: cfg.send_interval.unwrap_or(0.0),

                remaining_duration: cfg.duration.unwrap_or(0.0),
                staged_packet,
                ..UdpSender::default()
            })
        }

        fn prepare_packet(&mut self) -> BoxResult<()> {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

            //eight bytes after the test ID are the packet's ID, in big-endian order
            self.staged_packet[16..24].copy_from_slice(&self.next_packet_id.to_be_bytes());

            //the next eight are the seconds part of the UNIX timestamp and the following four are the nanoseconds
            self.staged_packet[24..32].copy_from_slice(&now.as_secs().to_be_bytes());
            self.staged_packet[32..36].copy_from_slice(&now.subsec_nanos().to_be_bytes());
            Ok(())
        }

        pub(crate) fn interval_process_packet_send(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            let interval_duration = Duration::from_secs_f32(self.send_interval);
            let mut interval_iteration = 0;
            let bytes_to_send = ((self.cfg.bandwidth.unwrap() as f32) * INTERVAL.as_secs_f32()) as i64;
            let mut bytes_to_send_remaining = bytes_to_send;
            let bytes_to_send_per_interval_slice = ((bytes_to_send as f32) * self.send_interval) as i64;
            let mut bytes_to_send_per_interval_slice_remaining = bytes_to_send_per_interval_slice;

            let mut packets_sent: u64 = 0;
            let mut sends_blocked: u64 = 0;
            let mut bytes_sent: u64 = 0;

            let cycle_start = Instant::now();

            while self.active && self.remaining_duration > 0.0 && bytes_to_send_remaining > 0 {
                log::trace!("writing {} bytes in UDP stream {}...", self.staged_packet.len(), self.stream_idx);
                let packet_start = Instant::now();

                let addr = self.target.ok_or(Box::new(error_gen!("unable to get target")))?;
                self.prepare_packet()?;
                let packet_size = match self.socket.as_mut().unwrap().send_to(&self.staged_packet, addr) {
                    Ok(packet_size) => packet_size,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        //send-buffer is full
                        //nothing to do, but avoid burning CPU cycles
                        sleep(BUFFER_FULL_TIMEOUT);
                        sends_blocked += 1;
                        continue;
                    }
                    Err(e) => {
                        log::error!("unable to send UDP packet: {}", e);
                        return Err(Box::new(e));
                    }
                };
                log::trace!("wrote {} bytes in UDP stream {}", packet_size, self.stream_idx);

                packets_sent += 1;
                //reflect that a packet is in-flight
                self.next_packet_id += 1;

                let bytes_written = packet_size as i64 + super::UDP_HEADER_SIZE as i64;
                bytes_sent += bytes_written as u64;
                bytes_to_send_remaining -= bytes_written;
                bytes_to_send_per_interval_slice_remaining -= bytes_written;

                let elapsed_time = cycle_start.elapsed();
                if elapsed_time >= INTERVAL {
                    self.remaining_duration -= packet_start.elapsed().as_secs_f32();
                    log::debug!(
                        "{} bytes sent via UDP stream {} in this interval; reporting...",
                        bytes_sent,
                        self.stream_idx
                    );
                    let send_result = TransmitState {
                        family: Some("udp".to_string()),
                        timestamp: get_unix_timestamp(),
                        stream_idx: Some(self.stream_idx),
                        duration: elapsed_time.as_secs_f32(),
                        bytes_sent: Some(bytes_sent),
                        packets_sent: Some(packets_sent),
                        sends_blocked: Some(sends_blocked),
                        ..TransmitState::default()
                    };
                    let send_result = Message::Send(send_result);
                    return Ok(Some(Box::new(UdpSendResult::try_from(send_result)?)));
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
                    "{} bytes sent via UDP stream {} in this interval; reporting...",
                    bytes_sent,
                    self.stream_idx
                );
                let send_result = TransmitState {
                    family: Some("udp".to_string()),
                    timestamp: get_unix_timestamp(),
                    stream_idx: Some(self.stream_idx),
                    duration: cycle_start.elapsed().as_secs_f32(),
                    bytes_sent: Some(bytes_sent),
                    packets_sent: Some(packets_sent),
                    sends_blocked: Some(sends_blocked),
                    ..TransmitState::default()
                };
                let send_result = Message::Send(send_result);
                Ok(Some(Box::new(UdpSendResult::try_from(&send_result)?)))
            } else {
                log::debug!(
                    "no bytes sent via UDP stream {} in this interval; shutting down...",
                    self.stream_idx
                );
                let addr = self.target.ok_or(Box::new(error_gen!("unable to get target")))?;
                //indicate that the test is over by sending the test ID by itself
                let mut remaining_announcements = 5;
                while remaining_announcements > 0 {
                    //do it a few times in case of loss
                    match self.socket.as_mut().unwrap().send_to(&self.staged_packet[0..16], addr) {
                        Ok(packet_size) => {
                            log::trace!("wrote {} bytes of test-end signal in UDP stream {}", packet_size, self.stream_idx);
                            remaining_announcements -= 1;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            //send-buffer is full
                            //wait to try again and avoid burning CPU cycles
                            sleep(BUFFER_FULL_TIMEOUT);
                        }
                        Err(e) => {
                            return Err(Box::new(e));
                        }
                    }
                }
                Ok(None)
            }
        }

        fn send_initial_packet(&mut self) -> BoxResult<()> {
            let mut written = 0;
            let start = Instant::now();
            let addr = self.target.ok_or(Box::new(error_gen!("unable to get target")))?;
            while self.active && start.elapsed() < INTERVAL {
                let err = Box::new(error_gen!("could not get socket"));
                let packet_size = match self.socket.as_ref().ok_or(err)?.send_to(&self.staged_packet[written..], addr) {
                    Ok(packet_size) => packet_size,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::Interrupted => {
                        sleep(BUFFER_FULL_TIMEOUT);
                        continue;
                    }
                    Err(e) => {
                        return Err(Box::new(e));
                    }
                };
                written += packet_size;
                if written > 16 {
                    return Ok(());
                }
            }
            Err(Box::new(error_gen!("unable to send initial packet")))
        }

        fn interval_process_packet_receive(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            if self.receiver.is_none() {
                self.send_initial_packet()?;
                // remove the ownership of the socket from the sender and give it to the receiver
                let socket = self.socket.take().ok_or(Box::new(error_gen!("unable to get socket")))?;
                let receiver = UdpReceiver::new_from_udp_socket(&self.cfg, self.stream_idx, socket)?;
                self.receiver = Some(Box::new(receiver));
            }
            let receiver = self.receiver.as_mut().ok_or(Box::new(error_gen!("unable to get receiver")))?;
            let res = receiver.interval_process_packet_receive()?;
            Ok(res)
        }
    }
    impl TestRunner for UdpSender {
        fn run_interval(&mut self) -> BoxResult<Option<IntervalResultBox>> {
            if self.cfg.reverse_nat.unwrap_or(false) {
                self.interval_process_packet_receive()
            } else {
                self.interval_process_packet_send()
            }
        }

        fn get_port(&self) -> BoxResult<u16> {
            match (&self.socket, &self.receiver) {
                (Some(s), _) => Ok(s.local_addr()?.port()),
                (_, Some(r)) => r.get_port(),
                _ => Err(Box::new(error_gen!("unable to get port"))),
            }
        }

        fn get_idx(&self) -> usize {
            self.stream_idx
        }

        fn stop(&mut self) {
            self.active = false;
            if let Some(r) = self.receiver.as_mut() {
                r.stop();
            }
        }
    }
}
