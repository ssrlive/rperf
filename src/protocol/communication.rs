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

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use crate::BoxResult;

/// how long to wait for keepalive events
/// the communications channels typically exchange data every second, so 2s is reasonable to avoid excess noise
#[cfg(unix)]
pub const KEEPALIVE_DURATION: Duration = Duration::from_secs(3);

/// how long to block on polling operations
const POLL_TIMEOUT: Duration = Duration::from_millis(50);

/// how long to allow for send-operations to complete
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// sends JSON data over a client-server communications stream
pub fn send(stream: &mut TcpStream, message: &serde_json::Value) -> BoxResult<()> {
    stream.set_write_timeout(Some(POLL_TIMEOUT))?;

    let serialised_message = serde_json::to_vec(message)?;

    log::debug!(
        "sending message to {}, length {}, {:?}...",
        stream.peer_addr()?,
        serialised_message.len(),
        message,
    );
    let mut output_buffer = vec![0_u8; serialised_message.len() + 2];
    output_buffer[..2].copy_from_slice(&(serialised_message.len() as u16).to_be_bytes());
    output_buffer[2..].copy_from_slice(serialised_message.as_slice());

    let start = Instant::now();
    let mut total_bytes_written: usize = 0;

    while start.elapsed() < SEND_TIMEOUT {
        match stream.write(&output_buffer[total_bytes_written..]) {
            Ok(bytes_written) => {
                total_bytes_written += bytes_written;
                if total_bytes_written == output_buffer.len() {
                    return Ok(());
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                // unable to write at the moment; keep trying until the full timeout is reached
                continue;
            }
            Err(e) => {
                return Err(Box::new(e));
            }
        }
    }
    let err = simple_error::simple_error!("timed out while attempting to send status-message to {}", stream.peer_addr()?);
    Err(Box::new(err))
}

/// receives the length-count of a pending message over a client-server communications stream
fn receive_length(stream: &mut TcpStream, alive_check: fn() -> bool, handler: &mut dyn FnMut() -> BoxResult<()>) -> BoxResult<u16> {
    stream.set_read_timeout(Some(POLL_TIMEOUT)).expect("unable to set TCP read-timeout");

    let mut length_bytes_read = 0;
    let mut length_spec: [u8; 2] = [0; 2];
    while alive_check() {
        //waiting to find out how long the next message is
        handler()?; //send any outstanding results between cycles

        let size = match stream.read(&mut length_spec[length_bytes_read..]) {
            Ok(size) => size,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                // nothing available to process
                continue;
            }
            Err(e) => {
                return Err(Box::new(e));
            }
        };
        if size == 0 {
            if alive_check() {
                return Err(Box::new(simple_error::simple_error!("connection lost")));
            } else {
                // shutting down; a disconnect is expected
                return Err(Box::new(simple_error::simple_error!("local shutdown requested")));
            }
        }

        length_bytes_read += size;
        if length_bytes_read == 2 {
            let length = u16::from_be_bytes(length_spec);
            log::debug!("received length-spec of {} from {}", length, stream.peer_addr()?);
            return Ok(length);
        } else {
            log::debug!("received partial length-spec from {}", stream.peer_addr()?);
        }
    }
    Err(Box::new(simple_error::simple_error!("system shutting down")))
}

/// receives the data-value of a pending message over a client-server communications stream
fn receive_payload(
    stream: &mut TcpStream,
    alive_check: fn() -> bool,
    results_handler: &mut dyn FnMut() -> BoxResult<()>,
    length: u16,
) -> BoxResult<serde_json::Value> {
    stream.set_read_timeout(Some(POLL_TIMEOUT)).expect("unable to set TCP read-timeout");

    let mut bytes_read = 0;
    let mut buffer = vec![0_u8; length.into()];
    while alive_check() {
        //waiting to receive the payload
        results_handler()?; //send any outstanding results between cycles

        let size = match stream.read(&mut buffer[bytes_read..]) {
            Ok(size) => size,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                // nothing available to process
                continue;
            }
            Err(e) => {
                return Err(Box::new(e));
            }
        };
        if size == 0 {
            if alive_check() {
                return Err(Box::new(simple_error::simple_error!("connection lost")));
            } else {
                //shutting down; a disconnect is expected
                return Err(Box::new(simple_error::simple_error!("local shutdown requested")));
            }
        }

        bytes_read += size;
        if bytes_read == length as usize {
            match serde_json::from_slice(&buffer) {
                Ok(v) => {
                    log::debug!("received message from {}: {:?}", stream.peer_addr()?, v);
                    return Ok(v);
                }
                Err(e) => {
                    return Err(Box::new(e));
                }
            }
        } else {
            log::debug!("received partial payload from {}", stream.peer_addr()?);
        }
    }
    Err(Box::new(simple_error::simple_error!("system shutting down")))
}

/// handles the full process of retrieving a message from a client-server communications stream
pub fn receive(
    stream: &mut TcpStream,
    alive_check: fn() -> bool,
    results_handler: &mut dyn FnMut() -> BoxResult<()>,
) -> BoxResult<serde_json::Value> {
    log::debug!("awaiting length-value from {}...", stream.peer_addr()?);
    let length = receive_length(stream, alive_check, results_handler)?;
    log::debug!("awaiting payload from {}...", stream.peer_addr()?);
    receive_payload(stream, alive_check, results_handler, length)
}
