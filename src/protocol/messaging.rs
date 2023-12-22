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

use crate::BoxResult;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct Configuration {
    pub family: Option<String>,
    pub length: u32,
    pub receive_buffer: Option<u32>,
    pub role: String,
    pub streams: u8,
    pub test_id: Vec<u8>,
    pub bandwidth: Option<u64>,
    pub duration: Option<f32>,
    pub send_interval: Option<f32>,
    pub send_buffer: Option<u32>,
    pub no_delay: Option<bool>,
    pub stream_ports: Option<Vec<u16>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct OperationResult {
    pub family: Option<String>,
    pub timestamp: f64,
    pub stream_idx: Option<u8>,
    pub duration: f32,
    pub bytes_sent: Option<u64>,
    pub packets_sent: Option<u64>,
    pub sends_blocked: Option<u64>,
    pub bytes_received: Option<u64>,
    pub packets_received: Option<u64>,
    pub packets_lost: Option<u64>,
    pub packets_out_of_order: Option<u64>,
    pub packets_duplicated: Option<u64>,
    pub jitter_seconds: Option<f32>,
    pub unbroken_sequence: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "kind")]
pub enum Message {
    #[serde(rename(deserialize = "configuration", serialize = "configuration"))]
    Configuration(Configuration),
    #[serde(rename(deserialize = "connect", serialize = "connect"))]
    Connect { stream_ports: Vec<u16> },
    #[serde(rename(deserialize = "connect-ready", serialize = "connect-ready"))]
    ConnectReady,
    #[serde(rename(deserialize = "begin", serialize = "begin"))]
    Begin,
    #[serde(rename(deserialize = "send", serialize = "send"))]
    Send(OperationResult),
    #[serde(rename(deserialize = "receive", serialize = "receive"))]
    Receive(OperationResult),
    #[serde(rename(deserialize = "done", serialize = "done"))]
    Done { origin: String, stream_idx: usize },
    #[serde(rename(deserialize = "end", serialize = "end"))]
    End,
}

/// prepares a message used to tell the server to begin operations
pub fn prepare_begin() -> serde_json::Value {
    serde_json::to_value(Message::Begin).unwrap()
}

/// prepares a message used to tell the client to connect its test-streams
pub fn prepare_connect(stream_ports: &[u16]) -> serde_json::Value {
    let msg = Message::Connect {
        stream_ports: stream_ports.to_vec(),
    };
    serde_json::to_value(msg).unwrap()
}

/// prepares a message used to tell the client that the server is ready to connect to its test-streams
pub fn prepare_connect_ready() -> serde_json::Value {
    serde_json::to_value(Message::ConnectReady).unwrap()
}

/// prepares a message used to tell the server that testing is finished
pub fn prepare_end() -> serde_json::Value {
    serde_json::to_value(Message::End).unwrap()
}

/// prepares a message used to describe the upload role of a TCP test
#[allow(clippy::too_many_arguments)]
fn prepare_configuration_tcp_upload(
    test_id: &[u8; 16],
    streams: u8,
    bandwidth: u64,
    seconds: f32,
    length: usize,
    send_interval: f32,
    send_buffer: u32,
    no_delay: bool,
) -> Configuration {
    Configuration {
        family: Some("tcp".to_string()),
        role: "upload".to_string(),
        test_id: test_id.to_vec(),
        streams: validate_streams(streams),
        bandwidth: Some(validate_bandwidth(bandwidth)),
        duration: Some(seconds),
        length: calculate_length_tcp(length) as u32,
        send_interval: Some(validate_send_interval(send_interval)),
        send_buffer: Some(send_buffer),
        no_delay: Some(no_delay),
        receive_buffer: Some(0),
        stream_ports: None,
    }
}

/// prepares a message used to describe the download role of a TCP test
fn prepare_configuration_tcp_download(test_id: &[u8; 16], streams: u8, length: usize, receive_buffer: u32) -> Configuration {
    Configuration {
        family: Some("tcp".to_string()),
        role: "download".to_string(),
        test_id: test_id.to_vec(),
        streams: validate_streams(streams),
        bandwidth: None,
        duration: None,
        length: calculate_length_tcp(length) as u32,
        send_interval: None,
        send_buffer: None,
        no_delay: None,
        receive_buffer: Some(receive_buffer),
        stream_ports: None,
    }
}

/// prepares a message used to describe the upload role of a UDP test
fn prepare_configuration_udp_upload(
    test_id: &[u8; 16],
    streams: u8,
    bandwidth: u64,
    seconds: f32,
    length: u16,
    send_interval: f32,
    send_buffer: u32,
) -> Configuration {
    Configuration {
        family: Some("udp".to_string()),
        role: "upload".to_string(),
        test_id: test_id.to_vec(),
        streams: validate_streams(streams),
        bandwidth: Some(validate_bandwidth(bandwidth)),
        duration: Some(seconds),
        length: calculate_length_udp(length) as u32,
        send_interval: Some(validate_send_interval(send_interval)),
        send_buffer: Some(send_buffer),
        no_delay: None,
        receive_buffer: None,
        stream_ports: None,
    }
}

/// prepares a message used to describe the download role of a UDP test
fn prepare_configuration_udp_download(test_id: &[u8; 16], streams: u8, length: u16, receive_buffer: u32) -> Configuration {
    Configuration {
        family: Some("udp".to_string()),
        role: "download".to_string(),
        test_id: test_id.to_vec(),
        streams: validate_streams(streams),
        bandwidth: None,
        duration: None,
        length: calculate_length_udp(length) as u32,
        send_interval: None,
        send_buffer: None,
        no_delay: None,
        receive_buffer: Some(receive_buffer),
        stream_ports: None,
    }
}

fn validate_streams(streams: u8) -> u8 {
    if streams > 0 {
        streams
    } else {
        log::warn!("parallel streams not specified; defaulting to 1");
        1
    }
}

fn validate_bandwidth(bandwidth: u64) -> u64 {
    if bandwidth > 0 {
        bandwidth
    } else {
        log::warn!("bandwidth was not specified; defaulting to 1024 bytes/second");
        1024
    }
}

fn validate_send_interval(send_interval: f32) -> f32 {
    if send_interval > 0.0 && send_interval <= 1.0 {
        send_interval
    } else {
        log::warn!("send-interval was invalid or not specified; defaulting to once per second");
        1.0
    }
}

fn calculate_length_tcp(length: usize) -> usize {
    if length < crate::stream::tcp::TEST_HEADER_SIZE {
        //length must be at least enough to hold the test data
        crate::stream::tcp::TEST_HEADER_SIZE
    } else {
        length
    }
}
fn calculate_length_udp(length: u16) -> u16 {
    if length < crate::stream::udp::TEST_HEADER_SIZE {
        //length must be at least enough to hold the test data
        crate::stream::udp::TEST_HEADER_SIZE
    } else {
        length
    }
}

/// prepares a message used to describe the upload role in a test
pub fn prepare_upload_configuration(args: &crate::args::Args, test_id: &[u8; 16]) -> BoxResult<Configuration> {
    let parallel_streams: u8 = args.parallel as u8;
    let mut seconds: f32 = args.time as f32;
    let mut send_interval: f32 = args.send_interval as f32;
    let mut length: u32 = args.length as u32;

    let mut send_buffer: u32 = args.send_buffer as u32;

    let mut bandwidth_string = args.bandwidth.as_str();
    let bandwidth: u64;
    let bandwidth_multiplier: f64;
    match bandwidth_string.chars().last() {
        Some(v) => {
            match v {
                'k' => {
                    //kilobits
                    bandwidth_multiplier = 1000.0 / 8.0;
                }
                'K' => {
                    //kilobytes
                    bandwidth_multiplier = 1000.0;
                }
                'm' => {
                    //megabits
                    bandwidth_multiplier = 1000.0 * 1000.0 / 8.0;
                }
                'M' => {
                    //megabytes
                    bandwidth_multiplier = 1000.0 * 1000.0;
                }
                'g' => {
                    //gigabits
                    bandwidth_multiplier = 1000.0 * 1000.0 * 1000.0 / 8.0;
                }
                'G' => {
                    //gigabytes
                    bandwidth_multiplier = 1000.0 * 1000.0 * 1000.0;
                }
                _ => {
                    bandwidth_multiplier = 1.0;
                }
            }

            if bandwidth_multiplier != 1.0 {
                //the value uses a suffix
                bandwidth_string = &bandwidth_string[0..(bandwidth_string.len() - 1)];
            }

            match bandwidth_string.parse::<f64>() {
                Ok(v2) => {
                    bandwidth = (v2 * bandwidth_multiplier) as u64;
                }
                Err(_) => {
                    //invalid input; fall back to 1mbps
                    log::warn!("invalid bandwidth: {}; setting value to 1mbps", args.bandwidth);
                    bandwidth = 125000;
                }
            }
        }
        None => {
            //invalid input; fall back to 1mbps
            log::warn!("invalid bandwidth: {}; setting value to 1mbps", args.bandwidth);
            bandwidth = 125000;
        }
    }

    if seconds <= 0.0 {
        log::warn!("time was not in an acceptable range and has been set to 0.0");
        seconds = 0.0
    }

    if send_interval > 1.0 || send_interval <= 0.0 {
        log::warn!("send-interval was not in an acceptable range and has been set to 0.05");
        send_interval = 0.05
    }

    if args.udp {
        log::debug!("preparing UDP upload config...");
        if length == 0 {
            length = 1024;
        }
        if send_buffer != 0 && send_buffer < length {
            log::warn!(
                "requested send-buffer, {}, is too small to hold the data to be exchanged; it will be increased to {}",
                send_buffer,
                length * 2
            );
            send_buffer = length * 2;
        }
        Ok(prepare_configuration_udp_upload(
            test_id,
            parallel_streams,
            bandwidth,
            seconds,
            length as u16,
            send_interval,
            send_buffer,
        ))
    } else {
        log::debug!("preparing TCP upload config...");
        if length == 0 {
            length = 32 * 1024;
        }
        if send_buffer != 0 && send_buffer < length {
            log::warn!(
                "requested send-buffer, {}, is too small to hold the data to be exchanged; it will be increased to {}",
                send_buffer,
                length * 2
            );
            send_buffer = length * 2;
        }

        let no_delay: bool = args.no_delay;

        Ok(prepare_configuration_tcp_upload(
            test_id,
            parallel_streams,
            bandwidth,
            seconds,
            length as usize,
            send_interval,
            send_buffer,
            no_delay,
        ))
    }
}
/// prepares a message used to describe the download role in a test
pub fn prepare_download_configuration(args: &crate::args::Args, test_id: &[u8; 16]) -> BoxResult<Configuration> {
    let parallel_streams: u8 = args.parallel as u8;
    let mut length: u32 = args.length as u32;
    let mut receive_buffer: u32 = args.receive_buffer as u32;

    if args.udp {
        log::debug!("preparing UDP download config...");
        if length == 0 {
            length = 1024;
        }
        if receive_buffer != 0 && receive_buffer < length {
            log::warn!(
                "requested receive-buffer, {}, is too small to hold the data to be exchanged; it will be increased to {}",
                receive_buffer,
                length * 2
            );
            receive_buffer = length * 2;
        }
        Ok(prepare_configuration_udp_download(
            test_id,
            parallel_streams,
            length as u16,
            receive_buffer,
        ))
    } else {
        log::debug!("preparing TCP download config...");
        if length == 0 {
            length = 32 * 1024;
        }
        if receive_buffer != 0 && receive_buffer < length {
            log::warn!(
                "requested receive-buffer, {}, is too small to hold the data to be exchanged; it will be increased to {}",
                receive_buffer,
                length * 2
            );
            receive_buffer = length * 2;
        }
        Ok(prepare_configuration_tcp_download(
            test_id,
            parallel_streams,
            length as usize,
            receive_buffer,
        ))
    }
}

#[test]
fn test_prepare_configuration_tcp_upload() {
    let test_id: [u8; 16] = [0; 16];
    let streams: u8 = 1;
    let bandwidth: u64 = 1024;
    let seconds: f32 = 1.0;
    let length: usize = 1024;
    let send_interval: f32 = 1.0;
    let send_buffer: u32 = 1024;
    let no_delay: bool = false;

    let cfg = prepare_configuration_tcp_upload(&test_id, streams, bandwidth, seconds, length, send_interval, send_buffer, no_delay);
    let msg = serde_json::to_value(Message::Configuration(cfg)).unwrap();
    assert_eq!(msg["kind"], "configuration");
    assert_eq!(msg["family"], "tcp");
    assert_eq!(msg["role"], "upload");
    assert_eq!(msg["test_id"], serde_json::json!(test_id));
    assert_eq!(msg["streams"], streams);
    assert_eq!(msg["bandwidth"], bandwidth);
    assert_eq!(msg["duration"], seconds);
    assert_eq!(msg["length"], length as u32);
    assert_eq!(msg["send_interval"], send_interval);
    assert_eq!(msg["send_buffer"], send_buffer);
    assert_eq!(msg["no_delay"], no_delay);
    assert_eq!(msg["receive_buffer"], 0);
}
