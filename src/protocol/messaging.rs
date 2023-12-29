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

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct Configuration {
    pub family: Option<String>,
    pub length: u32,
    pub receive_buffer: Option<u32>,
    pub role: String,
    pub reverse_nat: Option<bool>,
    pub streams: usize,
    pub test_id: uuid::Uuid,
    pub bandwidth: Option<u64>,
    pub duration: Option<f32>,
    pub send_interval: Option<f32>,
    pub send_buffer: Option<u32>,
    pub no_delay: Option<bool>,
    pub stream_ports: Option<Vec<u16>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct TransmitState {
    pub family: Option<String>,
    pub timestamp: f64,
    pub stream_idx: Option<usize>,
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

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct FinalState {
    pub origin: Option<String>,
    pub stream_idx: Option<usize>,
}

impl std::fmt::Display for FinalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.origin, &self.stream_idx) {
            (Some(origin), Some(stream_idx)) => write!(f, "stream {}, {}", stream_idx, origin),
            (Some(origin), None) => write!(f, "{}", origin),
            (None, Some(stream_idx)) => write!(f, "stream {}", stream_idx),
            (None, None) => write!(f, ""),
        }
    }
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
    Send(TransmitState),
    #[serde(rename(deserialize = "receive", serialize = "receive"))]
    Receive(TransmitState),
    #[serde(rename(deserialize = "done", serialize = "done"))]
    Done(FinalState),
    #[serde(rename(deserialize = "failed", serialize = "failed"))]
    Failed(FinalState),
    #[serde(rename(deserialize = "end", serialize = "end"))]
    End,
}

/// prepares a message used to tell the client to connect its test-streams
pub fn prepare_connect(stream_ports: &[u16]) -> Message {
    Message::Connect {
        stream_ports: stream_ports.to_vec(),
    }
}

fn validate_streams(streams: usize) -> usize {
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

fn calculate_length_final(length: u32, udp: bool) -> u32 {
    if udp {
        if length < crate::stream::udp::TEST_HEADER_SIZE as u32 {
            // length must be at least enough to hold the test data
            crate::stream::udp::TEST_HEADER_SIZE as u32
        } else {
            length
        }
    } else if length < crate::stream::tcp::TEST_HEADER_SIZE as u32 {
        // length must be at least enough to hold the test data
        crate::stream::tcp::TEST_HEADER_SIZE as u32
    } else {
        length
    }
}

fn calc_send_interval(mut send_interval: f32) -> f32 {
    if send_interval > 1.0 || send_interval <= 0.0 {
        log::warn!("send-interval was not in an acceptable range and has been set to 0.05");
        send_interval = 0.05
    }
    send_interval
}

fn calc_seconds(mut seconds: f32) -> f32 {
    if seconds <= 0.0 {
        log::warn!("time was not in an acceptable range and has been set to 0.0");
        seconds = 0.0
    }
    seconds
}

fn calc_bandwidth(bandwidth_s: &str) -> u64 {
    let mut bandwidth_string = bandwidth_s;
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
                    log::warn!("invalid bandwidth: {}; setting value to 1mbps", bandwidth_s);
                    bandwidth = 125000;
                }
            }
        }
        None => {
            //invalid input; fall back to 1mbps
            log::warn!("invalid bandwidth: {}; setting value to 1mbps", bandwidth_s);
            bandwidth = 125000;
        }
    }
    bandwidth
}

fn calc_exchange_length(mut length: u32, udp: bool) -> u32 {
    if udp {
        if length == 0 {
            length = 1024;
        }
    } else if length == 0 {
        length = 32 * 1024;
    }
    length
}

fn calc_buffer_length(mut buffer_length: u32, exchange_length: u32) -> u32 {
    if buffer_length != 0 && buffer_length < exchange_length {
        log::warn!(
            "requested receive or send buffer, {}, is too small to hold the data to be exchanged; it will be increased to {}",
            buffer_length,
            exchange_length * 2
        );
        buffer_length = exchange_length * 2;
    }
    buffer_length
}

/// prepares a message used to describe the download/upload role in a test
pub fn prepare_configuration(args: &crate::args::Args, test_id: uuid::Uuid) -> Configuration {
    let parallel_streams = args.parallel;
    let seconds: f32 = calc_seconds(args.time as f32);
    let send_interval: f32 = calc_send_interval(args.send_interval as f32);
    let bandwidth: u64 = calc_bandwidth(args.bandwidth.as_str());
    let length: u32 = calc_exchange_length(args.length as u32, args.udp);
    let send_buffer = calc_buffer_length(args.send_buffer as u32, length);
    let receive_buffer = calc_buffer_length(args.receive_buffer as u32, length);

    let reverse_nat = if args.reverse_nat { Some(true) } else { None };
    let role = if args.reverse { "upload" } else { "download" };
    let family = if args.udp { "udp" } else { "tcp" };
    let no_delay = if args.udp { None } else { Some(args.no_delay) };
    let length = calculate_length_final(length, args.udp);

    Configuration {
        family: Some(family.to_string()),
        role: role.to_string(),
        reverse_nat,
        test_id,
        streams: validate_streams(parallel_streams),
        bandwidth: Some(validate_bandwidth(bandwidth)),
        duration: Some(seconds),
        length,
        send_interval: Some(validate_send_interval(send_interval)),
        send_buffer: Some(send_buffer),
        no_delay,
        receive_buffer: Some(receive_buffer),
        ..Configuration::default()
    }
}

#[test]
fn test_prepare_configuration_tcp_upload() {
    let test_id = uuid::Uuid::new_v4();
    let streams: usize = 1;
    let bandwidth: u64 = 1024;
    let seconds: f32 = 1.0;
    let length: usize = 1024;
    let send_interval: f32 = 1.0;
    let send_buffer: u32 = 1024;
    let no_delay: bool = false;

    let cfg = Configuration {
        family: Some("tcp".to_string()),
        role: "upload".to_string(),
        test_id,
        streams,
        bandwidth: Some(bandwidth),
        duration: Some(seconds),
        length: length as u32,
        send_interval: Some(send_interval),
        send_buffer: Some(send_buffer),
        no_delay: Some(no_delay),
        receive_buffer: Some(0),
        ..Configuration::default()
    };
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
