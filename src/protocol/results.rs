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

use crate::protocol::messaging::{FinalState, Message};
use crate::{error_gen, BoxError, BoxResult};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/* This module contains structures used to represent and collect the results of tests.
 * Since everything is basically just a data-container with representation methods,
 * it isn't extensively documented.
 */

#[derive(PartialEq)]
pub enum IntervalResultKind {
    ClientDone,
    ClientFailed,
    ServerDone,
    ServerFailed,
    TcpReceive,
    TcpSend,
    UdpReceive,
    UdpSend,
}

pub fn get_unix_timestamp() -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_secs_f64(),
        Err(_) => panic!("SystemTime before UNIX epoch"),
    }
}

pub trait IntervalResult {
    fn kind(&self) -> IntervalResultKind;

    fn get_stream_idx(&self) -> usize;

    fn to_message(&self) -> Message;

    //produces test-results in tabular form
    fn to_string(&self, bit: bool) -> String;
}

pub type IntervalResultBox = Box<dyn IntervalResult + Sync + Send + 'static>;

pub struct ClientDoneResult {
    pub stream_idx: usize,
}
impl IntervalResult for ClientDoneResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::ClientDone
    }

    fn get_stream_idx(&self) -> usize {
        self.stream_idx
    }

    fn to_message(&self) -> Message {
        Message::Done(FinalState {
            origin: Some("client".to_string()),
            stream_idx: Some(self.stream_idx),
        })
    }

    fn to_string(&self, _bit: bool) -> String {
        format!(
            "----------\n\
            End of stream from client | stream: {}",
            self.stream_idx,
        )
    }
}
pub struct ServerDoneResult {
    pub stream_idx: usize,
}
impl IntervalResult for ServerDoneResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::ServerDone
    }

    fn get_stream_idx(&self) -> usize {
        self.stream_idx
    }

    fn to_message(&self) -> Message {
        Message::Done(FinalState {
            origin: Some("server".to_string()),
            stream_idx: Some(self.stream_idx),
        })
    }

    fn to_string(&self, _bit: bool) -> String {
        format!(
            "----------\n\
            End of stream from server | stream: {}",
            self.stream_idx,
        )
    }
}

pub struct ClientFailedResult {
    pub stream_idx: usize,
}
impl IntervalResult for ClientFailedResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::ClientFailed
    }

    fn get_stream_idx(&self) -> usize {
        self.stream_idx
    }

    fn to_message(&self) -> Message {
        Message::Failed(FinalState {
            origin: Some("client".to_string()),
            stream_idx: Some(self.stream_idx),
        })
    }

    fn to_string(&self, _bit: bool) -> String {
        format!(
            "----------\n\
            Failure in client stream | stream: {}",
            self.stream_idx,
        )
    }
}
pub struct ServerFailedResult {
    pub stream_idx: usize,
    pub error_info: String,
}

impl IntervalResult for ServerFailedResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::ServerFailed
    }

    fn get_stream_idx(&self) -> usize {
        self.stream_idx
    }

    fn to_message(&self) -> Message {
        let info = format!("server: {}", self.error_info);
        Message::Failed(FinalState {
            origin: Some(info),
            stream_idx: Some(self.stream_idx),
        })
    }

    fn to_string(&self, _bit: bool) -> String {
        format!(
            "----------\n\
            Failure in server stream | stream: {}",
            self.stream_idx,
        )
    }
}

pub struct TcpReceiveResult {
    pub receive_result: Message,
}

impl TryFrom<&Message> for TcpReceiveResult {
    type Error = BoxError;

    fn try_from(msg: &Message) -> Result<Self, Self::Error> {
        TcpReceiveResult::try_from(msg.clone())
    }
}

impl TryFrom<Message> for TcpReceiveResult {
    type Error = BoxError;

    fn try_from(receive_result: Message) -> Result<Self, Self::Error> {
        let mut receive_result = receive_result;
        if let Message::Receive(ref mut receive_result) = receive_result {
            if receive_result.family.as_deref() != Some("tcp") {
                return Err(Box::new(error_gen!("not a TCP receive-result: {:?}", receive_result.family)));
            }
        } else {
            return Err(Box::new(error_gen!("no kind specified for TCP stream-result")));
        }
        Ok(TcpReceiveResult { receive_result })
    }
}

impl TryFrom<&TcpReceiveResult> for serde_json::Value {
    type Error = BoxError;

    fn try_from(res: &TcpReceiveResult) -> Result<Self, Self::Error> {
        let mut msg = res.receive_result.clone();
        if let Message::Receive(ref mut receive_result) = msg {
            receive_result.stream_idx = None;
        } else {
            return Err(Box::new(error_gen!("not a TCP receive-result: {:?}", msg)));
        }
        Ok(serde_json::to_value(msg)?)
    }
}

impl IntervalResult for TcpReceiveResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::TcpReceive
    }

    fn get_stream_idx(&self) -> usize {
        if let Message::Receive(ref receive_result) = self.receive_result {
            receive_result.stream_idx.unwrap_or(0)
        } else {
            unreachable!();
        }
    }

    fn to_message(&self) -> Message {
        let mut msg = self.receive_result.clone();
        if let Message::Receive(ref mut receive_result) = msg {
            receive_result.family = Some("tcp".to_string());
        } else {
            unreachable!();
        }
        msg
    }

    fn to_string(&self, bit: bool) -> String {
        let receive_result = if let Message::Receive(ref receive_result) = self.receive_result {
            receive_result
        } else {
            unreachable!();
        };

        let duration_divisor = if receive_result.duration == 0.0 {
            //avoid zerodiv, which can happen if the stream fails
            1.0
        } else {
            receive_result.duration
        };

        let bytes_per_second = receive_result.bytes_received.unwrap() as f32 / duration_divisor;

        let throughput = match bit {
            true => format!("megabits/second: {:.3}", bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", bytes_per_second / 1_000_000.00),
        };

        let stream_idx = receive_result.stream_idx.unwrap();
        let bytes_received = receive_result.bytes_received.unwrap();
        format!(
            "----------\n\
            TCP receive result over {:.2}s | stream: {}\n\
            bytes: {} | per second: {:.3} | {}",
            receive_result.duration, stream_idx, bytes_received, bytes_per_second, throughput,
        )
    }
}

#[derive(Clone, Debug)]
pub struct TcpSendResult {
    pub send_result: Message,
}

impl TryFrom<&Message> for TcpSendResult {
    type Error = BoxError;

    fn try_from(msg: &Message) -> Result<Self, Self::Error> {
        TcpSendResult::try_from(msg.clone())
    }
}

impl TryFrom<Message> for TcpSendResult {
    type Error = BoxError;

    fn try_from(msg: Message) -> Result<Self, Self::Error> {
        let mut send_result = msg;
        if let Message::Send(ref mut send_result) = send_result {
            if send_result.sends_blocked.is_none() {
                // pre-0.1.8 peer
                send_result.sends_blocked = Some(0_u64); // report pre-0.1.8 status
            }

            if send_result.family.as_deref() != Some("tcp") {
                return Err(Box::new(error_gen!("not a TCP send-result: {:?}", send_result.family)));
            }
        } else {
            return Err(Box::new(error_gen!("no kind specified for UDP stream-result")));
        }
        Ok(TcpSendResult { send_result })
    }
}

impl TryFrom<&TcpSendResult> for serde_json::Value {
    type Error = BoxError;

    fn try_from(res: &TcpSendResult) -> Result<Self, Self::Error> {
        let mut msg = res.send_result.clone();
        if let Message::Send(ref mut send_result) = msg {
            send_result.stream_idx = None;
        } else {
            return Err(Box::new(error_gen!("not a TCP send-result: {:?}", msg)));
        }
        Ok(serde_json::to_value(msg)?)
    }
}

impl IntervalResult for TcpSendResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::TcpSend
    }

    fn get_stream_idx(&self) -> usize {
        if let Message::Send(ref send_result) = self.send_result {
            send_result.stream_idx.unwrap_or(0)
        } else {
            unreachable!();
        }
    }

    fn to_message(&self) -> Message {
        let mut msg = self.send_result.clone();
        if let Message::Send(ref mut send_result) = msg {
            send_result.family = Some("tcp".to_string());
        } else {
            unreachable!();
        }
        msg
    }

    fn to_string(&self, bit: bool) -> String {
        let send_result = if let Message::Send(ref send_result) = self.send_result {
            send_result
        } else {
            unreachable!();
        };

        let duration_divisor = if send_result.duration == 0.0 {
            //avoid zerodiv, which can happen if the stream fails
            1.0
        } else {
            send_result.duration
        };

        let bytes_per_second = send_result.bytes_sent.unwrap() as f32 / duration_divisor;

        let throughput = match bit {
            true => format!("megabits/second: {:.3}", bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", bytes_per_second / 1_000_000.00),
        };

        let stream_idx = send_result.stream_idx.unwrap();
        let bytes_sent = send_result.bytes_sent.unwrap();
        let mut output = format!(
            "----------\n\
            TCP send result over {:.2}s | stream: {}\n\
            bytes: {} | per second: {:.3} | {}",
            send_result.duration, stream_idx, bytes_sent, bytes_per_second, throughput,
        );
        let sends_blocked = send_result.sends_blocked.unwrap();
        if sends_blocked > 0 {
            output.push_str(&format!("\nstalls due to full send-buffer: {}", sends_blocked));
        }
        output
    }
}

pub struct UdpReceiveResult {
    pub receive_result: Message,
}

impl TryFrom<&Message> for UdpReceiveResult {
    type Error = BoxError;

    fn try_from(msg: &Message) -> Result<Self, Self::Error> {
        UdpReceiveResult::try_from(msg.clone())
    }
}

impl TryFrom<Message> for UdpReceiveResult {
    type Error = BoxError;

    fn try_from(msg: Message) -> Result<Self, Self::Error> {
        let receive_result = msg;
        if let Message::Receive(ref receive_result) = receive_result {
            if receive_result.family.as_deref() != Some("udp") {
                return Err(Box::new(error_gen!("not a UDP receive-result: {:?}", receive_result.family)));
            }
        } else {
            return Err(Box::new(error_gen!("no kind specified for UDP stream-result")));
        }
        Ok(UdpReceiveResult { receive_result })
    }
}

impl TryFrom<&UdpReceiveResult> for serde_json::Value {
    type Error = BoxError;

    fn try_from(res: &UdpReceiveResult) -> Result<Self, Self::Error> {
        let mut msg = res.receive_result.clone();
        if let Message::Receive(ref mut receive_result) = msg {
            receive_result.stream_idx = None;
        } else {
            return Err(Box::new(error_gen!("not a UDP receive-result: {:?}", msg)));
        }
        Ok(serde_json::to_value(msg)?)
    }
}

impl IntervalResult for UdpReceiveResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::UdpReceive
    }

    fn get_stream_idx(&self) -> usize {
        if let Message::Receive(ref receive_result) = self.receive_result {
            receive_result.stream_idx.unwrap_or(0)
        } else {
            unreachable!();
        }
    }

    fn to_message(&self) -> Message {
        let mut msg = self.receive_result.clone();
        if let Message::Receive(ref mut receive_result) = msg {
            receive_result.family = Some("udp".to_string());
        } else {
            unreachable!();
        }
        msg
    }

    fn to_string(&self, bit: bool) -> String {
        let receive_result = if let Message::Receive(ref receive_result) = self.receive_result {
            receive_result
        } else {
            unreachable!();
        };

        let duration_divisor = if receive_result.duration == 0.0 {
            //avoid zerodiv, which can happen if the stream fails
            1.0
        } else {
            receive_result.duration
        };

        let bytes_per_second = receive_result.bytes_received.unwrap() as f32 / duration_divisor;

        let throughput = match bit {
            true => format!("megabits/second: {:.3}", bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", bytes_per_second / 1_000_000.00),
        };

        let mut output = format!(
            "----------\n\
            UDP receive result over {:.2}s | stream: {}\n\
            bytes: {} | per second: {:.3} | {}\n\
            packets: {} | lost: {} | out-of-order: {} | duplicate: {} | per second: {:.3}",
            receive_result.duration,
            receive_result.stream_idx.unwrap(),
            receive_result.bytes_received.unwrap(),
            bytes_per_second,
            throughput,
            receive_result.packets_received.unwrap(),
            receive_result.packets_lost.unwrap(),
            receive_result.packets_out_of_order.unwrap(),
            receive_result.packets_duplicated.unwrap(),
            receive_result.packets_received.unwrap() as f32 / duration_divisor,
        );
        if receive_result.jitter_seconds.is_some() {
            output.push_str(&format!(
                "\njitter: {:.6}s over {} consecutive packets",
                receive_result.jitter_seconds.unwrap(),
                receive_result.unbroken_sequence.unwrap()
            ));
        }
        output
    }
}

pub struct UdpSendResult {
    pub send_result: Message,
}

impl TryFrom<&Message> for UdpSendResult {
    type Error = BoxError;

    fn try_from(msg: &Message) -> Result<Self, Self::Error> {
        UdpSendResult::try_from(msg.clone())
    }
}

impl TryFrom<Message> for UdpSendResult {
    type Error = BoxError;

    fn try_from(msg: Message) -> Result<Self, Self::Error> {
        let mut send_result = msg;
        if let Message::Send(ref mut send_result) = send_result {
            if send_result.sends_blocked.is_none() {
                // pre-0.1.8 peer
                send_result.sends_blocked = Some(0_u64); // report pre-0.1.8 status
            }

            if send_result.family.as_deref() != Some("udp") {
                return Err(Box::new(error_gen!("not a UDP send-result: {:?}", send_result.family)));
            }
        } else {
            return Err(Box::new(error_gen!("no kind specified for UDP stream-result")));
        }
        Ok(UdpSendResult { send_result })
    }
}

impl TryFrom<&UdpSendResult> for serde_json::Value {
    type Error = BoxError;

    fn try_from(res: &UdpSendResult) -> Result<Self, Self::Error> {
        let mut msg = res.send_result.clone();
        if let Message::Send(ref mut send_result) = msg {
            send_result.stream_idx = None;
        } else {
            return Err(Box::new(error_gen!("not a UDP send-result: {:?}", msg)));
        }
        Ok(serde_json::to_value(msg)?)
    }
}

impl IntervalResult for UdpSendResult {
    fn kind(&self) -> IntervalResultKind {
        IntervalResultKind::UdpSend
    }

    fn get_stream_idx(&self) -> usize {
        if let Message::Send(ref send_result) = self.send_result {
            send_result.stream_idx.unwrap_or(0)
        } else {
            unreachable!();
        }
    }

    fn to_message(&self) -> Message {
        let mut msg = self.send_result.clone();
        if let Message::Send(ref mut send_result) = msg {
            send_result.family = Some("udp".to_string());
        } else {
            unreachable!();
        }
        msg
    }

    fn to_string(&self, bit: bool) -> String {
        let send_result = if let Message::Send(ref send_result) = self.send_result {
            send_result
        } else {
            unreachable!();
        };

        let duration_divisor = if send_result.duration == 0.0 {
            //avoid zerodiv, which can happen if the stream fails
            1.0
        } else {
            send_result.duration
        };

        let bytes_per_second = send_result.bytes_sent.unwrap() as f32 / duration_divisor;

        let throughput = match bit {
            true => format!("megabits/second: {:.3}", bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", bytes_per_second / 1_000_000.00),
        };

        let mut output = format!(
            "----------\n\
            UDP send result over {:.2}s | stream: {}\n\
            bytes: {} | per second: {:.3} | {}\n\
            packets: {} per second: {:.3}",
            send_result.duration,
            send_result.stream_idx.unwrap(),
            send_result.bytes_sent.unwrap(),
            bytes_per_second,
            throughput,
            send_result.packets_sent.unwrap(),
            send_result.packets_sent.unwrap() as f32 / duration_divisor,
        );
        if send_result.sends_blocked.unwrap() > 0 {
            output.push_str(&format!("\nstalls due to full send-buffer: {}", send_result.sends_blocked.unwrap()));
        }
        output
    }
}

pub fn interval_result_from_message(msg: &Message) -> BoxResult<IntervalResultBox> {
    match msg {
        Message::Receive(res) => {
            if res.family.as_deref() == Some("tcp") {
                Ok(Box::new(TcpReceiveResult::try_from(msg)?))
            } else if res.family.as_deref() == Some("udp") {
                Ok(Box::new(UdpReceiveResult::try_from(msg)?))
            } else {
                Err(Box::new(error_gen!("unsupported interval-result family")))
            }
        }
        Message::Send(res) => {
            if res.family.as_deref() == Some("tcp") {
                Ok(Box::new(TcpSendResult::try_from(msg)?))
            } else if res.family.as_deref() == Some("udp") {
                Ok(Box::new(UdpSendResult::try_from(msg)?))
            } else {
                Err(Box::new(error_gen!("unsupported interval-result family")))
            }
        }
        _ => Err(Box::new(error_gen!("unsupported interval-result kind"))),
    }
}

pub trait ResultsCollection {
    fn update_from_message(&mut self, msg: &Message) -> BoxResult<()>;

    fn to_json(&self, omit_seconds: usize) -> serde_json::Value;
}

struct TcpResultsCollection {
    receive_results: Vec<TcpReceiveResult>,
    send_results: Vec<TcpSendResult>,
}
impl ResultsCollection for TcpResultsCollection {
    fn update_from_message(&mut self, msg: &Message) -> BoxResult<()> {
        match msg {
            Message::Receive(_) => self.receive_results.push(TcpReceiveResult::try_from(msg)?),
            Message::Send(_) => self.send_results.push(TcpSendResult::try_from(msg)?),
            _ => return Err(Box::new(error_gen!("unsupported message type for TCP stream-result"))),
        }
        Ok(())
    }

    fn to_json(&self, omit_seconds: usize) -> serde_json::Value {
        let mut duration_send: f64 = 0.0;
        let mut bytes_sent: u64 = 0;

        let mut duration_receive: f64 = 0.0;
        let mut bytes_received: u64 = 0;

        for (i, sr) in self.send_results.iter().enumerate() {
            if i < omit_seconds {
                continue;
            }

            if let Message::Send(ref sr) = sr.send_result {
                duration_send += sr.duration as f64;
                bytes_sent += sr.bytes_sent.unwrap();
            }
        }

        for (i, rr) in self.receive_results.iter().enumerate() {
            if i < omit_seconds {
                continue;
            }

            if let Message::Receive(ref rr) = rr.receive_result {
                duration_receive += rr.duration as f64;
                bytes_received += rr.bytes_received.unwrap();
            }
        }

        let summary = serde_json::json!({
            "duration_send": duration_send,
            "bytes_sent": bytes_sent,

            "duration_receive": duration_receive,
            "bytes_received": bytes_received,
        });

        serde_json::json!({
            "receive": self.receive_results.iter().map(|rr| rr.try_into().unwrap()).collect::<Vec<serde_json::Value>>(),
            "send": self.send_results.iter().map(|sr| sr.try_into().unwrap()).collect::<Vec<serde_json::Value>>(),
            "summary": summary,
        })
    }
}

struct UdpResultsCollection {
    receive_results: Vec<UdpReceiveResult>,
    send_results: Vec<UdpSendResult>,
}

impl ResultsCollection for UdpResultsCollection {
    fn update_from_message(&mut self, msg: &Message) -> BoxResult<()> {
        match msg {
            Message::Receive(_) => self.receive_results.push(UdpReceiveResult::try_from(msg)?),
            Message::Send(_) => self.send_results.push(UdpSendResult::try_from(msg)?),
            _ => {
                return Err(Box::new(error_gen!("unsupported message type for UDP stream-result")));
            }
        }
        Ok(())
    }

    fn to_json(&self, omit_seconds: usize) -> serde_json::Value {
        let mut duration_send: f64 = 0.0;

        let mut bytes_sent: u64 = 0;
        let mut packets_sent: u64 = 0;

        let mut duration_receive: f64 = 0.0;

        let mut bytes_received: u64 = 0;
        let mut packets_received: u64 = 0;
        let mut packets_out_of_order: u64 = 0;
        let mut packets_duplicated: u64 = 0;

        let mut jitter_calculated = false;
        let mut unbroken_sequence_count: u64 = 0;
        let mut jitter_weight: f64 = 0.0;

        for (i, sr) in self.send_results.iter().enumerate() {
            if i < omit_seconds {
                continue;
            }

            if let Message::Send(ref sr) = sr.send_result {
                duration_send += sr.duration as f64;

                bytes_sent += sr.bytes_sent.unwrap();
                packets_sent += sr.packets_sent.unwrap();
            }
        }

        for (i, rr) in self.receive_results.iter().enumerate() {
            if i < omit_seconds {
                continue;
            }

            if let Message::Receive(ref rr) = rr.receive_result {
                duration_receive += rr.duration as f64;

                bytes_received += rr.bytes_received.unwrap();
                packets_received += rr.packets_received.unwrap();
                packets_out_of_order += rr.packets_out_of_order.unwrap();
                packets_duplicated += rr.packets_duplicated.unwrap();

                if rr.jitter_seconds.is_some() {
                    jitter_weight += (rr.unbroken_sequence.unwrap() as f64) * (rr.jitter_seconds.unwrap() as f64);
                    unbroken_sequence_count += rr.unbroken_sequence.unwrap();

                    jitter_calculated = true;
                }
            }
        }

        let mut summary = serde_json::json!({
            "duration_send": duration_send,

            "bytes_sent": bytes_sent,
            "packets_sent": packets_sent,

            "duration_receive": duration_receive,

            "bytes_received": bytes_received,
            "packets_received": packets_received,
            "packets_lost": packets_sent - packets_received,
            "packets_out_of_order": packets_out_of_order,
            "packets_duplicated": packets_duplicated,
        });
        if packets_sent > 0 {
            summary["framed_packet_size"] = serde_json::json!(bytes_sent / packets_sent);
        }
        if jitter_calculated {
            summary["jitter_average"] = serde_json::json!(jitter_weight / (unbroken_sequence_count as f64));
            summary["jitter_packets_consecutive"] = serde_json::json!(unbroken_sequence_count);
        }

        serde_json::json!({
            "receive": self.receive_results.iter().map(|rr| rr.try_into().unwrap()).collect::<Vec<serde_json::Value>>(),
            "send": self.send_results.iter().map(|sr| sr.try_into().unwrap()).collect::<Vec<serde_json::Value>>(),
            "summary": summary,
        })
    }
}

pub trait TestResults {
    fn count_in_progress_streams(&self) -> usize;
    fn mark_stream_done(&mut self, idx: usize, success: bool);

    fn count_in_progress_streams_server(&self) -> usize;
    fn mark_stream_done_server(&mut self, idx: usize);

    fn is_success(&self) -> bool;

    fn update_from_message(&mut self, msg: &Message) -> BoxResult<()>;

    fn to_json(
        &self,
        omit_seconds: usize,
        upload_config: serde_json::Value,
        download_config: serde_json::Value,
        common_config: serde_json::Value,
        additional_config: serde_json::Value,
    ) -> serde_json::Value;

    //produces a pretty-printed JSON string with the test results
    fn to_json_string(
        &self,
        omit_seconds: usize,
        upload_config: serde_json::Value,
        download_config: serde_json::Value,
        common_config: serde_json::Value,
        additional_config: serde_json::Value,
    ) -> String {
        serde_json::to_string_pretty(&self.to_json(omit_seconds, upload_config, download_config, common_config, additional_config)).unwrap()
    }

    //produces test-results in tabular form
    fn to_string(&self, bit: bool, omit_seconds: usize) -> String;
}

pub struct TcpTestResults {
    stream_results: HashMap<usize, TcpResultsCollection>,
    pending_tests: HashSet<usize>,
    failed_tests: HashSet<usize>,
    server_tests_finished: HashSet<usize>,
}
impl TcpTestResults {
    pub fn new() -> TcpTestResults {
        TcpTestResults {
            stream_results: HashMap::new(),
            pending_tests: HashSet::new(),
            failed_tests: HashSet::new(),
            server_tests_finished: HashSet::new(),
        }
    }
    pub fn prepare_index(&mut self, idx: usize) {
        self.stream_results.insert(
            idx,
            TcpResultsCollection {
                receive_results: Vec::new(),
                send_results: Vec::new(),
            },
        );
        self.pending_tests.insert(idx);
    }
}
impl TestResults for TcpTestResults {
    fn count_in_progress_streams(&self) -> usize {
        self.pending_tests.len()
    }
    fn mark_stream_done(&mut self, idx: usize, success: bool) {
        self.pending_tests.remove(&idx);
        if !success {
            self.failed_tests.insert(idx);
        }
    }

    fn count_in_progress_streams_server(&self) -> usize {
        let mut count = 0;
        for idx in self.stream_results.keys() {
            if !self.server_tests_finished.contains(idx) {
                count += 1;
            }
        }
        count
    }
    fn mark_stream_done_server(&mut self, idx: usize) {
        self.server_tests_finished.insert(idx);
    }

    fn is_success(&self) -> bool {
        self.pending_tests.is_empty() && self.failed_tests.is_empty()
    }

    fn update_from_message(&mut self, msg: &Message) -> BoxResult<()> {
        match msg {
            Message::Receive(res) | Message::Send(res) => {
                if res.family.as_deref() == Some("tcp") {
                    let idx = res.stream_idx.unwrap();
                    match self.stream_results.get_mut(&idx) {
                        Some(stream_results) => {
                            stream_results.update_from_message(msg)?;
                            Ok(())
                        }
                        None => Err(Box::new(error_gen!("stream-index {} is not a valid identifier", idx))),
                    }
                } else {
                    Err(Box::new(error_gen!("unsupported family for TCP stream-result: {:?}", res.family)))
                }
            }
            _ => Err(Box::new(error_gen!("unsupported message type for TCP stream-result"))),
        }
    }

    fn to_json(
        &self,
        omit_seconds: usize,
        upload_config: serde_json::Value,
        download_config: serde_json::Value,
        common_config: serde_json::Value,
        additional_config: serde_json::Value,
    ) -> serde_json::Value {
        let mut duration_send: f64 = 0.0;
        let mut bytes_sent: u64 = 0;

        let mut duration_receive: f64 = 0.0;
        let mut bytes_received: u64 = 0;

        let mut streams = Vec::with_capacity(self.stream_results.len());
        for (idx, stream) in self.stream_results.iter() {
            streams.push(serde_json::json!({
                "intervals": stream.to_json(omit_seconds),
                "abandoned": self.pending_tests.contains(idx),
                "failed": self.failed_tests.contains(idx),
            }));

            for (i, sr) in stream.send_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }
                if let Message::Send(ref sr) = sr.send_result {
                    duration_send += sr.duration as f64;
                    bytes_sent += sr.bytes_sent.unwrap();
                }
            }

            for (i, rr) in stream.receive_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }

                if let Message::Receive(ref rr) = rr.receive_result {
                    duration_receive += rr.duration as f64;
                    bytes_received += rr.bytes_received.unwrap();
                }
            }
        }

        let summary = serde_json::json!({
            "duration_send": duration_send,
            "bytes_sent": bytes_sent,

            "duration_receive": duration_receive,
            "bytes_received": bytes_received,
        });

        serde_json::json!({
            "config": {
                "upload": upload_config,
                "download": download_config,
                "common": common_config,
                "additional": additional_config,
            },
            "streams": streams,
            "summary": summary,
            "success": self.is_success(),
        })
    }

    fn to_string(&self, bit: bool, omit_seconds: usize) -> String {
        let stream_count = self.stream_results.len();
        let mut stream_send_durations = vec![0.0; stream_count];
        let mut stream_receive_durations = vec![0.0; stream_count];

        let mut duration_send: f64 = 0.0;
        let mut bytes_sent: u64 = 0;

        let mut duration_receive: f64 = 0.0;
        let mut bytes_received: u64 = 0;

        let mut sends_blocked = false;

        for (stream_idx, stream) in self.stream_results.values().enumerate() {
            for (i, sr) in stream.send_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }

                if let Message::Send(ref sr) = sr.send_result {
                    duration_send += sr.duration as f64;
                    stream_send_durations[stream_idx] += sr.duration as f64;

                    bytes_sent += sr.bytes_sent.unwrap();

                    sends_blocked |= sr.sends_blocked.unwrap() > 0;
                }
            }

            for (i, rr) in stream.receive_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }

                if let Message::Receive(ref rr) = rr.receive_result {
                    duration_receive += rr.duration as f64;
                    stream_receive_durations[stream_idx] += rr.duration as f64;
                    bytes_received += rr.bytes_received.unwrap();
                }
            }
        }
        stream_send_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
        stream_receive_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let send_duration_divisor = if duration_send == 0.0 {
            //avoid zerodiv, which can happen if all streams fail
            1.0
        } else {
            duration_send
        };
        let send_bytes_per_second = bytes_sent as f64 / send_duration_divisor;
        let send_throughput = match bit {
            true => format!("megabits/second: {:.3}", send_bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", send_bytes_per_second / 1_000_000.00),
        };
        let total_send_throughput = match bit {
            true => format!(
                "megabits/second: {:.3}",
                (send_bytes_per_second / (1_000_000.00 / 8.0)) * stream_count as f64
            ),
            false => format!(
                "megabytes/second: {:.3}",
                (send_bytes_per_second / 1_000_000.00) * stream_count as f64
            ),
        };

        let receive_duration_divisor = if duration_receive == 0.0 {
            //avoid zerodiv, which can happen if all streams fail
            1.0
        } else {
            duration_receive
        };
        let receive_bytes_per_second = bytes_received as f64 / receive_duration_divisor;
        let receive_throughput = match bit {
            true => format!("megabits/second: {:.3}", receive_bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", receive_bytes_per_second / 1_000_000.00),
        };
        let total_receive_throughput = match bit {
            true => format!(
                "megabits/second: {:.3}",
                (receive_bytes_per_second / (1_000_000.00 / 8.0)) * stream_count as f64
            ),
            false => format!(
                "megabytes/second: {:.3}",
                (receive_bytes_per_second / 1_000_000.00) * stream_count as f64
            ),
        };

        let mut output = format!(
            "==========\n\
            TCP send result over {:.2}s | streams: {}\n\
            stream-average bytes per second: {:.3} | {}\n\
            total bytes: {} | per second: {:.3} | {}\n\
            ==========\n\
            TCP receive result over {:.2}s | streams: {}\n\
            stream-average bytes per second: {:.3} | {}\n\
            total bytes: {} | per second: {:.3} | {}",
            stream_send_durations[stream_send_durations.len() - 1],
            stream_count,
            send_bytes_per_second,
            send_throughput,
            bytes_sent,
            send_bytes_per_second * stream_count as f64,
            total_send_throughput,
            stream_receive_durations[stream_receive_durations.len() - 1],
            stream_count,
            receive_bytes_per_second,
            receive_throughput,
            bytes_received,
            receive_bytes_per_second * stream_count as f64,
            total_receive_throughput,
        );
        if sends_blocked {
            output.push_str("\nthroughput throttled by buffer limitations");
        }
        if !self.is_success() {
            output.push_str("\nTESTING DID NOT COMPLETE SUCCESSFULLY");
        }

        output
    }
}

pub struct UdpTestResults {
    stream_results: HashMap<usize, UdpResultsCollection>,
    pending_tests: HashSet<usize>,
    failed_tests: HashSet<usize>,
    server_tests_finished: HashSet<usize>,
}
impl UdpTestResults {
    pub fn new() -> UdpTestResults {
        UdpTestResults {
            stream_results: HashMap::new(),
            pending_tests: HashSet::new(),
            failed_tests: HashSet::new(),
            server_tests_finished: HashSet::new(),
        }
    }
    pub fn prepare_index(&mut self, idx: usize) {
        self.stream_results.insert(
            idx,
            UdpResultsCollection {
                receive_results: Vec::new(),
                send_results: Vec::new(),
            },
        );
        self.pending_tests.insert(idx);
    }
}
impl TestResults for UdpTestResults {
    fn count_in_progress_streams(&self) -> usize {
        self.pending_tests.len()
    }
    fn mark_stream_done(&mut self, idx: usize, success: bool) {
        self.pending_tests.remove(&idx);
        if !success {
            self.failed_tests.insert(idx);
        }
    }

    fn count_in_progress_streams_server(&self) -> usize {
        let mut count = 0;
        for idx in self.stream_results.keys() {
            if !self.server_tests_finished.contains(idx) {
                count += 1;
            }
        }
        count
    }
    fn mark_stream_done_server(&mut self, idx: usize) {
        self.server_tests_finished.insert(idx);
    }

    fn is_success(&self) -> bool {
        self.pending_tests.is_empty() && self.failed_tests.is_empty()
    }

    fn update_from_message(&mut self, msg: &Message) -> BoxResult<()> {
        match msg {
            Message::Receive(res) | Message::Send(res) => {
                if res.family.as_deref() == Some("udp") {
                    let idx = res.stream_idx.unwrap();
                    match self.stream_results.get_mut(&idx) {
                        Some(stream_results) => {
                            stream_results.update_from_message(msg)?;
                            Ok(())
                        }
                        None => Err(Box::new(error_gen!("stream-index {} is not a valid identifier", idx))),
                    }
                } else {
                    Err(Box::new(error_gen!("unsupported family for UDP stream-result: {:?}", res.family)))
                }
            }
            _ => Err(Box::new(error_gen!("unsupported message type for UDP stream-result"))),
        }
    }

    fn to_json(
        &self,
        omit_seconds: usize,
        upload_config: serde_json::Value,
        download_config: serde_json::Value,
        common_config: serde_json::Value,
        additional_config: serde_json::Value,
    ) -> serde_json::Value {
        let mut duration_send: f64 = 0.0;

        let mut bytes_sent: u64 = 0;
        let mut packets_sent: u64 = 0;

        let mut duration_receive: f64 = 0.0;

        let mut bytes_received: u64 = 0;
        let mut packets_received: u64 = 0;
        let mut packets_out_of_order: u64 = 0;
        let mut packets_duplicated: u64 = 0;

        let mut jitter_calculated = false;
        let mut unbroken_sequence_count: u64 = 0;
        let mut jitter_weight: f64 = 0.0;

        let mut streams = Vec::with_capacity(self.stream_results.len());
        for (idx, stream) in self.stream_results.iter() {
            streams.push(serde_json::json!({
                "intervals": stream.to_json(omit_seconds),
                "abandoned": self.pending_tests.contains(idx),
                "failed": self.failed_tests.contains(idx),
            }));

            for (i, sr) in stream.send_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }

                if let Message::Send(ref sr) = sr.send_result {
                    duration_send += sr.duration as f64;

                    bytes_sent += sr.bytes_sent.unwrap();
                    packets_sent += sr.packets_sent.unwrap();
                }
            }

            for (i, rr) in stream.receive_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }

                if let Message::Receive(ref rr) = rr.receive_result {
                    duration_receive += rr.duration as f64;

                    bytes_received += rr.bytes_received.unwrap();
                    packets_received += rr.packets_received.unwrap();
                    packets_out_of_order += rr.packets_out_of_order.unwrap();
                    packets_duplicated += rr.packets_duplicated.unwrap();

                    if rr.jitter_seconds.is_some() {
                        jitter_weight += (rr.unbroken_sequence.unwrap() as f64) * (rr.jitter_seconds.unwrap() as f64);
                        unbroken_sequence_count += rr.unbroken_sequence.unwrap();

                        jitter_calculated = true;
                    }
                }
            }
        }

        let mut summary = serde_json::json!({
            "duration_send": duration_send,

            "bytes_sent": bytes_sent,
            "packets_sent": packets_sent,

            "duration_receive": duration_receive,

            "bytes_received": bytes_received,
            "packets_received": packets_received,
            "packets_lost": packets_sent - packets_received,
            "packets_out_of_order": packets_out_of_order,
            "packets_duplicated": packets_duplicated,
        });
        if packets_sent > 0 {
            summary["framed_packet_size"] = serde_json::json!(bytes_sent / packets_sent);
        }
        if jitter_calculated {
            summary["jitter_average"] = serde_json::json!(jitter_weight / (unbroken_sequence_count as f64));
            summary["jitter_packets_consecutive"] = serde_json::json!(unbroken_sequence_count);
        }

        serde_json::json!({
            "config": {
                "upload": upload_config,
                "download": download_config,
                "common": common_config,
                "additional": additional_config,
            },
            "streams": streams,
            "summary": summary,
            "success": self.is_success(),
        })
    }

    fn to_string(&self, bit: bool, omit_seconds: usize) -> String {
        let stream_count = self.stream_results.len();
        let mut stream_send_durations = vec![0.0; stream_count];
        let mut stream_receive_durations = vec![0.0; stream_count];

        let mut duration_send: f64 = 0.0;

        let mut bytes_sent: u64 = 0;
        let mut packets_sent: u64 = 0;

        let mut duration_receive: f64 = 0.0;

        let mut bytes_received: u64 = 0;
        let mut packets_received: u64 = 0;
        let mut packets_out_of_order: u64 = 0;
        let mut packets_duplicated: u64 = 0;

        let mut jitter_calculated = false;
        let mut unbroken_sequence_count: u64 = 0;
        let mut jitter_weight: f64 = 0.0;

        let mut sends_blocked = false;

        for (stream_idx, stream) in self.stream_results.values().enumerate() {
            for (i, sr) in stream.send_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }

                if let Message::Send(ref sr) = sr.send_result {
                    duration_send += sr.duration as f64;
                    stream_send_durations[stream_idx] += sr.duration as f64;

                    bytes_sent += sr.bytes_sent.unwrap();
                    packets_sent += sr.packets_sent.unwrap();

                    sends_blocked |= sr.sends_blocked.unwrap() > 0;
                }
            }

            for (i, rr) in stream.receive_results.iter().enumerate() {
                if i < omit_seconds {
                    continue;
                }

                if let Message::Receive(ref rr) = rr.receive_result {
                    duration_receive += rr.duration as f64;
                    stream_receive_durations[stream_idx] += rr.duration as f64;

                    bytes_received += rr.bytes_received.unwrap();
                    packets_received += rr.packets_received.unwrap();
                    packets_out_of_order += rr.packets_out_of_order.unwrap();
                    packets_duplicated += rr.packets_duplicated.unwrap();

                    if rr.jitter_seconds.is_some() {
                        jitter_weight += (rr.unbroken_sequence.unwrap() as f64) * (rr.jitter_seconds.unwrap() as f64);
                        unbroken_sequence_count += rr.unbroken_sequence.unwrap();

                        jitter_calculated = true;
                    }
                }
            }
        }
        stream_send_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
        stream_receive_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let send_duration_divisor = if duration_send == 0.0 {
            //avoid zerodiv, which can happen if all streams fail
            1.0
        } else {
            duration_send
        };
        let send_bytes_per_second = bytes_sent as f64 / send_duration_divisor;
        let send_throughput = match bit {
            true => format!("megabits/second: {:.3}", send_bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", send_bytes_per_second / 1_000_000.00),
        };
        let total_send_throughput = match bit {
            true => format!(
                "megabits/second: {:.3}",
                (send_bytes_per_second / (1_000_000.00 / 8.0)) * stream_count as f64
            ),
            false => format!(
                "megabytes/second: {:.3}",
                (send_bytes_per_second / 1_000_000.00) * stream_count as f64
            ),
        };

        let receive_duration_divisor = if duration_receive == 0.0 {
            //avoid zerodiv, which can happen if all streams fail
            1.0
        } else {
            duration_receive
        };
        let receive_bytes_per_second = bytes_received as f64 / receive_duration_divisor;
        let receive_throughput = match bit {
            true => format!("megabits/second: {:.3}", receive_bytes_per_second / (1_000_000.00 / 8.0)),
            false => format!("megabytes/second: {:.3}", receive_bytes_per_second / 1_000_000.00),
        };
        let total_receive_throughput = match bit {
            true => format!(
                "megabits/second: {:.3}",
                (receive_bytes_per_second / (1_000_000.00 / 8.0)) * stream_count as f64
            ),
            false => format!(
                "megabytes/second: {:.3}",
                (receive_bytes_per_second / 1_000_000.00) * stream_count as f64
            ),
        };

        let packets_lost = packets_sent - (packets_received - packets_duplicated);
        let packets_sent_divisor = if packets_sent == 0 {
            //avoid zerodiv, which can happen if all streams fail
            1.0
        } else {
            packets_sent as f64
        };
        let mut output = format!(
            "==========\n\
            UDP send result over {:.2}s | streams: {}\n\
            stream-average bytes per second: {:.3} | {}\n\
            total bytes: {} | per second: {:.3} | {}\n\
            packets: {} per second: {:.3}\n\
            ==========\n\
            UDP receive result over {:.2}s | streams: {}\n\
            stream-average bytes per second: {:.3} | {}\n\
            total bytes: {} | per second: {:.3} | {}\n\
            packets: {} | lost: {} ({:.1}%) | out-of-order: {} | duplicate: {} | per second: {:.3}",
            stream_send_durations[stream_send_durations.len() - 1],
            stream_count,
            send_bytes_per_second,
            send_throughput,
            bytes_sent,
            send_bytes_per_second * stream_count as f64,
            total_send_throughput,
            packets_sent,
            (packets_sent as f64 / send_duration_divisor) * stream_count as f64,
            stream_receive_durations[stream_receive_durations.len() - 1],
            stream_count,
            receive_bytes_per_second,
            receive_throughput,
            bytes_received,
            receive_bytes_per_second * stream_count as f64,
            total_receive_throughput,
            packets_received,
            packets_lost,
            (packets_lost as f64 / packets_sent_divisor) * 100.0,
            packets_out_of_order,
            packets_duplicated,
            (packets_received as f64 / receive_duration_divisor) * stream_count as f64,
        );
        if jitter_calculated {
            output.push_str(&format!(
                "\njitter: {:.6}s over {} consecutive packets",
                jitter_weight / (unbroken_sequence_count as f64),
                unbroken_sequence_count
            ));
        }
        if sends_blocked {
            output.push_str("\nthroughput throttled by buffer limitations");
        }
        if !self.is_success() {
            output.push_str("\nTESTING DID NOT COMPLETE SUCCESSFULLY");
        }

        output
    }
}
