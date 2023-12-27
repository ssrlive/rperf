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

use crate::{
    args, error_gen,
    protocol::{
        communication::{receive_message, send_message},
        messaging::{prepare_download_configuration, prepare_upload_configuration, Message},
        results::{interval_result_from_message, ClientDoneResult, ClientFailedResult},
        results::{IntervalResultBox, IntervalResultKind, TcpTestResults, TestResults, UdpTestResults},
    },
    stream::{tcp, udp, TestRunner},
    utils::cpu_affinity::CpuAffinityManager,
    BoxResult,
};
use std::{
    net::{IpAddr, Shutdown, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, channel},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// when false, the system is shutting down
static ALIVE: AtomicBool = AtomicBool::new(true);

/// a deferred kill-switch to handle shutdowns a bit more gracefully in the event of a probable disconnect
static mut KILL_TIMER_RELATIVE_START_TIME: f64 = 0.0; //the time at which the kill-timer was started
const KILL_TIMEOUT: f64 = 5.0; //once testing finishes, allow a few seconds for the server to respond

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

fn connect_to_server(address: &str, port: u16) -> BoxResult<TcpStream> {
    let destination = format!("{}:{}", address, port);
    log::info!("connecting to server at {}...", destination);

    let server_addr = destination.to_socket_addrs()?.next();
    if server_addr.is_none() {
        return Err(Box::new(error_gen!("unable to resolve {}", address)));
    }
    let stream = match std::net::TcpStream::connect_timeout(&server_addr.unwrap(), CONNECT_TIMEOUT) {
        Ok(s) => s,
        Err(e) => return Err(Box::new(error_gen!("unable to connect: {}", e))),
    };

    log::debug!("connected TCP control-channel to {}", destination);
    stream.set_nodelay(true)?;

    #[cfg(unix)]
    {
        use crate::protocol::communication::KEEPALIVE_DURATION;
        let keepalive_parameters = socket2::TcpKeepalive::new().with_time(KEEPALIVE_DURATION);
        let raw_socket = socket2::SockRef::from(&stream);
        raw_socket.set_tcp_keepalive(&keepalive_parameters)?;
    }

    log::info!("connected to server");

    Ok(stream)
}

fn prepare_test_results(is_udp: bool, stream_count: usize) -> Mutex<Box<dyn TestResults + Send + Sync + 'static>> {
    if is_udp {
        //UDP
        let mut udp_test_results = UdpTestResults::new();
        for i in 0..stream_count {
            udp_test_results.prepare_index(i);
        }
        Mutex::new(Box::new(udp_test_results))
    } else {
        //TCP
        let mut tcp_test_results = TcpTestResults::new();
        for i in 0..stream_count {
            tcp_test_results.prepare_index(i);
        }
        Mutex::new(Box::new(tcp_test_results))
    }
}

pub fn execute(args: &args::Args) -> BoxResult<()> {
    let mut complete = false;

    //config-parsing and pre-connection setup
    let mut tcp_port_pool = tcp::receiver::TcpPortPool::new(&args.tcp_port_pool, &args.tcp6_port_pool);
    let mut udp_port_pool = udp::receiver::UdpPortPool::new(&args.udp_port_pool, &args.udp6_port_pool);

    let cpu_affinity_manager = Arc::new(Mutex::new(CpuAffinityManager::new(&args.affinity)?));

    let display_json: bool;
    let display_bit: bool;
    match args.format {
        args::Format::Json => {
            display_json = true;
            display_bit = false;
        }
        args::Format::Megabit => {
            display_json = false;
            display_bit = true;
        }
        args::Format::Megabyte => {
            display_json = false;
            display_bit = false;
        }
    }

    let is_udp = args.udp;

    let test_id = uuid::Uuid::new_v4();
    let mut upload_config = prepare_upload_configuration(args, test_id)?;
    let download_config = prepare_download_configuration(args, test_id)?;

    //connect to the server
    let mut stream = connect_to_server(args.client.as_ref().unwrap(), args.port)?;
    let server_addr = stream.peer_addr()?;

    //scaffolding to track and relay the streams and stream-results associated with this test
    let stream_count = download_config.streams as usize;
    let mut parallel_streams: Vec<Arc<Mutex<(dyn TestRunner + Sync + Send)>>> = Vec::with_capacity(stream_count);
    let mut parallel_streams_joinhandles = Vec::with_capacity(stream_count);
    let (results_tx, results_rx) = channel::<IntervalResultBox>();

    let test_results: Mutex<Box<dyn TestResults + Send + Sync + 'static>> = prepare_test_results(is_udp, stream_count);

    //a closure used to pass results from stream-handlers to the test-result structure
    let mut results_handler = || -> BoxResult<()> {
        //drain all results every time this closer is invoked
        while let Ok(result) = results_rx.try_recv() {
            //see if there's a result to pass on
            if !display_json {
                //since this runs in the main thread, which isn't involved in any testing, render things immediately
                println!("{}", result.to_string(display_bit));
            }

            //update the test-results accordingly
            let mut tr = test_results.lock().unwrap();
            match result.kind() {
                IntervalResultKind::ClientDone | IntervalResultKind::ClientFailed => {
                    let stream_idx = result.get_stream_idx();
                    if result.kind() == IntervalResultKind::ClientDone {
                        log::info!("stream {} is done", stream_idx);
                    } else {
                        log::warn!("stream {} failed", stream_idx);
                    }
                    tr.mark_stream_done(stream_idx, result.kind() == IntervalResultKind::ClientDone);
                    if tr.count_in_progress_streams() == 0 {
                        complete = true;

                        if tr.count_in_progress_streams_server() > 0 {
                            log::info!("giving the server a few seconds to report results...");
                            start_kill_timer();
                        } else {
                            //all data gathered from both sides
                            kill();
                        }
                    }
                }
                _ => {
                    let msg = result.to_message();
                    tr.update_from_message(&msg)?;
                }
            }
        }
        Ok(())
    };

    //depending on whether this is a forward- or reverse-test, the order of configuring test-streams will differ
    if args.reverse {
        log::debug!("running in reverse-mode: server will be uploading data");

        //when we're receiving data, we're also responsible for letting the server know where to send it
        let mut stream_ports = Vec::with_capacity(stream_count);

        if is_udp {
            //UDP
            log::info!("preparing for reverse-UDP test with {} streams...", stream_count);

            let test_definition = udp::UdpTestDefinition::new(&download_config)?;
            for stream_idx in 0..stream_count {
                log::debug!("preparing UDP-receiver for stream {}...", stream_idx);
                let test = udp::receiver::UdpReceiver::new(
                    test_definition.clone(),
                    stream_idx,
                    &mut udp_port_pool,
                    server_addr.ip(),
                    *download_config.receive_buffer.as_ref().unwrap() as usize,
                )?;
                stream_ports.push(test.get_port()?);
                parallel_streams.push(Arc::new(Mutex::new(test)));
            }
        } else {
            //TCP
            log::info!("preparing for reverse-TCP test with {} streams...", stream_count);

            let test_definition = tcp::TcpTestDefinition::new(&download_config)?;
            for stream_idx in 0..stream_count {
                log::debug!("preparing TCP-receiver for stream {}...", stream_idx);
                let test = tcp::receiver::TcpReceiver::new(test_definition.clone(), stream_idx, &mut tcp_port_pool, server_addr.ip())?;
                stream_ports.push(test.get_port()?);
                parallel_streams.push(Arc::new(Mutex::new(test)));
            }
        }

        //add the port-list to the upload-config that the server will receive; this is in stream-index order
        upload_config.stream_ports = Some(stream_ports);

        let upload_config = Message::Configuration(upload_config.clone());

        //let the server know what we're expecting
        send_message(&mut stream, &upload_config)?;
    } else {
        if args.reverse_nat {
            log::debug!("running in reverse-NAT-mode: server will be receiving data");
            download_config.reverse_nat = Some(true);
        } else {
            log::debug!("running in forward-mode: server will be receiving data");
        }

        let download_config = Message::Configuration(download_config.clone());

        //let the server know to prepare for us to connect
        send_message(&mut stream, &download_config)?;
        //NOTE: we don't prepare to send data at this point; that happens in the loop below, after the server signals that it's ready
    }

    //now that the server knows what we need to do, we have to wait for its response
    let connection_payload = receive_message(&mut stream, is_alive, &mut results_handler)?;
    match connection_payload {
        Message::Connect { stream_ports } => {
            // we need to connect to the server
            if is_udp {
                // UDP
                log::info!("preparing for UDP test with {} streams...", stream_count);

                let test_definition = udp::UdpTestDefinition::new(&upload_config)?;
                for (stream_idx, &port) in stream_ports.iter().enumerate() {
                    log::debug!("preparing UDP-sender for stream {}...", stream_idx);
                    let test = udp::sender::UdpSender::new(
                        test_definition.clone(),
                        stream_idx,
                        0,
                        server_addr.ip(),
                        port,
                        *upload_config.duration.as_ref().unwrap_or(&0.0) as f32,
                        *upload_config.send_interval.as_ref().unwrap_or(&0.0) as f32,
                        *upload_config.send_buffer.as_ref().unwrap_or(&0) as usize,
                    )?;
                    parallel_streams.push(Arc::new(Mutex::new(test)));
                }
            } else {
                // TCP
                log::info!("preparing for TCP test with {} streams...", stream_count);

                let test_definition = tcp::TcpTestDefinition::new(&upload_config)?;
                for (stream_idx, &port) in stream_ports.iter().enumerate() {
                    log::debug!("preparing TCP-sender for stream {}...", stream_idx);
                    let test = tcp::sender::TcpSender::new(
                        test_definition.clone(),
                        stream_idx,
                        server_addr.ip(),
                        port,
                        *upload_config.duration.as_ref().unwrap_or(&0.0) as f32,
                        *upload_config.send_interval.as_ref().unwrap_or(&0.0) as f32,
                        *upload_config.send_buffer.as_ref().unwrap_or(&0) as usize,
                        *upload_config.no_delay.as_ref().unwrap_or(&false),
                    )?;
                    parallel_streams.push(Arc::new(Mutex::new(test)));
                }
            }
        }
        Message::ConnectReady => {
            //server is ready to connect to us, nothing more to do in this flow
            log::trace!("server is ready to connect to us");
        }
        _ => {
            let json = serde_json::to_string(&connection_payload)?;
            log::error!("invalid data from {}: {}", stream.peer_addr()?, json);
            kill();
        }
    }

    if is_alive() {
        //if interrupted while waiting for the server to respond, there's no reason to continue
        log::info!("informing server that testing can begin...");
        //tell the server to start
        send_message(&mut stream, &Message::Begin)?;

        log::debug!("spawning stream-threads");
        // begin the test-streams
        for parallel_stream in parallel_streams.iter_mut() {
            let stream_idx = parallel_stream.lock().unwrap().get_idx();
            log::info!("beginning execution of stream {}...", stream_idx);
            let c_ps = Arc::clone(parallel_stream);
            let c_results_tx = results_tx.clone();
            let c_cam = cpu_affinity_manager.clone();
            let handle = thread::spawn(move || {
                if let Err(e) = client_test_run_interval(c_ps, c_cam, c_results_tx) {
                    log::error!("error in parallel stream: {:?}", e);
                }
            });
            parallel_streams_joinhandles.push(handle);
        }

        //watch for events from the server
        while is_alive() {
            let msg = match receive_message(&mut stream, is_alive, &mut results_handler) {
                Ok(payload) => payload,
                Err(e) => {
                    if !complete {
                        // when complete, this also occurs
                        return Err(e);
                    }
                    break;
                }
            };

            match msg {
                Message::Receive(_) | Message::Send(_) => {
                    // receive/send-results from the server
                    if !display_json {
                        let result = interval_result_from_message(&msg)?;
                        println!("{}", result.to_string(display_bit));
                    }
                    let mut tr = test_results.lock().unwrap();
                    tr.update_from_message(&msg)?;
                }
                Message::Done(ref res) | Message::Failed(ref res) => {
                    //completion-result from the server
                    match res.stream_idx {
                        Some(idx64) => {
                            let mut tr = test_results.lock().unwrap();
                            match msg {
                                Message::Done(_) => {
                                    log::info!("server reported completion of stream {}", idx64);
                                }
                                Message::Failed(_) => {
                                    log::warn!("server reported failure with stream {}", idx64);
                                    tr.mark_stream_done(idx64, false);
                                }
                                _ => (), //not possible
                            }
                            tr.mark_stream_done_server(idx64);
                            if tr.count_in_progress_streams() == 0 && tr.count_in_progress_streams_server() == 0 {
                                //all data gathered from both sides
                                kill();
                            }
                        }
                        None => {
                            log::error!("completion from server did not include a valid stream_idx")
                        }
                    }
                }
                _ => {
                    let value = serde_json::to_value(msg)?;
                    let json = serde_json::to_string(&value)?;
                    log::error!("invalid data from {}: {}", stream.peer_addr()?, json);
                    break;
                }
            }
        }
    }

    //assume this is a controlled shutdown; if it isn't, this is just a very slight waste of time
    if let Err(e) = send_message(&mut stream, &Message::End) {
        log::trace!("unable to send end-message to server: {}", e);
    }
    thread::sleep(Duration::from_millis(250)); //wait a moment for the "end" message to be queued for delivery to the server
    if let Err(err) = stream.shutdown(Shutdown::Both) {
        log::trace!("unable to shutdown connection to server: {}", err);
    }

    log::debug!("stopping any still-in-progress streams");
    for ps in parallel_streams.iter_mut() {
        let mut stream = match (*ps).lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::error!("a stream-handler was poisoned; this indicates some sort of logic error");
                poisoned.into_inner()
            }
        };
        stream.stop();
    }
    log::debug!("waiting for all streams to end");
    for jh in parallel_streams_joinhandles {
        match jh.join() {
            Ok(_) => (),
            Err(e) => log::error!("error in parallel stream: {:?}", e),
        }
    }

    let mut upload_config = serde_json::to_value(Message::Configuration(upload_config))?;
    let mut download_config = serde_json::to_value(Message::Configuration(download_config))?;

    let common_config: serde_json::Value;
    //sanitise the config structures for export
    {
        let upload_config_map = upload_config.as_object_mut().unwrap();
        let cc_family = upload_config_map.remove("family");
        upload_config_map.remove("kind");
        let cc_length = upload_config_map.remove("length");
        upload_config_map.remove("role");
        let cc_streams = upload_config_map.remove("streams");
        upload_config_map.remove("test_id");
        upload_config_map.remove("stream_ports");
        if upload_config_map["send_buffer"].as_i64().unwrap() == 0 {
            upload_config_map.remove("send_buffer");
        }

        let download_config_map = download_config.as_object_mut().unwrap();
        download_config_map.remove("family");
        download_config_map.remove("kind");
        download_config_map.remove("length");
        download_config_map.remove("role");
        download_config_map.remove("streams");
        download_config_map.remove("test_id");
        if download_config_map["receive_buffer"].as_i64().unwrap() == 0 {
            download_config_map.remove("receive_buffer");
        }

        common_config = serde_json::json!({
            "family": cc_family,
            "length": cc_length,
            "streams": cc_streams,
        });
    }

    log::debug!("displaying test results");
    let omit_seconds: usize = args.omit;
    {
        let tr = test_results.lock().unwrap();
        if display_json {
            println!(
                "{}",
                tr.to_json_string(
                    omit_seconds,
                    upload_config,
                    download_config,
                    common_config,
                    serde_json::json!({
                        "omit_seconds": omit_seconds,
                        "ip_version": match server_addr.ip() {
                            IpAddr::V4(_) => 4,
                            IpAddr::V6(_) => 6,
                        },
                        "reverse": args.reverse,
                    })
                )
            );
        } else {
            println!("{}", tr.to_string(display_bit, omit_seconds));
        }
    }

    Ok(())
}

fn client_test_run_interval(
    c_ps: Arc<Mutex<dyn TestRunner + Send + Sync>>,
    c_cam: Arc<Mutex<CpuAffinityManager>>,
    c_results_tx: mpsc::Sender<IntervalResultBox>,
) -> BoxResult<()> {
    {
        // set CPU affinity, if enabled
        c_cam.lock().unwrap().set_affinity();
    }
    loop {
        let mut test = c_ps.lock().unwrap();
        let stream_idx = test.get_idx();
        log::debug!("beginning test-interval for stream {}", stream_idx);

        let interval_result = match test.run_interval() {
            Some(interval_result) => interval_result,
            None => {
                c_results_tx.send(Box::new(ClientDoneResult { stream_idx }))?;
                break;
            }
        };

        match interval_result {
            Ok(ir) => {
                c_results_tx.send(ir)?;
            }
            Err(e) => {
                log::error!("unable to process stream: {}", e);
                c_results_tx.send(Box::new(ClientFailedResult { stream_idx }))?;
                break;
            }
        }
    }
    Ok(())
}

pub fn kill() -> bool {
    ALIVE.swap(false, Ordering::Relaxed)
}
fn start_kill_timer() {
    unsafe {
        KILL_TIMER_RELATIVE_START_TIME = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    }
}
fn is_alive() -> bool {
    unsafe {
        if KILL_TIMER_RELATIVE_START_TIME != 0.0 {
            //initialised
            if SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64() - KILL_TIMER_RELATIVE_START_TIME >= KILL_TIMEOUT {
                return false;
            }
        }
    }
    ALIVE.load(Ordering::Relaxed)
}
