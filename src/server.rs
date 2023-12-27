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

use std::io;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::args::Args;
use crate::protocol::communication::{receive_message, send_message};
use crate::protocol::messaging::{prepare_connect, Message};
use crate::protocol::results::{IntervalResultBox, ServerDoneResult, ServerFailedResult};
use crate::stream::{tcp, udp, TestRunner};
use crate::utils::cpu_affinity::CpuAffinityManager;
use crate::BoxResult;

const POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// when false, the system is shutting down
static ALIVE: AtomicBool = AtomicBool::new(true);

/// a count of connected clients
static CLIENTS: AtomicU16 = AtomicU16::new(0);

fn handle_client(
    stream: &mut TcpStream,
    cpu_affinity_manager: Arc<Mutex<CpuAffinityManager>>,
    tcp_port_pool: Arc<Mutex<tcp::receiver::TcpPortPool>>,
    udp_port_pool: Arc<Mutex<udp::receiver::UdpPortPool>>,
) -> BoxResult<()> {
    let mut started = false;
    let peer_addr = stream.peer_addr()?;

    //scaffolding to track and relay the streams and stream-results associated with this client
    let mut parallel_streams: Vec<Arc<Mutex<(dyn TestRunner + Sync + Send)>>> = Vec::new();
    let mut parallel_streams_joinhandles = Vec::new();

    let (results_tx, results_rx) = mpsc::channel::<IntervalResultBox>();

    //a closure used to pass results from stream-handlers to the client-communication stream
    let mut forwarding_send_stream = stream.try_clone()?;
    let mut results_handler = || -> BoxResult<()> {
        // drain all results every time this closer is invoked
        while let Ok(result) = results_rx.try_recv() {
            // if there's something to forward, write it to the client
            send_message(&mut forwarding_send_stream, &result.to_message())?;
        }
        Ok(())
    };

    //server operations are entirely driven by client-signalling, making this a (simple) state-machine
    while is_alive() {
        let msg = receive_message(stream, is_alive, &mut results_handler)?;

        match &msg {
            Message::Configuration(cfg) => {
                //we either need to connect streams to the client or prepare to receive connections
                if cfg.role == "download" {
                    log::info!("[{}] running in forward-mode: server will be receiving data", &peer_addr);

                    let stream_count = cfg.streams;
                    //since we're receiving data, we're also responsible for letting the client know where to send it
                    let mut stream_ports = Vec::with_capacity(stream_count);

                    if cfg.family.as_deref() == Some("udp") {
                        log::info!("[{}] preparing for UDP test with {} streams...", &peer_addr, stream_count);

                        let mut c_udp_port_pool = udp_port_pool.lock().unwrap();

                        for stream_idx in 0..stream_count {
                            log::debug!("[{}] preparing UDP-receiver for stream {}...", &peer_addr, stream_idx);
                            let test = udp::receiver::UdpReceiver::new(
                                cfg,
                                stream_idx,
                                &mut c_udp_port_pool,
                                peer_addr.ip(),
                                cfg.receive_buffer.unwrap_or(0) as usize,
                            )?;
                            stream_ports.push(test.get_port()?);
                            parallel_streams.push(Arc::new(Mutex::new(test)));
                        }
                    } else {
                        //TCP
                        log::info!("[{}] preparing for TCP test with {} streams...", &peer_addr, stream_count);

                        let mut c_tcp_port_pool = tcp_port_pool.lock().unwrap();

                        for stream_idx in 0..stream_count {
                            log::debug!("[{}] preparing TCP-receiver for stream {}...", &peer_addr, stream_idx);
                            let test = tcp::receiver::TcpReceiver::new(cfg, stream_idx, &mut c_tcp_port_pool, peer_addr.ip())?;
                            stream_ports.push(test.get_port()?);
                            parallel_streams.push(Arc::new(Mutex::new(test)));
                        }
                    }

                    //let the client know we're ready to receive the connection; stream-ports are in stream-index order
                    send_message(stream, &prepare_connect(&stream_ports))?;
                } else {
                    //upload
                    log::info!("[{}] running in reverse-mode: server will be uploading data", &peer_addr);

                    let dummy = Vec::new();
                    let stream_ports = cfg.stream_ports.as_ref().unwrap_or(&dummy);

                    if cfg.family.as_deref() == Some("udp") {
                        log::info!("[{}] preparing for UDP test with {} streams...", &peer_addr, stream_ports.len());

                        for (stream_idx, &port) in stream_ports.iter().enumerate() {
                            log::debug!("[{}] preparing UDP-sender for stream {}...", &peer_addr, stream_idx);
                            let test = udp::sender::UdpSender::new(
                                cfg,
                                stream_idx,
                                0,
                                peer_addr.ip(),
                                port,
                                *cfg.duration.as_ref().unwrap_or(&0.0),
                                *cfg.send_interval.as_ref().unwrap_or(&1.0),
                                *cfg.send_buffer.as_ref().unwrap_or(&0) as usize,
                            )?;
                            parallel_streams.push(Arc::new(Mutex::new(test)));
                        }
                    } else {
                        //TCP
                        log::info!("[{}] preparing for TCP test with {} streams...", &peer_addr, stream_ports.len());

                        for (stream_idx, &port) in stream_ports.iter().enumerate() {
                            log::debug!("[{}] preparing TCP-sender for stream {}...", &peer_addr, stream_idx);
                            let test = tcp::sender::TcpSender::new(
                                cfg,
                                stream_idx,
                                peer_addr.ip(),
                                port,
                                *cfg.duration.as_ref().unwrap_or(&0.0),
                                *cfg.send_interval.as_ref().unwrap_or(&1.0),
                                *cfg.send_buffer.as_ref().unwrap_or(&0) as usize,
                                *cfg.no_delay.as_ref().unwrap_or(&false),
                            )?;
                            parallel_streams.push(Arc::new(Mutex::new(test)));
                        }
                    }

                    //let the client know we're ready to begin
                    send_message(stream, &Message::ConnectReady)?;
                }
            }
            Message::Begin => {
                //the client has indicated that testing can begin
                if !started {
                    //a simple guard to protect against reinitialisaion
                    for parallel_stream in parallel_streams.iter_mut() {
                        let stream_idx = parallel_stream.lock().unwrap().get_idx();
                        log::info!("[{}] beginning execution of stream {}...", &peer_addr, stream_idx);
                        let c_ps = Arc::clone(parallel_stream);
                        let c_results_tx = results_tx.clone();
                        let c_cam = cpu_affinity_manager.clone();
                        let handle = thread::spawn(move || {
                            if let Err(e) = test_run_interval(c_cam, c_ps, peer_addr, c_results_tx) {
                                log::error!("[{}] error in parallel stream: {}", &peer_addr, e);
                            }
                        });
                        parallel_streams_joinhandles.push(handle);
                    }
                    started = true;
                } else {
                    //this can only happen in case of malicious action
                    log::error!("[{}] duplicate begin-signal", &peer_addr);
                    break;
                }
            }
            Message::End => {
                //the client has indicated that testing is done; stop cleanly
                log::info!("[{}] end of testing signaled", &peer_addr);
                break;
            }
            _ => {
                log::error!("[{}] invalid data", &peer_addr);
                break;
            }
        }
    }

    log::debug!("[{}] stopping any still-in-progress streams", &peer_addr);
    for ps in parallel_streams.iter_mut() {
        let mut test_stream = match (*ps).lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                log::error!(
                    "[{}] a stream-handler was poisoned; this indicates some sort of logic error",
                    &peer_addr
                );
                poisoned.into_inner()
            }
        };
        test_stream.stop();
    }
    log::debug!("[{}] waiting for all streams to end", &peer_addr);
    for jh in parallel_streams_joinhandles {
        match jh.join() {
            Ok(_) => (),
            Err(e) => log::error!("[{}] error in parallel stream: {:?}", &peer_addr, e),
        }
    }

    Ok(())
}

fn test_run_interval(
    c_cam: Arc<Mutex<CpuAffinityManager>>,
    c_ps: Arc<Mutex<dyn TestRunner + Send + Sync>>,
    peer_addr: SocketAddr,
    c_results_tx: mpsc::Sender<IntervalResultBox>,
) -> BoxResult<()> {
    {
        c_cam.lock().unwrap().set_affinity(); // set CPU affinity, if enabled
    }
    loop {
        let mut test = c_ps.lock().unwrap();
        let stream_idx = test.get_idx();
        log::debug!("[{}] beginning test-interval for stream {}", &peer_addr, stream_idx);
        let interval_result = match test.run_interval() {
            Some(interval_result) => interval_result,
            None => {
                log::trace!("[{}] stream {} has finished", &peer_addr, stream_idx);
                c_results_tx.send(Box::new(ServerDoneResult { stream_idx }))?;
                break;
            }
        };
        match interval_result {
            Ok(ir) => {
                c_results_tx.send(ir)?;
            }
            Err(e) => {
                log::error!("[{}] unable to process stream: {}", peer_addr, e);
                c_results_tx.send(Box::new(ServerFailedResult { stream_idx }))?;
                break;
            }
        }
    }
    Ok(())
}

/// a panic-tolerant means of indicating that a client has been disconnected
struct ClientThreadMonitor {
    client_address: String,
}
impl Drop for ClientThreadMonitor {
    fn drop(&mut self) {
        CLIENTS.fetch_sub(1, Ordering::Relaxed);
        if thread::panicking() {
            log::warn!("[{}] disconnecting due to panic", self.client_address);
        } else {
            log::info!("[{}] disconnected", self.client_address);
        }
    }
}

pub fn serve(args: &Args) -> BoxResult<()> {
    //config-parsing and pre-connection setup
    let tcp_port_pool = Arc::new(Mutex::new(tcp::receiver::TcpPortPool::new(
        &args.tcp_port_pool,
        &args.tcp6_port_pool,
    )));
    let udp_port_pool = Arc::new(Mutex::new(udp::receiver::UdpPortPool::new(
        &args.udp_port_pool,
        &args.udp6_port_pool,
    )));

    let cpu_affinity_manager = Arc::new(Mutex::new(CpuAffinityManager::new(&args.affinity)?));

    let client_limit: u16 = args.client_limit as u16;
    if client_limit > 0 {
        log::debug!("limiting service to {} concurrent clients", client_limit);
    }

    //start listening for connections
    let port: u16 = args.port;
    let listener = TcpListener::bind(SocketAddr::new(args.bind, port))?;
    listener.set_nonblocking(true)?;
    log::info!("server listening on {}", listener.local_addr()?);

    while is_alive() {
        let (mut stream, address) = match listener.accept() {
            Ok((stream, address)) => (stream, address),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // no pending clients
                thread::sleep(POLL_TIMEOUT);
                continue;
            }
            Err(e) => {
                return Err(Box::new(e));
            }
        };

        log::info!("connection from {}", address);

        stream.set_nodelay(true)?;

        #[cfg(unix)]
        {
            use crate::protocol::communication::KEEPALIVE_DURATION;
            let keepalive_parameters = socket2::TcpKeepalive::new().with_time(KEEPALIVE_DURATION);
            let raw_socket = socket2::SockRef::from(&stream);
            raw_socket.set_tcp_keepalive(&keepalive_parameters)?;
        }

        let client_count = CLIENTS.fetch_add(1, Ordering::Relaxed) + 1;
        if client_limit > 0 && client_count > client_limit {
            log::warn!("client-limit ({}) reached; disconnecting {}...", client_limit, address.to_string());
            if let Err(e) = stream.shutdown(Shutdown::Both) {
                log::trace!("error disconnecting [{}]: {}", address.to_string(), e);
            }
            CLIENTS.fetch_sub(1, Ordering::Relaxed);
        } else {
            let c_cam = cpu_affinity_manager.clone();
            let c_tcp_port_pool = tcp_port_pool.clone();
            let c_udp_port_pool = udp_port_pool.clone();
            let thread_builder = thread::Builder::new().name(address.to_string());
            thread_builder.spawn(move || {
                // ensure the client is accounted-for even if the handler panics
                let _client_thread_monitor = ClientThreadMonitor {
                    client_address: address.to_string(),
                };

                match handle_client(&mut stream, c_cam, c_tcp_port_pool, c_udp_port_pool) {
                    Ok(_) => (),
                    Err(e) => log::error!("error in client-handler: {}", e),
                }

                //in the event of panic, this will happen when the stream is dropped
                if let Err(e) = stream.shutdown(Shutdown::Both) {
                    log::trace!("error disconnecting [{}]: {}", address.to_string(), e);
                }
            })?;
        }
    }

    //wait until all clients have been disconnected
    loop {
        let clients_count = CLIENTS.load(Ordering::Relaxed);
        if clients_count > 0 {
            log::info!("waiting for {} clients to finish...", clients_count);
            thread::sleep(POLL_TIMEOUT);
        } else {
            break;
        }
    }
    Ok(())
}

pub fn kill() -> bool {
    ALIVE.swap(false, Ordering::Relaxed)
}
fn is_alive() -> bool {
    ALIVE.load(Ordering::Relaxed)
}
