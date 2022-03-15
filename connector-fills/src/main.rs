use {
    log::*,
    solana_accountsdb_connector_lib::*,
    std::{fs::File, io::Read},
};

use core::time;
use std::{
    cell::RefCell,
    collections::HashMap,
    env,
    io::Error as IoError,
    mem::ManuallyDrop,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_util::{pin_mut, stream::TryStreamExt, SinkExt, Stream, StreamExt};

use solana_accountsdb_connector_lib::fill_event_filter::FillEventFilterMessage;
use tokio::{
    net::{TcpListener, TcpStream},
    pin,
    sync::broadcast,
};

use tokio_tungstenite::tungstenite::protocol::Message;

type PeerMap = Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>;

async fn handle_connection(peer_map: PeerMap, raw_stream: TcpStream, addr: SocketAddr) {
    info!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    // Insert the write part of this peer to the peer map.
    let (tx, rx) = unbounded();

    peer_map.lock().unwrap().insert(addr, tx);

    let (outgoing, incoming) = ws_stream.split();

    let receive_from_others = rx.map(Ok).forward(outgoing);

    // pin_mut!(broadcast_incoming, receive_from_others);
    pin_mut!(receive_from_others);
    (receive_from_others).await.unwrap();

    info!("{} disconnected", &addr);
    peer_map.lock().unwrap().remove(&addr);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        error!("requires a config file argument");
        return Ok(());
    }

    let config: Config = {
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        toml::from_str(&contents).unwrap()
    };

    solana_logger::setup_with_default("info");
    info!("startup");

    let metrics_tx = metrics::start();

    let (account_write_queue_sender, slot_queue_sender, fill_receiver) =
        fill_event_filter::init(config.markets.clone()).await?;

    let state = PeerMap::new(Mutex::new(HashMap::new()));

    let try_socket = TcpListener::bind(&config.bind_ws_addr).await;
    let listener = try_socket.expect("Failed to bind");
    info!("Listening on: {}", config.bind_ws_addr);

    let state_clone = state.clone();

    //breadcaster thread
    tokio::spawn(async move {
        pin!(fill_receiver);
        loop {
            let message = fill_receiver.recv().await.unwrap();
            match message {
                FillEventFilterMessage::Update(update) => {
                    info!("ws update {} {:?} fill", update.market, update.status);

                    let mut state_clone3 = state_clone.lock().unwrap().clone();

                    for (k, v) in state_clone3.iter_mut() {
                        trace!("  > {}", k);

                        let json = serde_json::to_string(&update);

                        v.send(Message::Text(json.unwrap())).await.unwrap()
                    }
                }
                FillEventFilterMessage::Checkpoint(checkpoint) => {
                    // let json = serde_json::to_string(&checkpoint.queue);
                    // println!("{}", json.unwrap());

                    //let queue = ManuallyDrop::take checkpoint);
                }
            }
        }
    });

    info!("open socket");
    tokio::spawn(async move {
        // Let's spawn the handling of each connection in a separate task.
        while let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(state.clone(), stream, addr));
        }
    });

    info!("connect to rpc");
    let use_accountsdb = true;
    if use_accountsdb {
        grpc_plugin_source::process_events(
            config,
            account_write_queue_sender,
            slot_queue_sender,
            metrics_tx,
        )
        .await;
    } else {
        websocket_source::process_events(config, account_write_queue_sender, slot_queue_sender)
            .await;
    }

    Ok(())
}
