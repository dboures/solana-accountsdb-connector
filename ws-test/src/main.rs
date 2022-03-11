//! A chat server that broadcasts a message to all connections.
//!
//! This is a simple line-based server which accepts WebSocket connections,
//! reads lines from those connections, and broadcasts the lines to all other
//! connected clients.
//!
//! You can test this out by running:
//!
//!     cargo run --example server 127.0.0.1:12345
//!
//! And then in another window run:
//!
//!     cargo run --example client ws://127.0.0.1:12345/
//!
//! You can run the second command in multiple windows and then chat between the
//! two, seeing the messages from the other client as they're received. For all
//! connected clients they'll all join the same room and see everyone else's
//! messages.

use core::time;
use std::{
    cell::RefCell,
    collections::HashMap,
    env,
    io::Error as IoError,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use futures_channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, SinkExt, Stream, StreamExt};

use tokio::{
    net::{TcpListener, TcpStream},
    pin,
    sync::broadcast,
};
use tokio_tungstenite::tungstenite::protocol::Message;

type Rx = UnboundedReceiver<Message>;
type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;

async fn handle_connection(peer_map: PeerMap, raw_stream: TcpStream, addr: SocketAddr) {
    println!("Incoming TCP connection from: {}", addr);

    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    // Insert the write part of this peer to the peer map.
    let (tx, rx) = unbounded();

    peer_map.lock().unwrap().insert(addr, tx);

    let (outgoing, incoming) = ws_stream.split();

    // let broadcast_incoming = incoming.try_for_each(|msg| {
    //     println!(
    //         "Received a message from {}: {}",
    //         addr,
    //         msg.to_text().unwrap()
    //     );
    //     let peers = peer_map.lock().unwrap();

    //     // We want to broadcast the message to everyone except ourselves.
    //     let broadcast_recipients = peers
    //         .iter()
    //         .filter(|(peer_addr, _)| peer_addr != &&addr)
    //         .map(|(_, ws_sink)| ws_sink);

    //     for recp in broadcast_recipients {
    //         recp.unbounded_send(msg.clone()).unwrap();
    //     }

    //     future::ok(())
    // });

    let receive_from_others = rx.map(Ok).forward(outgoing);

    // pin_mut!(broadcast_incoming, receive_from_others);
    pin_mut!(receive_from_others);
    (receive_from_others).await;

    println!("{} disconnected", &addr);
    peer_map.lock().unwrap().remove(&addr);
}

#[tokio::main]
async fn main() -> Result<(), IoError> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let state = PeerMap::new(Mutex::new(HashMap::new()));

    // Create the event loop and TCP listener we'll accept connections on.
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind");
    println!("Listening on: {}", addr);



    

    let (sender, receiver) = async_channel::unbounded::<String>();

    let producer = tokio::spawn(async move {
        loop {
            println!("producer");
            tokio::time::sleep(time::Duration::from_secs(5)).await;
            sender.send("timer".to_string()).await.unwrap();
        }
    });

    let state_clone = state.clone();
    let broadcaster = tokio::spawn(async move {
        println!("bcaster");
        pin!(receiver);
        loop {
            let message = receiver.recv().await.unwrap();
            println!("bcast => {}", message);

            let mut state_clone3 = state_clone.lock().unwrap().clone();

            for (k, v) in state_clone3.iter_mut() {
                println!("  > {}", k);
                v.send(Message::Text(message.clone())).await.unwrap()
            }
        }
        println!("bcaster3");
    });

    // Let's spawn the handling of each connection in a separate task.
    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(handle_connection(state.clone(), stream, addr));
    }

    println!("NOT");

    Ok(())
}
