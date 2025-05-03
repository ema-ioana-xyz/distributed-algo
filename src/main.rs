mod network_service;
mod client;
mod register_manager;
mod perfect_link_manager;

use std::{env, thread};
use std::net::SocketAddr;
use std::sync::mpsc::channel;
use network_service::NetworkService;
use uuid::Uuid;
use crate::client::Client;
use crate::protobuf::message::Type;

type Envelope = protobuf::Message;

pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/protobuf.rs"));
}

struct Options {
    hub_address: SocketAddr,
    own_addresses: Vec<SocketAddr>
}

fn main() {
    let options = set_config();
    println!("Hub address is: {}", options.hub_address);

    let mut server_threads = Vec::with_capacity(3);
    let mut client_threads = Vec::with_capacity(3);

    for index in 0..3 {
        let node_socket = options.own_addresses[index];

        // Create message queue for current node
        let (tx, rx)  = channel();

        let server_thread = NetworkService::start_listener(
            &options.own_addresses[index], tx.clone()
        );
        server_threads.push(server_thread);

        // Start client for current node
        let mut client = Client::new(rx, tx, node_socket.port(), options.hub_address);
        let client_thread = thread::spawn(move || {
            client.start_worker()
        });
        client_threads.push(client_thread);

        // Register the new node with the Hub
        let connection_message = make_connection_message((index + 1) as u8, &options.hub_address);
        NetworkService::send(&options.hub_address, connection_message, node_socket.port());
    }

    // Join server threads before exiting
    for thread in server_threads {
        thread.join().expect("Joining server threads with the main one should not cause a panic");
    }
    for thread in client_threads {
        thread.join().expect("Joining client threads with the main one should not cause a panic");
    }
}

fn make_connection_message(index: u8, destination: &SocketAddr) -> Envelope {
    let mut register_message = protobuf::ProcRegistration::default();
    register_message.owner = "uwu".to_string();
    register_message.index = index as i32;

    let mut register_wrapper = Envelope::default();
    register_wrapper.proc_registration = Option::from(register_message);
    register_wrapper.set_type(Type::ProcRegistration);
    register_wrapper.to_abstraction_id = "app".to_string();
    register_wrapper.message_uuid = Uuid::new_v4().to_string();

    let pl_destination = protobuf::ProcessId {
        host: destination.ip().to_string(),
        port: destination.port() as i32,
        owner: "ref".to_string(),
        ..Default::default()
    };
    let mut pl_send_msg = protobuf::PlSend::default();
    pl_send_msg.message = Option::from(Box::from(register_wrapper));
    pl_send_msg.destination = Option::from(pl_destination);


    let mut pl_wrapper = protobuf::Message::default();
    pl_wrapper.set_type(Type::PlSend);
    pl_wrapper.from_abstraction_id = "app.pl".to_string();
    pl_wrapper.to_abstraction_id = "app.pl".to_string();
    pl_wrapper.pl_send = Option::from(Box::from(pl_send_msg));
    pl_wrapper.system_id = "uwu".to_string();
    pl_wrapper.message_uuid = Uuid::new_v4().to_string();
    pl_wrapper
}
fn show_usage_info() {
    println!("Usage");
    println!("dp-algo <Hub IP address>:<Hub port> <Node-1 IP>:<Node-1 port> <Node-2 IP>:<Node-2 port> <Node-3 IP>:<Node-3 port>");
}

fn set_config() -> Options {
    fn failure_message(message: &str) -> String {
        let failure =
            format!(
                "The provided arguments are not valid\
                Reason: {}",
                message);
        show_usage_info();
        failure
    }

    let args = env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        panic!("{}", failure_message("Not enough arguments"));
    }

    let hub_address: SocketAddr = match args.get(1).unwrap().parse() {
        Ok(val) => val,
        Err(err) => panic!("{} {}", failure_message("Invalid hub IP-port pair"), err.to_string())
    };

    let mut own_addresses= Vec::with_capacity(3);
    for index in 0..3 {
        let address= match args.get(2 + index) {
            Some(val) => val,
            None => panic!("Missing own IP-port pair number {}", index)
        };

        let address = match address.parse() {
            Ok(val) => val,
            Err(err) => panic!("{} {}", failure_message("Invalid own IP-port pair"), err)
        };
        own_addresses.push(address);
    }


    Options {
        hub_address,
        own_addresses,
    }
}
