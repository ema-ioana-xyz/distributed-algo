mod network_service;
mod messaging;

use std::collections::VecDeque;
use std::env;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use network_service::NetworkService;
use uuid::Uuid;
use crate::protobuf::message::Type;

type Envelope = protobuf::Message;

pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/protobuf.rs"));
}

struct Node {
    address: SocketAddr
}

struct Options {
    hub_address: SocketAddr,
    own_address: SocketAddr
}

fn main() {
    let options = set_config();
    println!("Hub address is: {}", options.hub_address);

    let message_queue: VecDeque<Envelope> = VecDeque::new();
    let queue_arc = Arc::from(Mutex::from(message_queue));
    let server_thread = NetworkService::start_listener(&options.own_address, queue_arc);

    NetworkService::send(&options.hub_address, base_envelope, options.own_address.port());
    
    server_thread.join().expect("TODO: panic message");
    }

fn make_connection_message() -> Envelope {
    let mut process_registration_msg = protobuf::ProcRegistration::default();
    process_registration_msg.owner = "uwu".to_string();
    process_registration_msg.index = 1;
    let mut pr_envelope = protobuf::Message::default();
    pr_envelope.proc_registration = Option::from(process_registration_msg);
    pr_envelope.to_abstraction_id = "app".to_string();
    pr_envelope.message_uuid = Uuid::new_v4().to_string();

    let mut pl_send_msg = protobuf::PlSend::default();
    pl_send_msg.message = Option::from(Box::from(pr_envelope));


    let mut base_envelope = protobuf::Message::default();
    base_envelope.set_type(Type::PlSend);
    base_envelope.from_abstraction_id = "app.pl".to_string();
    base_envelope.to_abstraction_id = "app.pl".to_string();
    base_envelope.pl_send = Option::from(Box::from(pl_send_msg));
    base_envelope.system_id = "uwu".to_string();
    base_envelope.message_uuid = Uuid::new_v4().to_string();

}
fn show_usage_info() {
    println!("Usage");
    println!("dp-algo <Hub IP address>:<Hub port>");
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

    let own_address: SocketAddr = match args.get(2).unwrap().parse() {
        Ok(val) => val,
        Err(err) => panic!("{} {}", failure_message("Invalid own IP-port pair"), err.to_string())
    };

    Options {
        hub_address,
        own_address,
    }
}
