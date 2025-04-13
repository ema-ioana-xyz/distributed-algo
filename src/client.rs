use crate::network_service::NetworkService;
use crate::protobuf::message::Type;
use crate::protobuf::ProcessId;
use crate::Envelope;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::mpsc::{Receiver, Sender};

pub struct Client {
    rx: Receiver<Envelope>,
    tx: Sender<Envelope>,
    own_port: u16,
    nodes: Vec<ProcessId>,
    system_id: String
}

impl Client {
    pub fn new(rx: Receiver<Envelope>, tx: Sender<Envelope>, own_port: u16) -> Self {
        Client { rx, tx, own_port, nodes: vec![], system_id: String::new()}
    }

    pub fn start_worker(&mut self) {
        loop {
            match self.rx.recv() {
                Ok(msg) => Self::handle_message(self, msg),
                Err(err) => panic!("{}", err)
            }
        };
    }

    fn handle_message(&mut self, message: Envelope) {
        match message.r#type() {
            Type::PlDeliver => self.handle_pl_deliver(message),
            Type::PlSend => self.handle_pl_send(message),
            Type::ProcInitializeSystem => self.handle_proc_initialize_system(message),
            Type::BebDeliver => self.handle_beb_deliver(message),

            _ => {}
        }
    }

    fn handle_pl_deliver(&mut self, message: Envelope) {
        // Unwrap and recurse
        let inner = message.pl_deliver.unwrap().message.unwrap();
        let inner = *inner;
        self.handle_message(inner);
    }

    fn handle_pl_send(&self, message: Envelope) {
        let destination_data = message.clone()
            .pl_send.unwrap()
            .destination.unwrap();
        let destination_ip: Ipv4Addr = destination_data.host.parse().unwrap();
        let destination_port = destination_data.port as u16;
        let destination_socket = SocketAddr::new(IpAddr::V4(destination_ip), destination_port);
        NetworkService::send(&destination_socket, message, self.own_port);
    }

    fn handle_beb_deliver(&mut self, message: Envelope) {
        let inner = message.beb_deliver.unwrap().message.unwrap();
        let inner = *inner;
        self.handle_message(inner);
    }

    fn handle_proc_initialize_system(&mut self, message: Envelope) {
        let init_message = message.proc_initialize_system.unwrap();
        self.system_id = message.system_id;
        self.nodes = init_message.processes;

        println!("[Port {}] Got a list of the system's nodes: ", self.own_port);
        for process in &self.nodes {
            println!("[{}] {:?}", self.own_port, process);
        }
    }
}