use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use crate::Envelope;
use crate::network_service::NetworkService;
use crate::protobuf::ProcessId;

struct PlManager {
    pub most_recent_sender: ProcessId,
    system_id: String,
    own_port: u16
}

impl PlManager {
    pub fn handle_pl_deliver(&mut self, message: Envelope) {
        // Unwrap and recurse
        let inner = message.pl_deliver.unwrap().message.unwrap();
        let inner = *inner;
        self.handle_message(inner);
    }

    pub fn handle_pl_send(&self, message: Envelope) {
        let mut to_be_sent = message.clone();
        to_be_sent.to_abstraction_id = format!("{}.pl", message.to_abstraction_id);
        to_be_sent.system_id = self.system_id.clone();

        let destination_data = message.clone()
            .pl_send.unwrap()
            .destination.unwrap();
        let destination_ip: Ipv4Addr = destination_data.host.parse().unwrap();
        let destination_port = destination_data.port as u16;
        let destination_socket = SocketAddr::new(IpAddr::V4(destination_ip), destination_port);

        NetworkService::send(&destination_socket, to_be_sent, self.own_port);
    }
}