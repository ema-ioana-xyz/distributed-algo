use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use crate::Envelope;
use crate::network_service::NetworkService;
use crate::protobuf::ProcessId;

pub struct PerfectLinkManager {
    most_recent_sender: ProcessId,
    own_port: u16
}

impl PerfectLinkManager {
    pub fn new(my_port: u16) -> Self {
        PerfectLinkManager {
            most_recent_sender: ProcessId::default(),
            own_port: my_port
        }
    }
    
    pub fn get_most_recent_sender(&self) -> &ProcessId {
        &self.most_recent_sender
    }

    pub fn handle_pl_deliver(&mut self, message: Envelope) -> Envelope {
        let inner = message.pl_deliver.unwrap().message.unwrap();
        *inner
    }

    pub fn handle_pl_send(&self, message: Envelope, my_system_id: &str) {
        let mut to_be_sent = message.clone();
        to_be_sent.to_abstraction_id = format!("{}.pl", message.to_abstraction_id);
        to_be_sent.system_id = my_system_id.to_string();

        let destination_data = message.clone()
            .pl_send.unwrap()
            .destination.unwrap();
        let destination_ip: Ipv4Addr = destination_data.host.parse().unwrap();
        let destination_port = destination_data.port as u16;
        let destination_socket = SocketAddr::new(IpAddr::V4(destination_ip), destination_port);

        NetworkService::send(&destination_socket, to_be_sent, self.own_port);
    }
}