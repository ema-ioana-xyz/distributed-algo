use crate::{protobuf, Envelope};
use crate::network_service::NetworkService;
use crate::protobuf::message::Type;
use std::sync::mpsc::Sender;
use crate::protobuf::ProcessId;

pub struct BroadcastManager {
}

impl BroadcastManager {
    pub fn handle_beb_deliver(message: Envelope, tx: &Sender<Envelope>) {
        let inner = message.beb_deliver.unwrap().message.unwrap();
        let inner = *inner;
        tx.send(inner).unwrap();
    }

    pub fn do_beb_broadcast(message: Envelope, tx: &Sender<Envelope>, nodes: &Vec<ProcessId>, system_id: &str) {
        for node in nodes {
            let mut pl_send_msg = protobuf::PlSend::default();
            pl_send_msg.message = NetworkService::wrap_envelope_contents(message.clone());
            pl_send_msg.destination = Option::from(node.clone());

            let mut wrapped_pl_send = Envelope::with_shipping_label(Type::PlSend);
            wrapped_pl_send.pl_send = NetworkService::wrap_envelope_contents(pl_send_msg);
            wrapped_pl_send.system_id = system_id.to_string();
            wrapped_pl_send.to_abstraction_id = format!("{}.beb", message.to_abstraction_id);

            tx.send(wrapped_pl_send).unwrap()
        }
    }
}