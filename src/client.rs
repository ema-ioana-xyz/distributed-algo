use crate::network_service::NetworkService;
use crate::protobuf::message::Type;
use crate::protobuf::ProcessId;
use crate::{protobuf, Envelope};
use std::net::{SocketAddr};
use std::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;
use crate::perfect_link_manager::PerfectLinkManager;
use crate::register_manager::RegisterManager;

pub struct Client {
    rx: Receiver<Envelope>,
    tx: Sender<Envelope>,
    own_port: u16,
    hub_socket: SocketAddr,
    nodes: Vec<ProcessId>,
    system_id: String,
    register_manager: RegisterManager,
    perfect_link_manager: PerfectLinkManager,
}

impl Client {
    pub fn new(rx: Receiver<Envelope>, tx: Sender<Envelope>, own_port: u16, hub_socket: SocketAddr) -> Self {
        let system_id = String::new();
        Client { rx, tx: tx.clone(), own_port, nodes: vec![],
            system_id, hub_socket, register_manager: RegisterManager::new(tx),
            perfect_link_manager: PerfectLinkManager::new(own_port)
        }
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
            Type::PlDeliver => {
                let res = self.perfect_link_manager.handle_pl_deliver(message);
                self.handle_message(res);
            },
            Type::PlSend => self.perfect_link_manager.handle_pl_send(message, &self.system_id),
            
            Type::ProcInitializeSystem => self.handle_proc_initialize_system(message),
            Type::BebBroadcast => self.do_beb_broadcast(message),
            Type::BebDeliver => self.handle_beb_deliver(message),
            Type::AppBroadcast => self.handle_app_broadcast(message),
            Type::AppValue => self.handle_app_broadcast_value(message),
            
            Type::NnarInternalAck | Type::NnarInternalRead | Type::NnarInternalValue |
            Type::NnarInternalWrite | Type::NnarRead | Type::NnarWrite=> {
                self.register_manager.handle_message(message);
            },
            
            Type::NnarReadReturn => {},
            Type::NnarWriteReturn => {},

            _ => {println!("Unknown message type received: {:?}", message)}
        }
    }
        
    fn handle_beb_deliver(&self, message: Envelope) {
        message.beb_deliver.unwrap().
        let inner = message.beb_deliver.unwrap().message.unwrap();
        let inner = *inner;
        self.tx.send(inner).unwrap();
    }

    fn do_beb_broadcast(&self, message: Envelope) {
        for node in &self.nodes {
            let mut pl_send_msg = protobuf::PlSend::default();
            pl_send_msg.message = NetworkService::wrap_envelope_contents(message.clone());
            pl_send_msg.destination = Option::from(node.clone());

            let mut wrapped_pl_send = Envelope::with_shipping_label(Type::PlSend);
            wrapped_pl_send.pl_send = NetworkService::wrap_envelope_contents(pl_send_msg);
            wrapped_pl_send.system_id = self.system_id.clone();
            wrapped_pl_send.to_abstraction_id = format!("{}.beb", message.to_abstraction_id);
            
            self.tx.send(wrapped_pl_send).unwrap()
        }
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

    fn handle_app_broadcast(&self, message: Envelope) {
        let value = message.app_broadcast.unwrap().value.unwrap();
        let value = Option::from(value);
        let value = protobuf::AppValue { value };

        let mut app_value_wrapper = Envelope::with_shipping_label(Type::AppValue);
        app_value_wrapper.app_value = Option::from(value);
        app_value_wrapper.to_abstraction_id = "app".to_string();

        self.do_beb_broadcast(app_value_wrapper);
    }

    fn handle_app_broadcast_value(&mut self, message: Envelope) {
        let mut pl_send = protobuf::PlSend::default();
        pl_send.message = NetworkService::wrap_envelope_contents(message);
        pl_send.destination = Option::from(ProcessId {
            host: self.hub_socket.ip().to_string(),
            port: self.hub_socket.port() as i32,
            ..Default::default()
        });
        let mut pl_send_wrapper = Envelope::with_shipping_label(Type::PlSend);
        pl_send_wrapper.pl_send = NetworkService::wrap_envelope_contents(pl_send);

        self.tx.send(pl_send_wrapper).unwrap();
    }
}

impl Envelope {
    pub fn with_shipping_label(message_type: Type) -> Self {
        let mut envelope = Envelope::default();
        envelope.set_type(message_type);
        envelope.message_uuid = Uuid::new_v4().to_string();
        envelope
    }
}