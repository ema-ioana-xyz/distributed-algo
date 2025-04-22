use crate::network_service::NetworkService;
use crate::protobuf::message::Type;
use crate::protobuf::ProcessId;
use crate::{protobuf, Envelope};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;
use crate::register_manager::RegisterManager;

pub struct Client {
    rx: Receiver<Envelope>,
    tx: Sender<Envelope>,
    own_port: u16,
    hub_socket: SocketAddr,
    nodes: Vec<ProcessId>,
    system_id: String,
    register_mgr: RegisterManager,
}

impl Client {
    pub fn new(rx: Receiver<Envelope>, tx: Sender<Envelope>, own_port: u16, hub_socket: SocketAddr) -> Self {
        Client { rx, tx, own_port, nodes: vec![],
            system_id: String::new(), hub_socket, register_mgr: RegisterManager::new()
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
            Type::PlDeliver => self.handle_pl_deliver(message),
            Type::PlSend => self.handle_pl_send(message),
            Type::ProcInitializeSystem => self.handle_proc_initialize_system(message),
            Type::BebDeliver => self.handle_beb_deliver(message),
            Type::AppBroadcast => self.handle_app_broadcast(message),
            Type::AppValue => self.handle_app_broadcast_value(message),

            _ => {}
        }
    }


    fn handle_beb_deliver(&mut self, message: Envelope) {
        let inner = message.beb_deliver.unwrap().message.unwrap();
        let inner = *inner;
        self.handle_message(inner);
    }

    fn do_beb_broadcast(&mut self, message: Envelope) {
        for node in &self.nodes {
            let mut pl_send_msg = protobuf::PlSend::default();
            pl_send_msg.message = NetworkService::wrap_envelope_contents(message.clone());
            pl_send_msg.destination = Option::from(node.clone());

            let mut wrapped_pl_send = Envelope::default();
            wrapped_pl_send.pl_send = NetworkService::wrap_envelope_contents(pl_send_msg);
            wrapped_pl_send.system_id = self.system_id.clone();
            wrapped_pl_send.to_abstraction_id = format!("{}.beb", message.to_abstraction_id);
            wrapped_pl_send.set_type(Type::PlSend);
            wrapped_pl_send.message_uuid = Uuid::new_v4().to_string();

            self.handle_pl_send(wrapped_pl_send);
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

    fn handle_app_broadcast(&mut self, message: Envelope) {
        let value = message.app_broadcast.unwrap().value.unwrap();
        let value = Option::from(value);
        let value = protobuf::AppValue { value };

        let mut app_value_wrapper = Envelope::default();
        app_value_wrapper.app_value = Option::from(value);
        app_value_wrapper.to_abstraction_id = "app".to_string();
        app_value_wrapper.set_type(Type::AppValue);
        app_value_wrapper.message_uuid = Uuid::new_v4().to_string();

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
        let mut pl_send_wrapper = Envelope::default();
        pl_send_wrapper.pl_send = NetworkService::wrap_envelope_contents(pl_send);
        pl_send_wrapper.set_type(Type::PlSend);
        pl_send_wrapper.message_uuid = Uuid::new_v4().to_string();

        self.handle_pl_send(pl_send_wrapper);
    }
}