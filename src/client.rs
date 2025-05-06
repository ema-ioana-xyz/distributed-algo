use crate::network_service::NetworkService;
use crate::protobuf::message::Type;
use crate::protobuf::ProcessId;
use crate::{protobuf, Envelope};
use std::net::{SocketAddr};
use std::sync::mpsc::{Receiver, Sender};
use uuid::Uuid;
use crate::broadcast_manager::BroadcastManager;
use crate::perfect_link_manager::PerfectLinkManager;
use crate::register_manager::RegisterManager;

pub struct Client {
    rx: Receiver<Envelope>,
    tx: Sender<Envelope>,
    own_port: u16,
    hub_socket: SocketAddr,
    nodes: Vec<ProcessId>,
    system_id: String,
    rank: i32,
    register_manager: RegisterManager,
}

pub struct ClientState {
    pub own_port: u16,
    pub hub_socket: SocketAddr,
    pub nodes: Vec<ProcessId>,
    pub system_id: String,
    pub rank: i32,
}

impl Client {
    pub fn new(rx: Receiver<Envelope>, tx: Sender<Envelope>, own_port: u16, hub_socket: SocketAddr) -> Self {
        let system_id = String::new();
        Client { rx, tx: tx.clone(), own_port, nodes: vec![], system_id, hub_socket, rank: -1,
            register_manager: RegisterManager::new(tx.clone()),
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
    
    pub fn clone_state(&self) -> ClientState {
        ClientState {
            own_port: self.own_port,
            hub_socket: self.hub_socket,
            nodes: self.nodes.clone(),
            system_id: self.system_id.clone(),
            rank: self.rank
        }
    }

    fn handle_message(&mut self, message: Envelope) {
        if message.to_abstraction_id.starts_with("app.nnar") {
            self.register_manager.handle_message(message, self.clone_state());
            return
        }
        
        match message.r#type() {
            Type::PlDeliver => {
                let res = PerfectLinkManager::handle_pl_deliver(message);
                self.handle_message(res);
            },
            Type::PlSend => PerfectLinkManager::handle_pl_send(message, &self.system_id, self.own_port),
            
            Type::ProcInitializeSystem => self.handle_proc_initialize_system(message),
            Type::BebBroadcast => BroadcastManager::do_beb_broadcast(message, &self.tx, &self.nodes, &self.system_id),
            Type::BebDeliver => BroadcastManager::handle_beb_deliver(message, &self.tx),
            Type::AppBroadcast => self.handle_app_broadcast(message),
            Type::AppValue => self.handle_app_broadcast_value(message),
            Type::AppRead => self.handle_app_read(message),
            Type::AppWrite => self.handle_app_write(message),
            
            _ => {
                println!("Unknown message type received: {:?}", message)
            }
        }
    }
        
    fn handle_proc_initialize_system(&mut self, message: Envelope) {
        let init_message = message.proc_initialize_system.unwrap();
        self.system_id = message.system_id;
        self.nodes = init_message.processes;

        self.rank = self.nodes.iter()
            .filter(|node| {node.port == self.own_port as i32})
            .map(|node| {node.rank})
            .next()
            .expect("One of the nodes should be the one with this client's listening port");

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

        BroadcastManager::do_beb_broadcast(app_value_wrapper, &self.tx, &self.nodes, &self.system_id);
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
    
    fn handle_app_read(&self, message: Envelope) {
        let app_read = message.app_read.unwrap();
        let nnar_read = protobuf::NnarRead::default();
        let mut nnar_wrapper = Envelope::with_shipping_label(Type::NnarRead);
        nnar_wrapper.nnar_read = Option::from(nnar_read);
        nnar_wrapper.to_abstraction_id = format!("app.nnar[{}]", app_read.register);
        
        self.tx.send(nnar_wrapper).unwrap()
    }

    fn handle_app_write(&self, message: Envelope) {
        let app_write = message.app_write.unwrap();
        let mut nnar_write = protobuf::NnarWrite::default();
        nnar_write.value = app_write.value;

        let mut nnar_wrapper = Envelope::with_shipping_label(Type::NnarWrite);
        nnar_wrapper.nnar_write = Option::from(nnar_write);
        nnar_wrapper.to_abstraction_id = format!("app.nnar[{}]", app_write.register);

        self.tx.send(nnar_wrapper).unwrap();
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