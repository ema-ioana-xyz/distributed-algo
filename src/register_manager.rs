use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::mpsc::Sender;
use regex::Regex;
use crate::{protobuf, Envelope};
use crate::broadcast_manager::BroadcastManager;
use crate::client::ClientState;
use crate::network_service::NetworkService;
use crate::perfect_link_manager::PerfectLinkManager;
use crate::protobuf::message::Type;
use crate::protobuf::{NnarInternalValue, ProcessId};

type RegisterValue = protobuf::Value;

pub struct RegisterManager {
    registers: HashMap<String, Register>,
    tx: Sender<Envelope>,
}


// Handles all messages addressed to `app.nnar` and its subroutes
impl RegisterManager {
    pub fn new(tx: Sender<Envelope>) -> Self {
        RegisterManager {
            registers: HashMap::new(),
            tx
        }
    }

    pub fn handle_message(&mut self, message: Envelope, client_state: ClientState) {
        // Forward to the right Register
        let regex = Regex::new(r"^.*\[(?<register>.*)].*$")
            .expect("Register regex should be valid");

        let destination = message.to_abstraction_id.clone();
        let register_name = match regex.captures(&destination).unwrap().name("register") {
            Some(val) => val.as_str(),
            None => panic!("Message not addressed to NNAR arrived in RegisterManager")
        };

        match self.registers.get_mut(register_name) {
            Some(register) => register.handle_message(message, client_state),
            None => {
                self.registers.insert(
                    register_name.to_string(),
                    Register::new(self.tx.clone(), register_name)
                );
                self.registers.get_mut(register_name).unwrap()
                    .handle_message(message, client_state);
            }
        };
    }
}

struct Register {
    timestamp: usize,
    writer_rank: usize,
    value: RegisterValue,
    ack_count: usize,
    my_value_for_writing: RegisterValue,
    my_value_for_reading: RegisterValue,
    read_tracking_counter: usize,
    read_receipts: HashMap<ProcessId, NnarInternalValue>,
    am_i_reading: bool,
    reply_to: ProcessId,

    my_name: String,

    tx: Sender<Envelope>
}

impl Register {
    fn new(tx: Sender<Envelope>, name: &str) -> Self {
        Self {
            timestamp: 0,
            writer_rank: 0,
            value: RegisterValue {defined: false, v: 0},
            ack_count: 0,
            my_value_for_writing: RegisterValue {defined: false, v: 0},
            my_value_for_reading: RegisterValue {defined: false, v: 0},
            read_tracking_counter: 0,
            read_receipts: HashMap::new(),
            am_i_reading: false,
            my_name: name.to_string(),
            reply_to: ProcessId::default(),
            tx
        }
    }
    
    pub fn handle_message(&mut self, message: Envelope, client_state: ClientState) {
        match message.r#type() {
            Type::NnarRead => self.handle_nnar_read(client_state),
            Type::NnarInternalRead => self.handle_nnar_internal_read(message, client_state),
            Type::NnarWrite => self.handle_nnar_write(message, client_state),
            Type::NnarInternalWrite => self.handle_nnar_internal_write(message, client_state),
            Type::NnarInternalValue => self.handle_nnar_internal_value(message, client_state),
            Type::NnarInternalAck => self.handle_nnar_internal_ack(message, client_state),
            Type::NnarWriteReturn => self.handle_nnar_write_return(client_state),
            Type::NnarReadReturn => self.handle_nnar_read_return(message, client_state),
            

            Type::BebDeliver => self.unwrap_beb(message, client_state),
            Type::BebBroadcast => BroadcastManager::do_beb_broadcast(message, &self.tx, &client_state.nodes, &*client_state.system_id),
            Type::PlDeliver => self.unwrap_pl(message, client_state),
            Type::PlSend => PerfectLinkManager::handle_pl_send(message, &client_state.system_id, client_state.own_port),
            
            _ => {println!("Register '{}' got an unknown message type: {:?}", self.my_name, message)}
        }
    }

    fn unwrap_beb(&mut self, message: Envelope, client_state: ClientState) {
        let beb_deliver = message.beb_deliver.unwrap();
        self.reply_to = beb_deliver.sender.unwrap().clone();
        self.handle_message(*beb_deliver.message.unwrap(), client_state);
    }

    fn unwrap_pl(&mut self, message: Envelope, client_state: ClientState) {
        let pl_deliver = message.pl_deliver.unwrap();
        self.reply_to = pl_deliver.sender.unwrap().clone();
        self.handle_message(*pl_deliver.message.unwrap(), client_state);
    }

    fn handle_nnar_read(&mut self, client_state: ClientState) {
        self.read_tracking_counter += 1;
        self.ack_count = 0;
        self.read_receipts.clear();
        self.am_i_reading = true;

        let payload = protobuf::NnarInternalRead {
            read_id: self.read_tracking_counter as i32
        };

        let mut beb_wrapper = Envelope::with_shipping_label(Type::NnarInternalRead);
        beb_wrapper.nnar_internal_read = Option::from(payload);
        beb_wrapper.from_abstraction_id = format!("app.nnar[{}]", self.my_name);
        beb_wrapper.to_abstraction_id = format!("app.nnar[{}]", self.my_name);
        BroadcastManager::do_beb_broadcast(beb_wrapper, &self.tx, &client_state.nodes, &client_state.system_id);
    }
    
    fn handle_nnar_internal_read(&self, message: Envelope, client_state: ClientState) {
        let read_command = message.nnar_internal_read.unwrap();

        let mut value = protobuf::NnarInternalValue::default();
        value.writer_rank = self.writer_rank as i32;
        value.timestamp = self.timestamp as i32;
        value.read_id = read_command.read_id;
        value.value = Option::from(self.value);

        let mut value_wrapper = Envelope::with_shipping_label(Type::NnarInternalValue);
        value_wrapper.nnar_internal_value = Option::from(value);

        let mut pl_send = protobuf::PlSend::default();
        pl_send.message = NetworkService::wrap_envelope_contents(value_wrapper);
        pl_send.destination = Option::from(self.reply_to.clone());

        let mut pl_send_wrapper = Envelope::with_shipping_label(Type::PlSend);
        pl_send_wrapper.to_abstraction_id = format!("app.nnar[{}]", self.my_name);
        pl_send_wrapper.pl_send = NetworkService::wrap_envelope_contents(pl_send);

        PerfectLinkManager::handle_pl_send(
            pl_send_wrapper, &client_state.system_id, client_state.own_port
        );
    }

    fn handle_nnar_internal_value(&mut self, message: Envelope, client_state: ClientState) {
        let read_value = message.nnar_internal_value.unwrap();

        if read_value.read_id < self.read_tracking_counter as i32 {
            return;
        } else if read_value.read_id > self.read_tracking_counter as i32 {
            panic!("NNAR received a Value for a Read that has not yet happened");
        }

        self.read_receipts.insert(self.reply_to.clone(), read_value);

        if self.read_receipts.len() <= (client_state.nodes.len() / 2) {
            return;
        }

        let mut max_timestamp = -1;
        let mut max_writer_rank = -1;
        let mut max_value = RegisterValue::default();
        for receipt in self.read_receipts.values() {
            if receipt.timestamp > max_timestamp {
                max_timestamp = receipt.timestamp;
                max_writer_rank = receipt.writer_rank;
                max_value = receipt.value.unwrap();
            } else if receipt.timestamp == max_timestamp && receipt.writer_rank > max_writer_rank {
                max_timestamp = receipt.timestamp;
                max_writer_rank = receipt.writer_rank;
                max_value = receipt.value.unwrap();
            }
        }
        self.my_value_for_reading = max_value;

        self.read_receipts.clear();

        let mut payload = protobuf::NnarInternalWrite::default();
        payload.read_id = self.read_tracking_counter as i32;

        if self.am_i_reading {
            payload.timestamp = max_timestamp;
            payload.writer_rank = max_writer_rank;
            payload.value = Option::from(self.my_value_for_reading);
        } else {
            payload.timestamp = max_timestamp + 1;
            payload.writer_rank = client_state.rank;
            payload.value = Option::from(self.my_value_for_writing);
        }

        let mut wrapper = Envelope::with_shipping_label(Type::NnarInternalWrite);
        wrapper.nnar_internal_write = Option::from(payload);
        wrapper.from_abstraction_id = format!("app.nnar[{}]", self.my_name);
        wrapper.to_abstraction_id = format!("app.nnar[{}]", self.my_name);
        BroadcastManager::do_beb_broadcast(wrapper, &self.tx, &client_state.nodes, &client_state.system_id);
    }


    fn handle_nnar_write(&mut self, message: Envelope, client_state: ClientState) {
        let nnar_write_command = message.nnar_write.unwrap();

        self.read_tracking_counter += 1;
        self.my_value_for_writing = nnar_write_command.value.unwrap();
        self.ack_count = 0;
        self.read_receipts.clear();

        let payload = protobuf::NnarInternalRead {
            read_id: self.read_tracking_counter as i32
        };

        let mut wrapper = Envelope::with_shipping_label(Type::NnarInternalRead);
        wrapper.nnar_internal_read = Option::from(payload);
        wrapper.from_abstraction_id = format!("app.nnar[{}]", self.my_name);
        wrapper.to_abstraction_id = format!("app.nnar[{}]", self.my_name);
        BroadcastManager::do_beb_broadcast(wrapper, &self.tx, &client_state.nodes, &client_state.system_id)
    }
    
    fn handle_nnar_internal_write(&mut self, message: Envelope, client_state: ClientState) {
        let nnar_internal_write = message.nnar_internal_write.unwrap();
        let my_ts = self.timestamp as i32;
        let my_wr = self.writer_rank as i32;

        if (nnar_internal_write.timestamp > my_ts) ||
            (nnar_internal_write.timestamp == my_ts && nnar_internal_write.writer_rank > my_wr) {
            self.timestamp = nnar_internal_write.timestamp as usize;
            self.writer_rank = nnar_internal_write.writer_rank as usize;
            self.value = nnar_internal_write.value.unwrap();
        }

        let mut ack = protobuf::NnarInternalAck::default();
        ack.read_id = nnar_internal_write.read_id;
        let mut ack_wrapper = Envelope::with_shipping_label(Type::NnarInternalAck);
        ack_wrapper.nnar_internal_ack = Option::from(ack);
        ack_wrapper.to_abstraction_id = format!("app.nnar[{}]", self.my_name);

        let mut pl_send = protobuf::PlSend::default();
        pl_send.destination = Option::from(self.reply_to.clone());
        pl_send.message = NetworkService::wrap_envelope_contents(ack_wrapper);
        let mut pl_send_wrapper = Envelope::with_shipping_label(Type::PlSend);
        pl_send_wrapper.pl_send = NetworkService::wrap_envelope_contents(pl_send);
        pl_send_wrapper.to_abstraction_id = format!("app.nnar[{}]", self.my_name);

        PerfectLinkManager::handle_pl_send(pl_send_wrapper, &client_state.system_id, client_state.own_port);
    }

    fn handle_nnar_internal_ack(&mut self, message: Envelope, client_state: ClientState) {
        let ack = message.nnar_internal_ack.unwrap();

        if ack.read_id < self.read_tracking_counter as i32 {
            return;
        } else if ack.read_id > self.read_tracking_counter as i32 {
            panic!("NNAR received an Ack for a Read that has not yet happened");
        }

        self.ack_count += 1;

        if self.ack_count <= (client_state.nodes.len() / 2) { return }

        self.ack_count = 0;
        if self.am_i_reading {
            self.am_i_reading = false;
            
            let mut read_return = protobuf::NnarReadReturn::default();
            read_return.value = Option::from(self.my_value_for_reading.clone());
            
            let mut wrapper = Envelope::with_shipping_label(Type::NnarReadReturn);
            wrapper.nnar_read_return = Option::from(read_return);
            
            // self.tx.send(wrapper).unwrap();
            self.handle_nnar_read_return(wrapper, client_state);
        } else {
            // let write_return = protobuf::NnarWriteReturn::default();

            // let mut wrapper = Envelope::with_shipping_label(Type::NnarWriteReturn);
            // wrapper.nnar_write_return = Option::from(write_return);

            self.handle_nnar_write_return(client_state);
            // self.tx.send(wrapper).unwrap();
        }
    }
    
    fn handle_nnar_read_return(&self, message: Envelope, client_state: ClientState) {
        let nnar_read_return = message.nnar_read_return.unwrap();
        
        let mut app_read_return = protobuf::AppReadReturn::default();
        app_read_return.value = nnar_read_return.value;
        app_read_return.register = self.my_name.clone();
        
        let mut app_wrapper = Envelope::with_shipping_label(Type::AppReadReturn);
        app_wrapper.app_read_return = Option::from(app_read_return);
        
        let mut pl_send = protobuf::PlSend::default();
        pl_send.message = NetworkService::wrap_envelope_contents(app_wrapper);
        pl_send.destination = Option::from(ProcessId {
            host: client_state.hub_socket.ip().to_string(),
            port: client_state.hub_socket.port() as i32,
            ..Default::default()
        });
        
        let mut pl_wrapper = Envelope::with_shipping_label(Type::PlSend);
        pl_wrapper.pl_send = NetworkService::wrap_envelope_contents(pl_send);
        
        self.tx.send(pl_wrapper).unwrap();
    }
    
    fn handle_nnar_write_return(&self, client_state: ClientState) {
        let mut app_write_return = protobuf::AppWriteReturn::default();
        app_write_return.register = self.my_name.clone();

        let mut app_wrapper = Envelope::with_shipping_label(Type::AppWriteReturn);
        app_wrapper.app_write_return = Option::from(app_write_return);

        let mut pl_send = protobuf::PlSend::default();
        pl_send.message = NetworkService::wrap_envelope_contents(app_wrapper);
        pl_send.destination = Option::from(ProcessId {
            host: client_state.hub_socket.ip().to_string(),
            port: client_state.hub_socket.port() as i32,
            ..Default::default()
        });

        let mut pl_wrapper = Envelope::with_shipping_label(Type::PlSend);
        pl_wrapper.pl_send = NetworkService::wrap_envelope_contents(pl_send);

        self.tx.send(pl_wrapper).unwrap();
    }
}

impl Eq for ProcessId {}
impl Hash for ProcessId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host.hash(state);
        self.port.hash(state);
        self.owner.hash(state);
        self.index.hash(state);
        self.rank.hash(state);
    }
}