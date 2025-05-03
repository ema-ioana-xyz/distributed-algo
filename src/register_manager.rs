use std::collections::HashMap;
use std::sync::mpsc::Sender;
use regex::Regex;
use crate::{protobuf, Envelope};
use crate::protobuf::message::Type;
use crate::protobuf::ProcessId;

type RegisterValue = protobuf::Value;

pub struct RegisterManager {
    registers: HashMap<String, Register>,
    tx: Sender<Envelope>
}

// Handles all messages addressed to `app.nnar` and its subroutes
impl RegisterManager {
    pub fn new(tx: Sender<Envelope>) -> Self {
        RegisterManager {
            registers: HashMap::new(),
            tx
        }
    }

    pub fn handle_message(&mut self, message: Envelope) {
        // Forward to the right Register
        let regex = Regex::new(r"^.*\[(?register)].*$")
            .expect("Register regex should be valid");

        let destination = message.to_abstraction_id.clone();
        let register_name = match regex.captures(&destination).unwrap().name("register") {
            Some(val) => val.as_str(),
            None => panic!("Message not addressed to NNAR arrived in RegisterManager")
        };

        match self.registers.get(register_name) {
            Some(register) => register.handle_message(message),
            None => {
                self.registers.insert(
                    register_name.to_string(),
                    Register::new(self.tx.clone(), register_name)
                );
                self.registers.get(register_name).unwrap()
                    .handle_message(message);
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
    read_tracking_counter: usize,
    read_receipts: HashMap<ProcessId, RegisterValue>,
    am_i_reading: bool,

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
            read_tracking_counter: 0,
            read_receipts: HashMap::new(),
            am_i_reading: false,
            my_name: name.to_string(),
            tx
        }
    }
    
    pub fn handle_message(&mut self, message: Envelope) {
        match message.r#type() {
            Type::NnarRead => self.handle_nnar_read(message),
            Type::NnarInternalRead => self.handle_nnar_internal_read(message),
            Type::NnarWrite => self.handle_nnar_write(message),
            Type::NnarInternalWrite => self.handle_nnar_internal_write(message),
            
            _ => {}
        }
    }

    fn handle_nnar_read(&mut self, message: Envelope) {
        self.read_tracking_counter += 1;
        self.ack_count = 0;
        self.read_receipts.clear();
        self.am_i_reading = true;

        let payload = protobuf::NnarInternalRead {
            read_id: self.read_tracking_counter as i32
        };

        let mut beb_wrapper = Envelope::with_shipping_label(Type::BebBroadcast);
        beb_wrapper.nnar_internal_read = Option::from(payload);
        beb_wrapper.from_abstraction_id = format!("app.nnar[{}]", self.my_name);
        self.tx.send(beb_wrapper).unwrap();
    }
    
    fn handle_nnar_internal_read(&self, message: Envelope) {
        
    }
    
    fn handle_nnar_write(&self, message: Envelope) {
        
    }
    
    fn handle_nnar_internal_write(&self, message: Envelope) {
        
    }
}