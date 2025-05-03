use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::Sender;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use uuid::Uuid;
use crate::{protobuf, Envelope};
use crate::protobuf::message::Type;


pub struct NetworkService {
}

impl NetworkService {
    pub fn start_listener(listening_socket: &SocketAddr, queue: Sender<Envelope>) -> JoinHandle<()> {
        // Open TCP Listener socket
        let server = TcpListener::bind(listening_socket).unwrap();
        let thread = thread::spawn(move || {
            for stream in server.incoming() {
                match stream {
                    Ok(mut stream) => {
                        Self::receive(&mut stream, queue.clone());
                    }
                    Err(e) => { eprintln!("Server connection accept failed; {}", e)}
                }
            }
        });
        thread
    }

    // Read a NetworkMessage over a TCP connection, and transform it into a PL message
    fn receive(connection: &mut TcpStream, queue: Sender<Envelope>) {
        let message_buffer = Self::read(connection);
        let envelope = match Envelope::decode(message_buffer) {
            Ok(val) => val,
            Err(err) => panic!("Failed to decode message from {:?}; {}", connection.peer_addr(), err)
        };

        if envelope.r#type() != Type::NetworkMessage {
            eprintln!("Received a message from the network that is not a NetworkMessage");
            return;
        }

        let net_msg = match envelope.network_message {
            Some(val) => val,
            None => panic!("Message from {:?} field NetworkMessage is not populated", connection.peer_addr())
        };

        let payload = match net_msg.message {
            Some(val) => val,
            None => panic!("Message from {:?} has a NetworkMessage but contains no inner message", connection.peer_addr())
        };

        let mut pl_deliver = protobuf::PlDeliver::default();
        pl_deliver.message = Option::from(payload);

        let mut to_be_added = Envelope::default();
        to_be_added.pl_deliver = Self::wrap_envelope_contents(pl_deliver);
        to_be_added.set_type(Type::PlDeliver);

        println!("Got message: {:?}", to_be_added);

        match queue.send(to_be_added) {
            Ok(_) => {},
            Err(err) => panic!(
                "[{:?}] Could not add received message to internal queue; {}",
                connection.local_addr(), err)
        };
    }

    /// Transform a PL message into a NetworkMessage, and send it over to a host via TCP
    pub fn send(destination: &SocketAddr, message: protobuf::Message, reply_port: u16) {
        // We implement a Perfect Link using TCP connections.
        // The specification requires we strip the outer Envelope and the PL_Send-layer message.
        let inner = message.pl_send
            .expect("Tried to send a Message that was not a PL_Send")
            .message.expect("Tried to send an empty PL_Send message");

        // Next, we must wrap the inner message in a NetworkMessage, and then an Envelope
        let mut network_message = protobuf::NetworkMessage::default();
        network_message.message = Option::from(Box::from(inner));
        network_message.sender_listening_port = reply_port as i32;
        network_message.sender_host = "127.0.0.1".to_string();

        let mut network_message_wrapper = Envelope::default();
        network_message_wrapper.set_type(Type::NetworkMessage);
        network_message_wrapper.network_message = Self::wrap_envelope_contents(network_message);
        network_message_wrapper.system_id = message.system_id;
        network_message_wrapper.to_abstraction_id = message.to_abstraction_id;
        network_message_wrapper.message_uuid = Uuid::new_v4().to_string();

        // Actually send the message
        Self::write(destination, &*network_message_wrapper.encode_to_vec())
    }
    fn write(destination: &SocketAddr, message: &[u8]) {
        let mut connection = match TcpStream::connect_timeout(
            destination,
            Duration::new(10, 0)
        ) {
            Ok(val) => val,
            Err(err) => panic!("Connecting to {} failed; {}", destination, err)
        };
        Self::write_message_length(&mut connection, message.len());
        connection.write_all(message).unwrap();
    }

    fn write_message_length(connection: &mut TcpStream, length: usize) {
        let mut message_length = vec![];
        message_length.write_u32::<NetworkEndian>(length as u32).expect("TODO: panic message");
        connection.write(&*message_length).expect("TODO: panic message");
    }

    fn read(connection: &mut TcpStream) -> Bytes {
        let length = Self::read_message_length(connection);
        let mut buffer  = BytesMut::zeroed(length as usize);

        match connection.read_exact(&mut *buffer) {
            Ok(_) => {},
            Err(err) => panic!(
                "Failed reading {} octets from {:?}; {}",
                length, connection.peer_addr(), err
            ),
        };
        buffer.freeze()
    }

    fn read_message_length(connection: &mut TcpStream) -> u32 {
        connection.read_i32::<NetworkEndian>().expect("TODO: yeah, handle this some day") as u32
    }

    pub fn wrap_envelope_contents<T>(contents: T) -> Option<Box<T>> {
        Option::from(Box::from(contents))
    }
}