use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use prost::bytes::{Bytes, BytesMut};
use prost::Message;
use crate::{protobuf, Envelope};
use crate::protobuf::message::Type;


pub struct NetworkService {
}
type MessageQueue = Arc<Mutex<VecDeque<Envelope>>>;

impl NetworkService {
    pub fn start_listener(listening_socket: &SocketAddr, queue: MessageQueue) -> JoinHandle<()> {
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
    fn receive(connection: &mut TcpStream, queue: MessageQueue) {
        let message_buffer = Self::read(connection);
        let envelope = protobuf::Message::decode(message_buffer).unwrap();

        if envelope.r#type() != Type::NetworkMessage {
            eprintln!("Received a message from the network that is not a NetworkMessage");
            return;
        }

        let payload = envelope.network_message.unwrap().message.unwrap();
        let mut pl_deliver = protobuf::PlDeliver::default();
        pl_deliver.message = Option::from(payload);

        let mut to_be_added = Envelope::default();
        to_be_added.pl_deliver = Self::wrap_envelope_contents(pl_deliver);
        to_be_added.set_type(Type::PlDeliver);

        println!("Got message: {}", to_be_added.message_uuid);

        queue.lock().unwrap().push_back(to_be_added);
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

        let mut to_be_sent = protobuf::Message::default();
        to_be_sent.set_type(Type::NetworkMessage);
        to_be_sent.network_message = Self::wrap_envelope_contents(network_message);
        to_be_sent.system_id = message.system_id;
        to_be_sent.to_abstraction_id = message.to_abstraction_id;

        // Actually send the message
        Self::write(destination, &*to_be_sent.encode_to_vec())
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
        let mut buffer  = BytesMut::with_capacity(length as usize);
        connection.read_exact(&mut *buffer).expect("TODO: panic message");
        buffer.freeze()
    }

    fn read_message_length(connection: &mut TcpStream) -> u32 {
        connection.read_u32::<NetworkEndian>().expect("TODO: yeah, handle this some day")
    }

    fn wrap_envelope_contents<T>(contents: T) -> Option<Box<T>> {
        Option::from(Box::from(contents))
    }
}