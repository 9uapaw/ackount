use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use crate::transaction::{DisputeState, Transaction};

/// Represents a client entry in the storage
#[derive(Clone)]
pub struct Client {
    pub available: f32,
    pub held: f32,
    pub total: f32,
    pub locked: bool,
}

impl Client {
    /// Creates a client with an initial zeroed state
    pub fn new() -> Self {
        Client { available: 0f32, total: 0f32, held: 0f32, locked: false }
    }
}

/// An in-memory storage that stores client and transaction information in HashMaps.
pub struct InMemoryAccountStorage {
    clients: HashMap<u16, Client>,
    disputes: HashMap<u16, HashMap<u32, DisputeState>>,
    transactions: HashMap<u32, Transaction>,
}

impl Display for Client {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.4}, {:.4}, {:.4}, {}", self.available, self.held, self.total, self.locked)
    }
}

impl AccountStorage for InMemoryAccountStorage {
    fn get_clients(&self) -> Iter<u16, Client> {
        self.clients.iter()
    }

    fn get_client(&self, client_id: u16) -> Option<&Client> {
        self.clients.get(&client_id)
    }

    fn get_dispute(&self, client_id: u16, tx_id: u32) -> Option<&DisputeState> {
        self.disputes.get(&client_id).and_then(|txs| txs.get(&tx_id))
    }

    fn get_transaction(&self, tx_id: u32) -> Option<&Transaction> {
        self.transactions.get(&tx_id)
    }

    fn update_client(&mut self, client_id: u16, updated_client: Client) {
        self.clients.insert(client_id, updated_client);
    }

    fn update_dispute(&mut self, client_id: u16, tx_id: u32, new_state: DisputeState) {
        if let Some(txs) = self.disputes.get_mut(&client_id) {
            txs.insert(tx_id, new_state);
        } else {
            self.disputes.insert(client_id, HashMap::new());
            self.disputes.get_mut(&client_id).unwrap().insert(tx_id, new_state);
        }
    }

    fn add_transaction(&mut self, tx_id: u32, transaction: Transaction) {
        self.transactions.insert(tx_id, transaction);
    }
}

impl InMemoryAccountStorage {
    pub fn new() -> Self {
        InMemoryAccountStorage { clients: HashMap::new(), disputes: HashMap::new(), transactions: HashMap::new() }
    }
}

/// Interface to indicate that a struct is able to hold information about clients and transactions
pub trait AccountStorage {
    fn get_clients(&self) -> Iter<u16, Client>;
    fn get_client(&self, client_id: u16) -> Option<&Client>;
    fn get_dispute(&self, client_id: u16, tx_id: u32) -> Option<&DisputeState>;
    fn get_transaction(&self, tx_id: u32) -> Option<&Transaction>;
    fn update_client(&mut self, client_id: u16, updated_client: Client);
    fn update_dispute(&mut self, client_id: u16, tx_id: u32, new_state: DisputeState);
    fn add_transaction(&mut self, tx_id: u32, transaction: Transaction);
}
