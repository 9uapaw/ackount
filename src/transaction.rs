use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

use csv_async::{DeserializeRecordsStream};
use futures_core::Stream;
use serde::Deserialize;
use tokio::fs::File;

pub type Result<T> = std::result::Result<T, TransactionError>;

/// Represents an error when processing or reading a transaction
#[derive(Debug)]
pub enum TransactionError {
    MalformedTransaction(String)
}

impl Display for TransactionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedTransaction(s) => write!(f, "Error deserializing transaction {}", s),
        }
    }
}

/// A stream that encapsulates a CSV file by fetching its content line by line
pub struct TransactionCSVStream<'a> {
    stream: Pin<Box<DeserializeRecordsStream<'a, File, Transaction>>>,
}

impl<'a> TransactionCSVStream<'a> {
    pub fn new(stream: DeserializeRecordsStream<'a, File, Transaction>) -> Self {
        TransactionCSVStream { stream: Pin::new(Box::new(stream)) }
    }
}

impl<'a> Stream for TransactionCSVStream<'a> {
    type Item = Result<Transaction>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx).map_err(|e| { TransactionError::MalformedTransaction(e.to_string()) })
    }
}

/// Type of a transactional action
#[derive(Clone, Debug, Deserialize, Copy)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// Represents a transaction event
#[derive(Clone, Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub(crate) transaction_type: TransactionType,
    pub(crate) client: u16,
    pub(crate) tx: u32,
    pub(crate) amount: Option<f32>,
}

/// Represents an actual state of a transaction dispute
pub enum DisputeState {
    Dispute,
    Resolve,
    ChargeBack,
}
