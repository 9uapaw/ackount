use csv_async::{AsyncReaderBuilder, Trim};
use futures_core::Stream;
use tokio::fs::File;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

use crate::storage::{AccountStorage, Client, InMemoryAccountStorage};
use crate::transaction;
use crate::transaction::{DisputeState, Transaction, TransactionCSVStream, TransactionType};

/// Processes a CSV file that contains valid transactions
///
///  # Arguments
///
/// * `path` - A string path to the file to be processed
pub async fn process_csv_file(path: &str) -> TransactionProcessor<InMemoryAccountStorage> {
    let f = File::open(path).await.expect("File not found");
    let mut rdr = AsyncReaderBuilder::new().flexible(true).trim(Trim::All).create_deserializer(f);
    let stream = rdr.deserialize::<Transaction>();
    let stream = TransactionCSVStream::new(stream);

    let processor = TransactionProcessor::new(InMemoryAccountStorage::new());
    processor.process_stream(stream).await;
    processor
}

/// Encapsulates the driving logic of processing, validating transactions and executing
/// the corresponding calculation with regards to client accounts
pub struct TransactionProcessor<T> where T: AccountStorage {
    pub storage: Mutex<T>,
}

impl<T> TransactionProcessor<T> where T: AccountStorage {
    pub fn new(storage: T) -> Self {
        TransactionProcessor { storage: Mutex::new(storage) }
    }

    /// Processes a stream containing transactions. The transaction is validated by the corresponding
    /// client account and the processing is executed by taking the transaction type into consideration.
    ///
    ///  # Arguments
    ///  * `s` - The stream to be processed
    pub async fn process_stream<S>(&self, mut s: S) where S: Stream<Item=transaction::Result<Transaction>> + Unpin {
        while let Some(t) = s.next().await {
            match &t {
                Ok(transaction) => self.process_transaction(&transaction).await,
                Err(_error) => {
                    // eprintln!("Error processing transaction {}", _error)
                }
            }
        }
    }

    async fn process_transaction(&self, transaction: &Transaction) {
        let mut storage = self.storage.lock().await;
        let mut client = storage.get_client(transaction.client).cloned();
        if client.is_none() {
            client.replace(Client::new());
            storage.update_client(transaction.client, client.as_ref().unwrap().clone());
        }
        drop(storage);

        let client = client.unwrap();
        if client.locked {
            return;
        }

        match transaction.transaction_type {
            TransactionType::Deposit => self.handle_deposit(transaction, &client).await,
            TransactionType::Withdrawal => self.handle_withdrawal(transaction, &client).await,
            TransactionType::Dispute => self.handle_dispute(transaction, &client).await,
            TransactionType::Resolve => self.handle_resolve(transaction, &client).await,
            TransactionType::Chargeback => self.handle_chargeback(transaction, &client).await,
        }
    }

    async fn handle_deposit(&self, transaction: &Transaction, client: &Client) {
        let mut storage = self.storage.lock().await;
        let existing_tx = storage.get_transaction(transaction.tx);
        if existing_tx.is_some() {
            return;
        }

        let mut updated_client = client.clone();
        let amount = transaction.amount.expect("Deposit should have an amount");
        updated_client.available += amount;
        updated_client.total += amount;

        storage.update_client(transaction.client, updated_client);

        storage.add_transaction(transaction.tx, transaction.clone())
    }

    async fn handle_withdrawal(&self, transaction: &Transaction, client: &Client) {
        let mut storage = self.storage.lock().await;
        let amount = transaction.amount.expect("Withdrawal should have an amount");
        let existing_tx = storage.get_transaction(transaction.tx);
        if client.available < amount || client.total < amount || existing_tx.is_some() {
            return;
        }

        let mut updated_client = client.clone();
        updated_client.available -= amount;
        updated_client.total -= amount;

        storage.update_client(transaction.client, updated_client);

        storage.add_transaction(transaction.tx, transaction.clone())
    }

    async fn handle_dispute(&self, transaction: &Transaction, client: &Client) {
        let mut storage = self.storage.lock().await;
        // A transaction could be disputed multiple times even after resolving
        let dispute_state = storage.get_dispute(transaction.client, transaction.tx).or(Some(&DisputeState::Resolve));
        let stored_transaction = storage.get_transaction(transaction.tx);
        let transaction_type = stored_transaction.map(|tx| tx.transaction_type);
        match (dispute_state, stored_transaction, transaction_type) {
            (Some(&DisputeState::Resolve), Some(tx), Some(TransactionType::Deposit)) => {
                let amount = tx.amount.expect(&format!("Transaction {} should have an amount field", tx.tx));
                if client.available < amount {
                    return;
                }
                let mut updated_client = client.clone();
                updated_client.available -= amount;
                updated_client.held += amount;
                storage.update_client(transaction.client, updated_client);
                storage.update_dispute(transaction.client, transaction.tx, DisputeState::Dispute);
            }
            _ => {}
        }
    }

    async fn handle_resolve(&self, transaction: &Transaction, client: &Client) {
        let mut storage = self.storage.lock().await;
        let dispute_state = storage.get_dispute(transaction.client, transaction.tx);
        let stored_transaction = storage.get_transaction(transaction.tx);
        let transaction_type = stored_transaction.map(|tx| tx.transaction_type);
        match (dispute_state, stored_transaction, transaction_type) {
            (Some(DisputeState::Dispute), Some(tx), Some(TransactionType::Deposit)) => {
                let amount = tx.amount.expect(&format!("Transaction {} should have an amount field", tx.tx));
                let mut updated_client = client.clone();
                updated_client.available += amount;
                updated_client.held -= amount;
                storage.update_client(transaction.client, updated_client);
                storage.update_dispute(transaction.client, transaction.tx, DisputeState::Resolve);
            }
            _ => {}
        }
    }

    async fn handle_chargeback(&self, transaction: &Transaction, client: &Client) {
        let mut storage = self.storage.lock().await;

        let dispute_state = storage.get_dispute(transaction.client, transaction.tx);
        let stored_transaction = storage.get_transaction(transaction.tx);
        let transaction_type = stored_transaction.map(|tx| tx.transaction_type);
        match (dispute_state, stored_transaction, transaction_type) {
            (Some(DisputeState::Dispute), Some(tx), Some(TransactionType::Deposit)) => {
                let amount = tx.amount.expect(&format!("Transaction {} should have an amount field", tx.tx));
                let mut updated_client = client.clone();
                updated_client.total -= amount;
                updated_client.held -= amount;
                updated_client.locked = true;
                storage.update_client(transaction.client, updated_client);
                storage.update_dispute(transaction.client, transaction.tx, DisputeState::ChargeBack);
            }
            _ => {}
        }
    }
}

mod tests {
    use std::pin::Pin;
    use std::time::Duration;

    use assert_approx_eq::assert_approx_eq;
    use async_stream::stream;
    use futures_core::Stream;
    use tokio::join;
    use tokio::time::{Instant, interval};

    use crate::processor::TransactionProcessor;
    use crate::storage::{AccountStorage, InMemoryAccountStorage};
    use crate::transaction::{Transaction, TransactionError, TransactionType};

    #[tokio::test]
    async fn test_stream_processing() {
        let storage = InMemoryAccountStorage::new();
        let processor = TransactionProcessor::new(storage);
        let transactions = vec![
            Ok(Transaction { transaction_type: TransactionType::Deposit, client: 1, tx: 1, amount: Some(3.6) }),
            Ok(Transaction { transaction_type: TransactionType::Deposit, client: 1, tx: 2, amount: Some(10.6) }),
            Ok(Transaction { transaction_type: TransactionType::Deposit, client: 1, tx: 3, amount: Some(4.6) }),
            Ok(Transaction { transaction_type: TransactionType::Withdrawal, client: 1, tx: 5, amount: Some(9.6) }),
            Ok(Transaction { transaction_type: TransactionType::Deposit, client: 2, tx: 4, amount: Some(4.6) }),
            Ok(Transaction { transaction_type: TransactionType::Deposit, client: 2, tx: 10, amount: Some(4.6) }),
        ];

        let test_stream = Box::pin(stream! {
            for t in transactions {
                yield t;
            }
        });

        processor.process_stream(test_stream).await;
        let storage = processor.storage.lock().await;

        let client_1 = storage.get_client(1);
        assert!(client_1.is_some(), "Client is not added to the storage");
        assert_approx_eq!(9.2, client_1.unwrap().available, 10e-4);
        assert_approx_eq!(9.2, client_1.unwrap().total, 10e-4);

        let client_2 = storage.get_client(2);
        assert!(client_2.is_some(), "Client is not added to the storage");
        assert_approx_eq!(9.2, client_2.unwrap().available, 10e-4);
        assert_approx_eq!(9.2, client_2.unwrap().total, 10e-4);
    }

    #[tokio::test]
    async fn test_stream_processor_not_blocking_executor() {
        let storage = InMemoryAccountStorage::new();
        let processor = TransactionProcessor::new(storage);

        let test_stream = create_test_stream();
        let test_stream_2 = create_test_stream();
        let test_stream_3 = create_test_stream();

        let time_before = Instant::now();
        join!(processor.process_stream(test_stream), processor.process_stream(test_stream_2), processor.process_stream(test_stream_3));
        let time_after = Instant::now();
        let elapsed = time_after - time_before;

        // Checks whether we block the single threaded executor. If the streams were processed sequentially,
        // the elapsed time would be ~7.5 sec, while concurrently it should execute in ~2.5 sec.
        assert!(elapsed.as_millis() < 3000);
    }

    fn create_test_stream() -> Pin<Box<dyn Stream<Item = Result<Transaction, TransactionError>>>> {
        let test_stream = Box::pin(stream! {
            let mut interval = interval(Duration::from_millis(500));
            for _ in 0..5 {
                yield Ok(Transaction{transaction_type: TransactionType::Deposit, client: 2, tx: 2, amount: Some(4.6)});
                interval.tick().await;
            }
        });
        test_stream
    }

    #[tokio::test]
    async fn test_deposit_withdrawal() {
        let storage = InMemoryAccountStorage::new();
        let processor = TransactionProcessor::new(storage);
        let transaction_1 = Transaction { transaction_type: TransactionType::Deposit, client: 1, tx: 1, amount: Some(325.5) };
        let transaction_2 = Transaction { transaction_type: TransactionType::Deposit, client: 2, tx: 2, amount: Some(5.0) };
        let transaction_3 = Transaction { transaction_type: TransactionType::Deposit, client: 2, tx: 3, amount: Some(1000.0) };
        let transaction_4 = Transaction { transaction_type: TransactionType::Withdrawal, client: 2, tx: 4, amount: Some(500.0) };
        let transaction_5 = Transaction { transaction_type: TransactionType::Withdrawal, client: 1, tx: 5, amount: Some(125.5) };

        processor.process_transaction(&transaction_1).await;
        processor.process_transaction(&transaction_2).await;

        let mut storage_lock = processor.storage.lock().await;
        let client_2 = storage_lock.get_client(2);
        assert!(client_2.is_some(), "Client is not added to the storage");

        let mut updated_client = client_2.unwrap().clone();
        updated_client.locked = true;
        storage_lock.update_client(2, updated_client);
        drop(storage_lock);

        processor.process_transaction(&transaction_3).await;
        processor.process_transaction(&transaction_4).await;
        processor.process_transaction(&transaction_5).await;

        let mut storage_lock = processor.storage.lock().await;
        let client_2 = storage_lock.get_client(2);
        let client_1 = storage_lock.get_client(1);
        assert!(client_1.is_some(), "Client is not added to the storage");

        // Tests whether deposit and withdrawal was successful
        assert_approx_eq!(200.0, client_1.unwrap().total, 10e-4);
        assert_approx_eq!(200.0, client_1.unwrap().available, 10e-4);
        // Tests whether locking the account prevents further deposits/withdrawals
        assert_approx_eq!(5.0, client_2.unwrap().total, 10e-4);
        assert_approx_eq!(5.0, client_2.unwrap().available, 10e-4);

        let mut updated_client = client_2.unwrap().clone();
        updated_client.locked = false;
        storage_lock.update_client(2, updated_client);
        drop(storage_lock);

        let transaction_6 = Transaction { transaction_type: TransactionType::Withdrawal, client: 2, tx: 6, amount: Some(500.0) };
        processor.process_transaction(&transaction_6).await;

        let storage_lock = processor.storage.lock().await;
        let client_2 = storage_lock.get_client(2);
        // Tests whether withdrawal failed when available amount is less than the amount requested
        assert_approx_eq!(5.0, client_2.unwrap().total, 10e-4);
        assert_approx_eq!(5.0, client_2.unwrap().available, 10e-4);
    }

    #[tokio::test]
    async fn test_disputes() {
        let storage = InMemoryAccountStorage::new();
        let processor = TransactionProcessor::new(storage);
        let transaction_1 = Transaction { transaction_type: TransactionType::Deposit, client: 1, tx: 1, amount: Some(325.5) };
        let transaction_2 = Transaction { transaction_type: TransactionType::Deposit, client: 1, tx: 2, amount: Some(5.0) };
        let transaction_4 = Transaction { transaction_type: TransactionType::Withdrawal, client: 1, tx: 5, amount: Some(125.5) };
        let transaction_5 = Transaction { transaction_type: TransactionType::Dispute, client: 1, tx: 2, amount: None };
        let transaction_6 = Transaction { transaction_type: TransactionType::Resolve, client: 1, tx: 2, amount: None };
        let transaction_7 = Transaction { transaction_type: TransactionType::Dispute, client: 1, tx: 2, amount: None };
        let transaction_8 = Transaction { transaction_type: TransactionType::Chargeback, client: 1, tx: 2, amount: None };
        let transaction_dispute_withdrawal = Transaction { transaction_type: TransactionType::Dispute, client: 1, tx: 5, amount: None };
        let transaction_dispute_invalid_tx = Transaction { transaction_type: TransactionType::Dispute, client: 1, tx: 10000, amount: None };
        let transaction_chargeback_non_disputed_tx = Transaction { transaction_type: TransactionType::Chargeback, client: 1, tx: 1, amount: None };

        processor.process_transaction(&transaction_1).await;
        processor.process_transaction(&transaction_2).await;
        processor.process_transaction(&transaction_4).await;
        processor.process_transaction(&transaction_5).await;

        let storage_lock = processor.storage.lock().await;
        let client_1 = storage_lock.get_client(1);
        assert!(client_1.is_some(), "Client is not added to the storage");
        // Tests whether dispute is in place
        assert_approx_eq!(205.0, client_1.unwrap().total, 10e-4);
        assert_approx_eq!(200.0, client_1.unwrap().available, 10e-4);
        assert_approx_eq!(5.0, client_1.unwrap().held, 10e-4);
        drop(storage_lock);

        processor.process_transaction(&transaction_6).await;

        let storage_lock = processor.storage.lock().await;
        let client_1 = storage_lock.get_client(1);
        assert!(client_1.is_some(), "Client is not added to the storage");
        // Tests whether dispute is resolved
        assert_approx_eq!(205.0, client_1.unwrap().total, 10e-4);
        assert_approx_eq!(205.0, client_1.unwrap().available, 10e-4);
        assert_approx_eq!(0.0, client_1.unwrap().held, 10e-4);
        drop(storage_lock);

        processor.process_transaction(&transaction_7).await;

        let storage_lock = processor.storage.lock().await;
        let client_1 = storage_lock.get_client(1);
        assert!(client_1.is_some(), "Client is not added to the storage");
        // Tests whether dispute is in place
        assert_approx_eq!(205.0, client_1.unwrap().total, 10e-4);
        assert_approx_eq!(200.0, client_1.unwrap().available, 10e-4);
        assert_approx_eq!(5.0, client_1.unwrap().held, 10e-4);
        drop(storage_lock);

        processor.process_transaction(&transaction_8).await;

        let storage_lock = processor.storage.lock().await;
        let client_1 = storage_lock.get_client(1);
        assert!(client_1.is_some(), "Client is not added to the storage");
        // Tests whether dispute is charged back
        assert_approx_eq!(200.0, client_1.unwrap().total, 10e-4);
        assert_approx_eq!(200.0, client_1.unwrap().available, 10e-4);
        assert_approx_eq!(0.0, client_1.unwrap().held, 10e-4);
        drop(storage_lock);

        processor.process_transaction(&transaction_dispute_withdrawal).await;
        processor.process_transaction(&transaction_dispute_invalid_tx).await;
        processor.process_transaction(&transaction_chargeback_non_disputed_tx).await;

        let storage_lock = processor.storage.lock().await;
        let client_1 = storage_lock.get_client(1);
        assert!(client_1.is_some(), "Client is not added to the storage");
        // Tests whether none of the invalid transactions are processed
        assert_approx_eq!(200.0, client_1.unwrap().total, 10e-4);
        assert_approx_eq!(200.0, client_1.unwrap().available, 10e-4);
        assert_approx_eq!(0.0, client_1.unwrap().held, 10e-4);
        drop(storage_lock);
    }
}

