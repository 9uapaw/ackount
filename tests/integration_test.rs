use assert_approx_eq::assert_approx_eq;
use ackount::processor::process_csv_file;
use ackount::storage::AccountStorage;

#[tokio::test]
async fn test_csv_reading() {
    let processor = process_csv_file("tests/test.csv").await;

    let storage = processor.storage.lock().await;
    let client_1 = storage.get_client(1);
    let client_2 = storage.get_client(2);
    let client_3 = storage.get_client(3);
    let client_4 = storage.get_client(4);

    assert!(client_1.is_some(), "Client is not present in the storage");
    assert!(client_2.is_some(), "Client is not present in the storage");
    assert!(client_3.is_some(), "Client is not present in the storage");
    assert!(client_4.is_some(), "Client is not present in the storage");

    let client_1 = client_1.unwrap();
    let client_2 = client_2.unwrap();
    let client_3 = client_3.unwrap();
    let client_4 = client_4.unwrap();

    assert_approx_eq!(client_1.total, 332.453, 10e-4);
    assert_approx_eq!(client_1.available, 332.453, 10e-4);
    assert_approx_eq!(client_1.held, 0.0, 10e-4);
    assert!(!client_1.locked);

    assert_approx_eq!(client_2.total, 100.0, 10e-4);
    assert_approx_eq!(client_2.available, 100.0, 10e-4);
    assert_approx_eq!(client_2.held, 0.0, 10e-4);
    assert!(!client_2.locked);

    assert_approx_eq!(client_3.total, 999.9, 10e-4);
    assert_approx_eq!(client_3.available, 0.0, 10e-4);
    assert_approx_eq!(client_3.held, 999.9, 10e-4);
    assert!(!client_3.locked);

    assert_approx_eq!(client_4.total, 432.0, 10e-4);
    assert_approx_eq!(client_4.available, 432.0, 10e-4);
    assert_approx_eq!(client_4.held, 0.0, 10e-4);
    assert!(client_4.locked);
}