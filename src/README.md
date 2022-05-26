# Getting Started
- Build project with:
```shell
cargo build
```
- To run the payment engine on a CSV file:
```shell
cargo run -- $CSV_FILE
```
- To run tests:
```shell
cargo test
```
# Assumptions
- Withdrawals can not be disputed
- Transaction IDs can not be duplicated
- A transaction could be disputed multiple times (if it had been resolved)
- A dispute can not be executed if the amount that was deposited has already been withdrawn
# Room for improvement
- Replace InMemory storage with a DB/File based storage
- Replace singe storage mutex with multiple sessions (currently one transaction locks the entire storage) in multiple event handlers
- Divide transaction and client storage to multiple storages
- Logging and error handling (error handling is currently turned off due to the fact that it would disrupt the automated tests)
