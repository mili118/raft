cargo run --bin server -- 0 &
cargo run --bin server -- 1 &
cargo run --bin server -- 2 &
cargo run --bin client &
wait