cargo build -q

./target/debug/server 0 &
PID0=$!
./target/debug/server 1 &
PID1=$!
./target/debug/server 2 &
PID2=$!

echo "Started servers: PID0=$PID0 PID1=$PID1 PID2=$PID2"

echo "Client sends some command"
./target/debug/client1

sleep 2

echo "Kill server 2 (PID $PID2)"
kill "$PID2"
wait "$PID2" || true

sleep 2

echo "Client sends more command"
./target/debug/client2

echo "Restart server 2"
./target/debug/server 2 &
PID2_NEW=$!

sleep 30

echo "Kill all servers and exit"
kill "$PID0" "$PID1" "$PID2_NEW" || true
wait || true