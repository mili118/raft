#!/bin/bash

# Script to run a 3-node Raft cluster for testing

echo "Starting 3-node Raft cluster..."

# Kill any existing instances
pkill -f raft-kv || true
sleep 1

# Remove old logs
rm -f node*.log

echo "Building release binary..."
cargo build --release 2>&1 | grep -E "(Compiling raft-kv|Finished)"

# Start node 1
./target/release/raft-kv \
  --id 1 \
  --port 50051 \
  --peer 2:localhost:50052 \
  --peer 3:localhost:50053 \
  > node1.log 2>&1 &

# Start node 2
./target/release/raft-kv \
  --id 2 \
  --port 50052 \
  --peer 1:localhost:50051 \
  --peer 3:localhost:50053 \
  > node2.log 2>&1 &

# Start node 3
./target/release/raft-kv \
  --id 3 \
  --port 50053 \
  --peer 1:localhost:50051 \
  --peer 2:localhost:50052 \
  > node3.log 2>&1 &

echo ""
echo "✓ Nodes started successfully!"
echo ""
echo "Waiting for leader election..."
sleep 2

# Show initial state
echo "=== Initial cluster state ==="
echo -e "\nNode 1:"
tail -5 node1.log
echo -e "\nNode 2:"
tail -5 node2.log
echo -e "\nNode 3:"
tail -5 node3.log

echo ""
echo "=== Cluster is running ==="
echo "To view logs in real-time:"
echo "  tail -f node1.log"
echo "  tail -f node2.log"
echo "  tail -f node3.log"
echo ""
echo "To stop the cluster:"
echo "  pkill -f raft-kv"
echo ""
echo "Press Ctrl+C to stop all nodes"

# Wait for user interrupt
trap "echo ''; echo 'Stopping cluster...'; pkill -f raft-kv; exit" INT
wait
