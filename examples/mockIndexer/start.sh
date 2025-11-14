#!/bin/bash

# Mock Indexer - Quick Start Script

echo "🎭 Mock Indexer - Dummy Data Generator"
echo ""
echo "This will generate fake blockchain data including:"
echo "  • 5-10 blocks per batch"
echo "  • Multiple transactions per block"
echo "  • Multiple logs per block (ALL with target address)"
echo "  • Posts a new batch every minute"
echo ""
echo "Prerequisites:"
echo "  ✓ bigPeer should be running on port 9176"
echo ""
echo "Starting mock indexer (runs continuously)..."
echo "Press Ctrl+C to stop"
echo ""

go run cmd/main.go

