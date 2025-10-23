#!/bin/bash

# Shinzo Web Demo App - Quick Start Script

echo "🚀 Starting Shinzo Web Demo App..."
echo ""
echo "Prerequisites:"
echo "  ✓ bigPeer should be running on port 9176"
echo "  ✓ indexer should be running on ports 9181-9182"
echo "  ✓ host should be running on ports 9180/9183"
echo ""
echo "Starting web server..."
echo "Dashboard will be available at: http://localhost:8080"
echo ""
echo "Press Ctrl+C to stop"
echo ""

go run cmd/main.go


