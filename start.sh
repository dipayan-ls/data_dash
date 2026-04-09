#!/bin/bash

# Trap CTRL+C to kill all background processes (both frontend and backend) gracefully when you stop it
trap 'kill 0' SIGINT

echo "Starting Flask Backend Server (Port 3000)..."
python3 app.py &

echo "Starting Vite React Frontend (Port 5173)..."
npm run dev

# Wait for both processes to stay alive
wait
