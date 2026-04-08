#!/usr/bin/env bash
set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Installing Python dependencies ==="
pip install -e "$REPO_DIR[dev]" --quiet

echo "=== Installing frontend dependencies ==="
cd "$REPO_DIR/frontend"
npm install --silent

echo "=== Building frontend ==="
npm run build

echo "=== Starting API server (port 8000) and frontend dev server (port 5173) ==="
cd "$REPO_DIR"

# Start uvicorn in the background
uvicorn app.main:app --host 0.0.0.0 --port 8000 &
API_PID=$!

# Start Vite dev server in the background (proxies /api to :8000)
cd "$REPO_DIR/frontend"
npm run dev -- --host 0.0.0.0 &
FRONTEND_PID=$!

# Cleanup on exit
trap "kill $API_PID $FRONTEND_PID 2>/dev/null; exit" INT TERM EXIT

echo ""
echo "  API:      http://localhost:8000"
echo "  API docs: http://localhost:8000/docs"
echo "  Frontend: http://localhost:5173"
echo ""
echo "Press Ctrl+C to stop."

wait
