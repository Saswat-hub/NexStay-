#!/bin/bash
# ─────────────────────────────────────────────────────────
# NexStay Hotel Price Optimizer — One-Click Startup
# ─────────────────────────────────────────────────────────

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "╔═══════════════════════════════════════════╗"
echo "║   NexStay · Hotel Price Optimizer  ⚡     ║"
echo "╚═══════════════════════════════════════════╝"
echo ""

# Install dependencies
echo "📦 Installing dependencies..."
pip install flask flask-cors --quiet 2>/dev/null || pip3 install flask flask-cors --quiet

# Initialize database
echo "🗄️  Initializing database..."
python3 database/init_db.py

# Start backend
echo "🚀 Starting Flask backend on http://localhost:5000"
echo "🌐 Open frontend/index.html in your browser"
echo ""
echo "  Dashboard    → http://localhost:5000/"
echo "  API Docs     → http://localhost:5000/api/hotels/search"
echo "  Spark Engine → POST http://localhost:5000/api/spark/optimize"
echo ""

python3 backend/app.py
