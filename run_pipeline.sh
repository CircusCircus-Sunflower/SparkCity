#!/bin/bash
# SparkCity Pipeline Runner
# Quick script to run the pipeline

set -e  # Exit on error

echo "🚀 Starting SparkCity Pipeline..."
echo ""

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "📦 Activating virtual environment..."
    source venv/bin/activate
fi

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

# Check if containers are running
if ! docker-compose ps | grep -q "Up"; then
    echo "🐳 Starting Docker containers..."
    docker-compose up -d
    echo "⏳ Waiting for services to be ready..."
    sleep 10
fi

echo "✅ Docker services ready"
echo ""

# Run the pipeline
python src/pipeline/main.py "$@"

echo ""
echo "✅ Pipeline complete!"