#!/bin/bash

echo "=========================================="
echo "Chicago Crime Data Pipeline Setup"
echo "=========================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.template .env
    echo "✓ .env file created"
    echo "⚠️  Please edit .env file with your API keys and AWS credentials"
else
    echo "✓ .env file already exists"
fi

# Install dependencies
echo "Installing Python dependencies..."
pipenv install
echo "✓ Dependencies installed"

# Create necessary directories
echo "Creating directories..."
mkdir -p RawData/DataSet1
echo "✓ Directories created"

echo "=========================================="
echo "Setup complete!"
echo "Next steps:"
echo "1. Edit .env file with your credentials"
echo "2. Run: pipenv run python src/ingestion/IngestData1.py"
echo "=========================================="
