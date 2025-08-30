#!/bin/bash

# ===========================================
# Environment Setup Script
# ===========================================

echo "Setting up environment variables for Chicago Crime Data Project..."

# Check if .env already exists
if [ -f ".env" ]; then
    echo "Warning: .env file already exists!"
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled. Existing .env file preserved."
        exit 0
    fi
fi

# Copy template to .env
if [ -f ".env.template" ]; then
    cp .env.template .env
    echo "✅ Created .env file from template"
    echo ""
    echo "📝 Next steps:"
    echo "1. Edit .env file with your actual API keys and configuration"
    echo "2. Add .env to your .gitignore file"
    echo "3. Never commit .env to version control"
    echo ""
    echo "🔧 You can edit the file with: nano .env"
else
    echo "❌ Error: .env.template file not found!"
    exit 1
fi
