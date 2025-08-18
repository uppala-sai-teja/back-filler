#!/bin/bash
# Quick setup script for Card Tracking System

echo "🚀 Setting up Card Tracking System for Hackathon..."

# Install required packages
echo "📦 Installing required Python packages..."
pip install jsonpath-ng

# Create master_config.json (you should paste the template content here)
if [ ! -f "master_config.json" ]; then
    echo "⚠️  Please create master_config.json with the template configuration"
    echo "   You can copy it from the artifacts provided"
fi

# Create test data files
echo "🎯 Creating test data files..."
python processor.py --create-tests

echo ""
echo "✅ Setup complete! Here's how to test:"
echo ""
echo "🧪 Testing sequence:"
echo "1. python processor.py test_bank_data.json --type bank"
echo "2. python processor.py test_manufacturer_data.json --type card_manufacturer"
echo "3. python processor.py test_logistics_data.json --type logistics"
echo "4. python processor.py --show-state"
echo ""
echo "🔧 Useful commands:"
echo "• python processor.py --show-state          # View current state"
echo "• python processor.py --reset               # Reset state"
echo "• python processor.py --debug               # Enable debug mode"
echo "• python processor.py --create-tests        # Recreate test files"
echo ""
echo "📁 Files created:"
echo "• processor.py                    # Main processor"
echo "• master_config.json             # Template configuration" 
echo "• test_bank_data.json            # Bank test data"
echo "• test_manufacturer_data.json    # Manufacturer test data"
echo "• test_logistics_data.json       # Logistics test data"
echo "• local_db_state.json           # State file (created after processing)"
echo "• processor.log                  # Processing logs"
echo ""


# 1. Create the files and install dependencies
pip install jsonpath-ng

# 2. Create master_config.json (copy from the template artifact)

# 3. Generate test files
python processor.py --create-tests

# 4. Test the complete flow
python processor.py test_bank_data.json --type bank
python processor.py test_manufacturer_data.json --type card_manufacturer  
python processor.py test_logistics_data.json --type logistics

# 5. View results
python processor.py --show-state