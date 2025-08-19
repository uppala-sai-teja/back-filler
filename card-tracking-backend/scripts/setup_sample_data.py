# scripts/setup_sample_data.py
#!/usr/bin/env python3
"""
Create sample data files for testing the card tracking system
"""

import json
import os
from datetime import datetime, timedelta

def create_sample_data():
    """Create sample JSON files for testing"""
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Sample bank data
    bank_data = [
        {
            "customer_id": "CUST_001",
            "customer_name": "John Doe",
            "mobile": "9876543210",
            "email": "john@example.com",
            "application_id": "APP_001",
            "application_date": "2025-08-15T09:30:00Z",
            "card_type": "credit_card",
            "card_variant": "platinum",
            "status": "submitted"
        },
        {
            "customer_id": "CUST_001",
            "customer_name": "John Doe",
            "mobile": "9876543210",
            "email": "john@example.com",
            "application_id": "APP_001",
            "card_type": "credit_card",
            "card_variant": "platinum",
            "status": "approved",
            "approval_date": "2025-08-15T14:30:00Z"
        },
        {
            "customer_id": "CUST_002",
            "customer_name": "Jane Smith",
            "mobile": "9876543211",
            "email": "jane@example.com",
            "application_id": "APP_002",
            "application_date": "2025-08-16T10:00:00Z",
            "card_type": "debit_card",
            "card_variant": "gold",
            "status": "submitted"
        }
    ]
    
    # Sample manufacturer data
    manufacturer_data = [
        {
            "bank_reference": "APP_001",
            "order_id": "MFG_ORDER_001",
            "batch_number": "BATCH_2025_08_001",
            "facility": "Chennai Production Unit",
            "production_history": [
                {
                    "status": "received",
                    "timestamp": "2025-08-16T08:00:00Z",
                    "location": "Chennai Production Unit"
                },
                {
                    "status": "in_production",
                    "timestamp": "2025-08-16T10:00:00Z",
                    "location": "Chennai Production Unit"
                },
                {
                    "status": "embossing",
                    "timestamp": "2025-08-16T14:00:00Z",
                    "location": "Chennai Production Unit"
                },
                {
                    "status": "quality_check",
                    "timestamp": "2025-08-16T16:00:00Z",
                    "location": "Chennai Production Unit"
                },
                {
                    "status": "completed",
                    "timestamp": "2025-08-16T18:00:00Z",
                    "location": "Chennai Production Unit"
                },
                {
                    "status": "dispatched",
                    "timestamp": "2025-08-17T09:00:00Z",
                    "location": "Chennai Hub",
                    "courier_partner": "DTDC",
                    "tracking_number": "DTDC123456789"
                }
            ]
        },
        {
            "bank_reference": "APP_002",
            "order_id": "MFG_ORDER_002",
            "batch_number": "BATCH_2025_08_002",
            "facility": "Mumbai Production Unit",
            "production_history": [
                {
                    "status": "received",
                    "timestamp": "2025-08-17T09:00:00Z",
                    "location": "Mumbai Production Unit"
                },
                {
                    "status": "in_production",
                    "timestamp": "2025-08-17T11:00:00Z",
                    "location": "Mumbai Production Unit"
                }
            ]
        }
    ]
    
    # Sample logistics data
    logistics_data = [
        {
            "awb_number": "DTDC123456789",
            "recipient_name": "John Doe",
            "recipient_mobile": "+919876543210",
            "delivery_address": "123 Main St, Mumbai, Maharashtra",
            "tracking_history": [
                {
                    "status": "picked_up",
                    "timestamp": "2025-08-17T10:00:00Z",
                    "location": "Chennai Hub",
                    "description": "Package picked up from sender"
                },
                {
                    "status": "in_transit",
                    "timestamp": "2025-08-17T15:30:00Z",
                    "location": "Bangalore Hub",
                    "description": "Package in transit"
                },
                {
                    "status": "in_transit",
                    "timestamp": "2025-08-18T08:00:00Z",
                    "location": "Mumbai Hub",
                    "description": "Package in transit"
                },
                {
                    "status": "out_for_delivery",
                    "timestamp": "2025-08-18T09:30:00Z",
                    "location": "Mumbai Local Hub",
                    "description": "Package out for delivery"
                },
                {
                    "status": "delivered",
                    "timestamp": "2025-08-18T14:45:00Z",
                    "location": "Customer Address",
                    "description": "Package delivered successfully"
                }
            ]
        }
    ]
    
    # Save sample files
    files_created = []
    
    with open('data/sample_bank_data.json', 'w') as f:
        json.dump(bank_data, f, indent=2)
    files_created.append('data/sample_bank_data.json')
    
    with open('data/sample_manufacturer_data.json', 'w') as f:
        json.dump(manufacturer_data, f, indent=2)
    files_created.append('data/sample_manufacturer_data.json')
    
    with open('data/sample_logistics_data.json', 'w') as f:
        json.dump(logistics_data, f, indent=2)
    files_created.append('data/sample_logistics_data.json')
    
    return files_created

def main():
    print("🚀 Creating sample data files...")
    files = create_sample_data()
    
    print("✅ Created sample data files:")
    for file in files:
        print(f"  - {file}")
    
    print("\n📝 Next steps:")
    print("1. Copy config/.env.template to .env")
    print("2. Configure MongoDB settings in .env")
    print("3. Run: python main.py --test-connection")
    print("4. Process data: python main.py data/sample_bank_data.json --type bank")

if __name__ == "__main__":
    main()
