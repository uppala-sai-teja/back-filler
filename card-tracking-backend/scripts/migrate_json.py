# scripts/migrate_json.py
#!/usr/bin/env python3
"""
Migrate existing JSON data to MongoDB
"""

import json
import sys
import os
import argparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.mongodb_manager import MongoDBManager

def migrate_json_to_mongodb(json_file: str, db_manager: MongoDBManager) -> bool:
    """Migrate JSON data to MongoDB"""
    try:
        print(f"📥 Loading data from {json_file}...")
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        migrated_count = 0
        for customer_id, customer_data in data.items():
            if db_manager.upsert_customer(customer_data):
                migrated_count += 1
                print(f"✅ Migrated customer: {customer_id}")
            else:
                print(f"❌ Failed to migrate customer: {customer_id}")
        
        print(f"\n🎉 Migration completed! Migrated {migrated_count} customers.")
        return True
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Migrate JSON data to MongoDB")
    parser.add_argument("json_file", help="JSON file to migrate")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.json_file):
        print(f"❌ File not found: {args.json_file}")
        sys.exit(1)
    
    # Connect to MongoDB
    db_manager = MongoDBManager(debug=args.debug)
    if not db_manager.connect():
        print("❌ Failed to connect to MongoDB")
        sys.exit(1)
    
    # Migrate data
    if migrate_json_to_mongodb(args.json_file, db_manager):
        print("✅ Migration successful!")
    else:
        print("❌ Migration failed!")
        sys.exit(1)
    
    db_manager.disconnect()

if __name__ == "__main__":
    main()