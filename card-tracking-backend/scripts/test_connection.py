# scripts/test_connection.py
#!/usr/bin/env python3
"""
Test MongoDB connection and basic operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.mongodb_manager import MongoDBManager
from datetime import datetime

def test_connection():
    """Test MongoDB connection and basic operations"""
    print("🔍 Testing MongoDB connection...")
    
    db_manager = MongoDBManager(debug=True)
    
    if not db_manager.connect():
        print("❌ Failed to connect to MongoDB")
        return False
    
    print("✅ MongoDB connection successful!")
    
    # Test basic operations
    test_customer = {
        "_id": "TEST_001",
        "customer_info": {
            "name": "Test User",
            "mobile": "+919999999999",
            "email": "test@example.com"
        },
        "cards": [],
        "metadata": {
            "created_at": datetime.now().isoformat() + "Z",
            "last_updated": datetime.now().isoformat() + "Z"
        }
    }
    
    # Test upsert
    print("🔍 Testing customer upsert...")
    if db_manager.upsert_customer(test_customer):
        print("✅ Customer upsert successful!")
    else:
        print("❌ Customer upsert failed!")
        return False
    
    # Test retrieval
    print("🔍 Testing customer retrieval...")
    retrieved = db_manager.get_customer("TEST_001")
    if retrieved:
        print("✅ Customer retrieval successful!")
        print(f"   Retrieved: {retrieved['customer_info']['name']}")
    else:
        print("❌ Customer retrieval failed!")
        return False
    
    # Test analytics
    print("🔍 Testing analytics...")
    status_summary = db_manager.get_status_summary()
    bank_performance = db_manager.get_bank_performance()
    print("✅ Analytics working!")
    
    # Cleanup
    print("🔍 Cleaning up test data...")
    db_manager.customers_collection.delete_one({"_id": "TEST_001"})
    print("✅ Test cleanup completed!")
    
    db_manager.disconnect()
    return True

def main():
    if test_connection():
        print("\n🎉 All tests passed! MongoDB is ready to use.")
    else:
        print("\n❌ Tests failed! Please check your MongoDB configuration.")
        sys.exit(1)

if __name__ == "__main__":
    main()
