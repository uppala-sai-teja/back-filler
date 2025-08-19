# core/mongodb_manager.py
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from dotenv import load_dotenv

load_dotenv()

class MongoDBManager:
    def __init__(self, debug=False):
        self.setup_logging(debug)
        self.client = None
        self.db = None
        self.customers_collection = None
        self.notifications_collection = None
        
    def setup_logging(self, debug):
        level = logging.DEBUG if debug else logging.INFO
        self.logger = logging.getLogger('mongodb_manager')
        self.logger.setLevel(level)
        
    def connect(self):
        """Establish MongoDB connection"""
        try:
            uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            
            # Test connection
            self.client.admin.command('ping')
            self.logger.info("✅ Connected to MongoDB successfully")
            
            # Setup database and collections
            db_name = os.getenv('MONGODB_DATABASE', 'card_tracking')
            self.db = self.client[db_name]
            
            self.customers_collection = self.db['customers']
            self.notifications_collection = self.db['notifications']
            
            # Create indexes
            self.create_indexes()
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"❌ Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Unexpected error: {e}")
            return False
    
    def create_indexes(self):
        """Create necessary indexes for performance"""
        try:
            # Customer collection indexes
            self.customers_collection.create_index("customer_info.email")
            self.customers_collection.create_index("customer_info.mobile")
            self.customers_collection.create_index("cards.tracking_ids.application_id")
            self.customers_collection.create_index("cards.tracking_ids.logistics_tracking_number")
            self.customers_collection.create_index("cards.current_status.status")
            self.customers_collection.create_index([("metadata.last_updated", -1)])  # -1 for descending
            
            # Notifications collection indexes
            self.notifications_collection.create_index("customer_id")
            self.notifications_collection.create_index("sent")
            self.notifications_collection.create_index([("timestamp", -1)])  # -1 for descending
            
            self.logger.info("✅ Created MongoDB indexes")
            
        except Exception as e:
            self.logger.error(f"❌ Error creating indexes: {e}")
    
    def disconnect(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self.logger.info("📝 Disconnected from MongoDB")

    # Customer Operations
    def get_customer(self, customer_id: str) -> Optional[Dict]:
        """Get customer by ID"""
        try:
            return self.customers_collection.find_one({"_id": customer_id})
        except Exception as e:
            self.logger.error(f"Error getting customer {customer_id}: {e}")
            return None
    
    def upsert_customer(self, customer_data: Dict) -> bool:
        """Insert or update customer"""
        try:
            customer_id = customer_data["_id"]
            customer_data["metadata"]["last_updated"] = datetime.now().isoformat() + "Z"
            
            result = self.customers_collection.replace_one(
                {"_id": customer_id},
                customer_data,
                upsert=True
            )
            
            if result.upserted_id:
                self.logger.info(f"✅ Created customer: {customer_id}")
            else:
                self.logger.info(f"✅ Updated customer: {customer_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error upserting customer: {e}")
            return False
    
    def find_card_by_tracking_id(self, tracking_type: str, tracking_value: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Find card and customer by any tracking ID"""
        try:
            query = {f"cards.tracking_ids.{tracking_type}": tracking_value}
            customer = self.customers_collection.find_one(query)
            
            if customer:
                for card in customer.get("cards", []):
                    if card.get("tracking_ids", {}).get(tracking_type) == tracking_value:
                        return card, customer["_id"]
            return None, None
            
        except Exception as e:
            self.logger.error(f"Error finding card by {tracking_type}: {e}")
            return None, None
    
    # Analytics Operations
    def get_status_summary(self) -> Dict:
        """Get summary of all card statuses"""
        try:
            pipeline = [
                {"$unwind": "$cards"},
                {"$group": {
                    "_id": "$cards.current_status.status",
                    "count": {"$sum": 1}
                }},
                {"$sort": {"count": -1}}
            ]
            
            result = list(self.customers_collection.aggregate(pipeline))
            return {item["_id"]: item["count"] for item in result}
            
        except Exception as e:
            self.logger.error(f"Error getting status summary: {e}")
            return {}
    
    def get_bank_performance(self) -> Dict:
        """Get performance metrics by bank"""
        try:
            pipeline = [
                {"$unwind": "$cards"},
                {"$group": {
                    "_id": "$cards.card_info.bank_name",
                    "total_cards": {"$sum": 1},
                    "completed_cards": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$cards.tracking_status", "completed"]},
                                1, 0
                            ]
                        }
                    }
                }}
            ]
            
            result = list(self.customers_collection.aggregate(pipeline))
            return {item["_id"]: {
                "total": item["total_cards"],
                "completed": item["completed_cards"]
            } for item in result}
            
        except Exception as e:
            self.logger.error(f"Error getting bank performance: {e}")
            return {}
    
    # Notification Operations
    def save_notification(self, notification: Dict) -> bool:
        """Save notification to database"""
        try:
            notification["_id"] = f"{notification['customer_id']}_{notification['card_id']}_{int(datetime.now().timestamp())}"
            notification["created_at"] = datetime.now().isoformat() + "Z"
            
            self.notifications_collection.insert_one(notification)
            self.logger.info(f"✅ Saved notification for {notification['customer_id']}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error saving notification: {e}")
            return False
    
    def get_pending_notifications(self, limit: int = 100) -> List[Dict]:
        """Get unsent notifications"""
        try:
            return list(self.notifications_collection.find(
                {"sent": False}
            ).limit(limit).sort("created_at", 1))  # 1 for ascending
            
        except Exception as e:
            self.logger.error(f"Error getting pending notifications: {e}")
            return []
