# core/card_processor.py - Updated with pending stages support
import json
import logging
import re
import os
from datetime import datetime, timedelta
from jsonpath_ng import parse
from typing import Dict, List, Optional, Generator
from .mongodb_manager import MongoDBManager

# Global stage order definition
STAGE_ORDER = ["application_and_approval", "card_production", "shipping_and_delivery"]

# Required fields for validation
REQUIRED_FIELDS = {
    "bank": ["customer_id", "application_id", "status"],
    "card_manufacturer": ["application_id"],
    "logistics": ["logistics_tracking_number"]
}

class CardTrackingProcessor:
    def __init__(self, debug=False):
        self.setup_logging(debug)
        self.stats = {"processed": 0, "errors": 0, "skipped": 0, "notifications_sent": 0}
        
        # Initialize MongoDB connection
        self.db_manager = MongoDBManager(debug)
        if not self.db_manager.connect():
            raise Exception("Failed to connect to MongoDB")
        
    def setup_logging(self, debug):
        level = logging.DEBUG if debug else logging.INFO
        
        # Ensure logs directory exists
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def __del__(self):
        """Cleanup MongoDB connection"""
        try:
            if hasattr(self, 'db_manager') and self.db_manager:
                self.db_manager.disconnect()
        except:
            pass  # Ignore cleanup errors during shutdown

    # Configuration
    def get_template(self, provider_type: str) -> Optional[Dict]:
        """Load provider template from config"""
        try:
            with open('config/master_config.json', 'r') as f:
                config = json.load(f)
            template = config.get(provider_type, {}).get("default")
            if not template:
                self.logger.error(f"No template found for {provider_type}")
                return None
            return template
        except Exception as e:
            self.logger.error(f"Error loading template: {e}")
            return None

    # Stage and Status Management
    def calculate_pending_stages(self, current_stage: str) -> List[str]:
        """Calculate pending stages based on current stage"""
        try:
            current_index = STAGE_ORDER.index(current_stage)
            return STAGE_ORDER[current_index + 1:]
        except (ValueError, IndexError):
            return STAGE_ORDER.copy()
    
    def update_card_pending_stages(self, card: Dict) -> Dict:
        """Update pending stages for a card"""
        current_stage = card.get("current_status", {}).get("stage", "")
        card["pending_stages"] = self.calculate_pending_stages(current_stage)
        return card

    # Data Processing
    def normalize_phone_number(self, phone: str) -> str:
        """Normalize phone number to +91XXXXXXXXXX format"""
        if not phone:
            return ""
        digits = re.sub(r'\D', '', str(phone))
        if digits.startswith('91') and len(digits) == 12:
            return f"+91{digits[2:]}"
        elif len(digits) == 10 and digits[0] in '6789':
            return f"+91{digits}"
        else:
            self.logger.warning(f"Could not normalize phone: {phone}")
            return phone

    def normalize_date(self, date_str: str) -> str:
        """Normalize date to ISO format"""
        if not date_str:
            return datetime.now().isoformat() + "Z"
        patterns = [
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y"
        ]
        for pattern in patterns:
            try:
                dt = datetime.strptime(date_str, pattern)
                return dt.isoformat() + "Z"
            except ValueError:
                continue
        return date_str

    def calculate_estimated_delivery(self, current_status: str) -> Optional[str]:
        """Calculate estimated delivery based on current status"""
        base_date = datetime.now()
        
        delivery_estimates = {
            "APPLICATION_APPROVED": 6,
            "PRODUCTION_QUEUED": 4,
            "PRODUCTION_STARTED": 4,
            "CARD_PERSONALIZED": 3,
            "DISPATCHED": 2,
            "IN_TRANSIT": 1,
            "OUT_FOR_DELIVERY": 0
        }
        
        days = delivery_estimates.get(current_status)
        if days is not None:
            estimated = base_date + timedelta(days=days)
            return estimated.isoformat() + "Z"
        return None

    def extract_fields(self, raw_data: Dict, field_mappings: Dict) -> Dict:
        """Extract fields using JSONPath mappings"""
        extracted = {}
        for key, path in field_mappings.items():
            try:
                matches = [match.value for match in parse(path).find(raw_data)]
                if matches:
                    extracted[key] = matches[0]
            except Exception:
                continue
        return extracted

    def validate_data(self, data: Dict, provider_type: str) -> List[str]:
        """Validate required fields and formats"""
        errors = []
        required = REQUIRED_FIELDS.get(provider_type, [])
        
        for field in required:
            if not data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate mobile format
        mobile = data.get("mobile")
        if mobile and not re.match(r'^\+91[6-9]\d{9}$', mobile):
            errors.append(f"Invalid mobile format: {mobile}")
            
        return errors

    def create_timeline_event(self, data: Dict, template: Dict) -> Optional[Dict]:
        """Create timeline event from data"""
        raw_status = data.get("status")
        if not raw_status:
            return None
            
        status_mapping = template.get("status_mappings", {}).get(raw_status)
        if not status_mapping:
            return None
        
        timestamp = (data.get("timestamp") or 
                     data.get("approval_date") or 
                     data.get("application_date") or 
                     datetime.now().isoformat() + "Z")
        
        return {
            "status": status_mapping["status"],
            "stage": status_mapping["stage"],
            "timestamp": self.normalize_date(timestamp),
            "description": status_mapping["description"],
            "location": data.get("location", data.get("facility_location", "Unknown")),
            "provider": template.get("provider_name")
        }

    def process_data(self, raw_data: Dict, template: Dict) -> Generator[Dict, None, None]:
        """Process raw data into timeline events"""
        try:
            base_data = self.extract_fields(raw_data, template.get("field_mappings", {}))
            if "mobile" in base_data:
                base_data["mobile"] = self.normalize_phone_number(base_data["mobile"])
            
            history_field = template.get("history_field")
            if history_field and history_field in raw_data:
                # Process historical data
                for item in raw_data.get(history_field, []):
                    processed = base_data.copy()
                    for hist_key, hist_path in template.get("history_mappings", {}).items():
                        if hist_path in item:
                            processed[hist_key] = item[hist_path]
                    timeline_event = self.create_timeline_event(processed, template)
                    if timeline_event:
                        processed["timeline_event"] = timeline_event
                        yield processed
            else:
                # Process single event
                timeline_event = self.create_timeline_event(base_data, template)
                if timeline_event:
                    base_data["timeline_event"] = timeline_event
                    yield base_data
                    
        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            self.stats["errors"] += 1

    # MongoDB Operations
    def find_or_create_customer(self, customer_id: str, customer_data: Dict = None) -> Dict:
        """Find existing customer or create new one"""
        customer = self.db_manager.get_customer(customer_id)
        
        if not customer:
            customer = {
                "_id": customer_id,
                "customer_info": {
                    "name": customer_data.get("customer_name", "Unknown") if customer_data else "Unknown",
                    "mobile": customer_data.get("mobile", "") if customer_data else "",
                    "email": customer_data.get("email", "") if customer_data else ""
                },
                "cards": [],
                "metadata": {
                    "created_at": datetime.now().isoformat() + "Z",
                    "last_updated": datetime.now().isoformat() + "Z"
                }
            }
            self.db_manager.upsert_customer(customer)
            
        return customer

    def create_new_card(self, data: Dict, template: Dict) -> Dict:
        """Create new card record with pending stages"""
        timestamp = datetime.now().isoformat() + "Z"
        bank_label = (template.get("provider_name", "Bank") 
                     if template.get("provider_type") == "bank" 
                     else (data.get("bank_name") or "Bank"))
        
        card = {
            "card_id": f"CARD_{data.get('application_id', 'UNK')}_{int(datetime.now().timestamp())}",
            "tracking_ids": {
                "application_id": data.get("application_id"),
                "customer_id": data.get("customer_id"),
                "manufacturer_order_id": data.get("manufacturer_order_id"),
                "logistics_tracking_number": data.get("tracking_number") or data.get("logistics_tracking_number")
            },
            "tracking_status": "active",
            "card_info": {
                "bank_name": bank_label,
                "card_type": data.get("card_type", "Unknown"),
                "card_variant": data.get("card_variant", "Standard"),
                "card_purpose": "new_application"
            },
            "current_status": {},
            "timeline": {
                "application_and_approval": [],
                "card_production": [],
                "shipping_and_delivery": []
            },
            "estimated_delivery": None,
            "pending_stages": STAGE_ORDER.copy(),  # Initially all stages are pending
            "application_metadata": {
                "courier_partner": None,
                "current_tracking_number": None,
                "production_batch": None,
                "facility_location": None,
                "priority": "standard"
            },
            "metadata": {
                "created_at": timestamp,
                "last_updated": timestamp
            }
        }
        
        return card

    def update_card_with_event(self, customer: Dict, card: Dict, data: Dict, timeline_event: Dict) -> bool:
        """Update card with new timeline event and pending stages"""
        # Find card index in customer's cards
        card_index = None
        for i, c in enumerate(customer.get("cards", [])):
            if c.get("card_id") == card.get("card_id"):
                card_index = i
                break
                
        if card_index is None:
            self.logger.error(f"Card not found: {card.get('card_id')}")
            return False

        # Update timeline
        stage = timeline_event["stage"]
        timeline_list = card["timeline"].setdefault(stage, [])
        
        # Check for duplicates
        if timeline_list:
            last_event = timeline_list[-1]
            if (timeline_event["timestamp"] <= last_event.get("timestamp", "") or 
                (last_event.get("status") == timeline_event["status"] and 
                 last_event.get("location") == timeline_event["location"])):
                self.stats["skipped"] += 1
                return False
        
        timeline_list.append(timeline_event)

        # Update current status
        card["current_status"] = {
            "status": timeline_event["status"],
            "stage": timeline_event["stage"],
            "location": timeline_event["location"],
            "last_updated": timeline_event["timestamp"],
            "description": timeline_event["description"]
        }

        # Update pending stages based on new current stage
        card = self.update_card_pending_stages(card)

        # Update metadata
        app_metadata = card["application_metadata"]
        if data.get("courier_partner"):
            app_metadata["courier_partner"] = data["courier_partner"]
        if data.get("tracking_number"):
            app_metadata["current_tracking_number"] = data["tracking_number"]
        if data.get("production_batch"):
            app_metadata["production_batch"] = data["production_batch"]

        # Update estimated delivery for key statuses
        if timeline_event["status"] in ["APPLICATION_APPROVED", "PRODUCTION_QUEUED", 
                                       "CARD_PERSONALIZED", "DISPATCHED"]:
            estimated = self.calculate_estimated_delivery(timeline_event["status"])
            if estimated:
                card["estimated_delivery"] = estimated

        # Update tracking IDs
        provider_type = data.get("provider_type") 
        if provider_type == "card_manufacturer" and data.get("manufacturer_order_id"):
            card["tracking_ids"]["manufacturer_order_id"] = data["manufacturer_order_id"]
        elif provider_type == "logistics" and data.get("logistics_tracking_number"):
            card["tracking_ids"]["logistics_tracking_number"] = data["logistics_tracking_number"]

        # Handle completion
        if timeline_event["status"] in ["DELIVERED", "APPLICATION_REJECTED", "RETURNED_TO_SENDER"]:
            card["tracking_status"] = "completed"
            card["pending_stages"] = []  # No more pending stages

        # Update timestamps
        now = datetime.now().isoformat() + "Z"
        card["metadata"]["last_updated"] = now
        customer["metadata"]["last_updated"] = now

        # Save to MongoDB
        customer["cards"][card_index] = card
        return self.db_manager.upsert_customer(customer)

    def process_bulk_data(self, bulk_data: List[Dict], template: Dict) -> bool:
        """Process bulk data and save to MongoDB"""
        provider_type = template.get("provider_type")
        
        for record in bulk_data:
            try:
                for processed_data in self.process_data(record, template):
                    # Validate data
                    validation_errors = self.validate_data(processed_data, provider_type)
                    if validation_errors:
                        self.logger.error(f"Validation errors: {validation_errors}")
                        self.stats["errors"] += 1
                        continue
                    
                    # Get timeline event
                    timeline_event = processed_data.get("timeline_event")
                    if not timeline_event:
                        continue
                    
                    # Add provider_type to processed_data for tracking ID updates
                    processed_data["provider_type"] = provider_type
                    
                    # Find or create customer and card
                    if provider_type == "bank":
                        customer_id = processed_data.get("customer_id")
                        customer = self.find_or_create_customer(customer_id, processed_data)
                        
                        # Find existing card or create new one
                        card = None
                        application_id = processed_data.get("application_id")
                        for c in customer.get("cards", []):
                            if c.get("tracking_ids", {}).get("application_id") == application_id:
                                card = c
                                break
                        
                        if not card:
                            card = self.create_new_card(processed_data, template)
                            customer["cards"].append(card)
                    
                    else:
                        # For manufacturer/logistics, find by tracking ID
                        lookup_key = template.get("lookup_key")
                        lookup_value = processed_data.get(lookup_key)
                        
                        if self.debug:
                            self.logger.debug(f"Looking for {lookup_key}: {lookup_value}")
                        
                        card, customer_id = self.db_manager.find_card_by_tracking_id(lookup_key, lookup_value)
                        if not card:
                            self.logger.warning(f"Card not found for {lookup_key}: {lookup_value}")
                            self.stats["skipped"] += 1
                            continue
                        
                        customer = self.db_manager.get_customer(customer_id)
                    
                    # Update card with event
                    if self.update_card_with_event(customer, card, processed_data, timeline_event):
                        self.stats["processed"] += 1
                    else:
                        self.stats["errors"] += 1
                        
            except Exception as e:
                self.logger.error(f"Error in record: {e}")
                self.stats["errors"] += 1
                continue
        
        return True

    # Utility Methods for Enhanced Features
    def get_cards_without_manufacturer_order_id(self) -> List[str]:
        """Get application IDs for cards that need manufacturer updates"""
        application_ids = []
        customers = list(self.db_manager.customers_collection.find())
        
        for customer in customers:
            for card in customer.get("cards", []):
                tracking_ids = card.get("tracking_ids", {})
                if (tracking_ids.get("application_id") and 
                    not tracking_ids.get("manufacturer_order_id")):
                    application_ids.append(tracking_ids["application_id"])
        
        return application_ids
    
    def get_tracking_numbers_for_active_shipments(self) -> List[str]:
        """Get tracking numbers for cards that are not yet delivered"""
        tracking_numbers = []
        customers = list(self.db_manager.customers_collection.find())
        
        for customer in customers:
            for card in customer.get("cards", []):
                current_status = card.get("current_status", {}).get("status")
                tracking_number = card.get("tracking_ids", {}).get("logistics_tracking_number")
                
                if (tracking_number and 
                    current_status not in ["DELIVERED", "RETURNED_TO_SENDER"]):
                    tracking_numbers.append(tracking_number)
        
        return tracking_numbers

    # Analytics and Reporting
    def print_stats(self):
        """Print processing statistics"""
        print(f"\n📊 Processing Stats:")
        for key, value in self.stats.items():
            print(f"  {key.replace('_', ' ').title()}: {value}")

    def print_analytics(self):
        """Print analytics from MongoDB"""
        print(f"\n📈 Analytics:")
        
        # Get total counts
        total_customers = self.db_manager.customers_collection.count_documents({})
        print(f"Total Customers: {total_customers}")
        
        # Count total cards and pending stages summary
        pipeline = [
            {"$unwind": "$cards"},
            {"$group": {
                "_id": None,
                "total_cards": {"$sum": 1},
                "active_cards": {
                    "$sum": {
                        "$cond": [{"$eq": ["$cards.tracking_status", "active"]}, 1, 0]
                    }
                },
                "completed_cards": {
                    "$sum": {
                        "$cond": [{"$eq": ["$cards.tracking_status", "completed"]}, 1, 0]
                    }
                }
            }}
        ]
        
        card_summary = list(self.db_manager.customers_collection.aggregate(pipeline))
        if card_summary:
            summary = card_summary[0]
            print(f"Total Cards: {summary.get('total_cards', 0)}")
            print(f"Active Cards: {summary.get('active_cards', 0)}")
            print(f"Completed Cards: {summary.get('completed_cards', 0)}")
        
        # Status summary
        status_summary = self.db_manager.get_status_summary()
        if status_summary:
            print(f"\n📊 Status Breakdown:")
            for status, count in status_summary.items():
                print(f"  {status}: {count}")
        
        # Stage summary
        stage_pipeline = [
            {"$unwind": "$cards"},
            {"$group": {
                "_id": "$cards.current_status.stage",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        stage_summary = list(self.db_manager.customers_collection.aggregate(stage_pipeline))
        if stage_summary:
            print(f"\n📋 Stage Breakdown:")
            for item in stage_summary:
                stage = item["_id"] or "Unknown"
                count = item["count"]
                print(f"  {stage}: {count}")
        
        # Pending stages summary
        pending_pipeline = [
            {"$unwind": "$cards"},
            {"$unwind": "$cards.pending_stages"},
            {"$group": {
                "_id": "$cards.pending_stages",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        pending_summary = list(self.db_manager.customers_collection.aggregate(pending_pipeline))
        if pending_summary:
            print(f"\n⏳ Pending Stages Summary:")
            for item in pending_summary:
                pending_stage = item["_id"]
                count = item["count"]
                print(f"  {pending_stage}: {count} cards")
        
        # Bank performance
        bank_performance = self.db_manager.get_bank_performance()
        if bank_performance:
            print(f"\n🏦 Bank Performance:")
            for bank, perf in bank_performance.items():
                completion_rate = (perf["completed"] / perf["total"] * 100) if perf["total"] > 0 else 0
                print(f"  {bank}: {perf['completed']}/{perf['total']} ({completion_rate:.1f}%)")

    def show_notifications(self):
        """Show pending notifications"""
        notifications = self.db_manager.get_pending_notifications(20)
        print(f"\n📱 {len(notifications)} pending notifications:")
        for notif in notifications:
            print(f"  {notif.get('customer_name', 'Unknown')}: {notif.get('status')} - {notif.get('description')}")
    
    def show_pending_stages_summary(self):
        """Show detailed pending stages analysis"""
        print(f"\n⏳ Detailed Pending Stages Analysis:")
        
        customers = list(self.db_manager.customers_collection.find())
        stage_details = {stage: [] for stage in STAGE_ORDER}
        
        for customer in customers:
            for card in customer.get("cards", []):
                pending_stages = card.get("pending_stages", [])
                application_id = card.get("tracking_ids", {}).get("application_id", "Unknown")
                customer_name = customer.get("customer_info", {}).get("name", "Unknown")
                
                for pending_stage in pending_stages:
                    if pending_stage in stage_details:
                        stage_details[pending_stage].append({
                            "application_id": application_id,
                            "customer": customer_name,
                            "current_status": card.get("current_status", {}).get("status", "Unknown")
                        })
        
        for stage, cards in stage_details.items():
            if cards:
                print(f"\n  {stage} ({len(cards)} cards pending):")
                for card_info in cards[:5]:  # Show first 5
                    print(f"    - {card_info['application_id']} ({card_info['customer']}) - {card_info['current_status']}")
                if len(cards) > 5:
                    print(f"    ... and {len(cards) - 5} more")