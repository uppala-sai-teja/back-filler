# core/card_processor.py
import json
import logging
import re
import os
from datetime import datetime, timedelta
from jsonpath_ng import parse
from typing import Dict, List, Optional, Generator
from .mongodb_manager import MongoDBManager

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
            "%Y-%m-%d"
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
        """Create new card record"""
        timestamp = datetime.now().isoformat() + "Z"
        bank_label = (template.get("provider_name", "Bank") 
                     if template.get("provider_type") == "bank" 
                     else (data.get("bank_name") or "Bank"))
        
        return {
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

    def update_card_with_event(self, customer: Dict, card: Dict, data: Dict, timeline_event: Dict) -> bool:
        """Update card with new timeline event"""
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

                        self.logger.debug(f"Looking for {lookup_key}: {lookup_value}")
                        self.logger.debug(f"Processed data: {processed_data}")
                        
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

    # Analytics
    def print_stats(self):
        """Print processing statistics"""
        print(f"\n📊 Processing Stats:")
        for key, value in self.stats.items():
            print(f"  {key.replace('_', ' ').title()}: {value}")

    def print_analytics(self):
        """Print analytics from MongoDB"""
        print(f"\n📈 Analytics:")
        
        # Status summary
        status_summary = self.db_manager.get_status_summary()
        print(f"Status Breakdown:")
        for status, count in status_summary.items():
            print(f"  {status}: {count}")
        
        # Bank performance
        bank_performance = self.db_manager.get_bank_performance()
        print(f"\n🏦 Bank Performance:")
        for bank, perf in bank_performance.items():
            completion_rate = (perf["completed"] / perf["total"] * 100) if perf["total"] > 0 else 0
            print(f"  {bank}: {perf['completed']}/{perf['total']} ({completion_rate:.1f}%)")