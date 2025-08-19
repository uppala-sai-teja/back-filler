import json
import argparse
import logging
import re
import os
from datetime import datetime, timedelta
from jsonpath_ng import parse
from typing import Dict, List, Any, Optional, Generator
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

# Configuration files
LOCAL_STATE_FILE = "local_db_state.json"
MASTER_CONFIG_FILE = "master_config.json"
LOG_FILE = "processor.log"
NOTIFICATIONS_FILE = "notifications.json"

# Required fields for validation
REQUIRED_FIELDS = {
    "bank": ["customer_id", "application_id", "status"],
    "card_manufacturer": ["application_id"],
    "logistics": ["logistics_tracking_number"]
}

# Status hierarchy for progression validation
STATUS_HIERARCHY = {
    "application_and_approval": [
        "APPLICATION_SUBMITTED", "APPLICATION_PROCESSING", 
        "APPLICATION_APPROVED", "APPLICATION_REJECTED"
    ],
    "card_production": [
        "PRODUCTION_QUEUED", "PRODUCTION_STARTED", "CARD_EMBOSSING",
        "QUALITY_CHECK", "CARD_PERSONALIZED"
    ],
    "shipping_and_delivery": [
        "DISPATCHED", "PICKED_UP", "IN_TRANSIT", "REACHED_HUB",
        "OUT_FOR_DELIVERY", "DELIVERED", "DELIVERY_FAILED", "RETURNED_TO_SENDER"
    ]
}


class CardTrackingProcessor:
    def __init__(self, debug=False):
        self.setup_logging(debug)
        self.stats = {"processed": 0, "errors": 0, "skipped": 0, "notifications_sent": 0}
        self.notification_queue = []
        
    def setup_logging(self, debug):
        level = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    # ------------------------- File Handling -------------------------

    def load_json_file(self, file_path: str) -> Optional[Dict]:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                self.logger.debug(f"Loaded {file_path} successfully")
                return data
        except FileNotFoundError:
            self.logger.warning(f"File not found: {file_path}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {file_path}: {e}")
            return None

    def save_json_file(self, file_path: str, data: Dict) -> bool:
        try:
            if os.path.exists(file_path):
                backup_file = f"{file_path}.backup"
                if os.path.exists(backup_file):
                    os.remove(backup_file)
                os.rename(file_path, backup_file)
                self.logger.debug(f"Created/overwritten backup: {backup_file}")
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            self.logger.info(f"Saved data to {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving {file_path}: {e}")
            return False

    def get_template(self, provider_type: str) -> Optional[Dict]:
        config = self.load_json_file(MASTER_CONFIG_FILE)
        if not config:
            self.logger.error("Could not load master configuration")
            return None
        template = config.get(provider_type, {}).get("default")
        if not template:
            self.logger.error(f"No default template found for {provider_type}")
            return None
        return template

    # ------------------------- Enhanced Normalizers -------------------------

    def normalize_phone_number(self, phone: str) -> str:
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

    def calculate_estimated_delivery(self, current_status: str, location: str = "") -> Optional[str]:
        """Calculate estimated delivery date based on current status"""
        base_date = datetime.now()
        
        if current_status == "APPLICATION_APPROVED":
            # 5-7 days for production + shipping
            estimated = base_date + timedelta(days=6)
        elif current_status in ["PRODUCTION_QUEUED", "PRODUCTION_STARTED"]:
            # 3-5 days for remaining production + shipping
            estimated = base_date + timedelta(days=4)
        elif current_status == "CARD_PERSONALIZED":
            # 2-3 days for shipping
            estimated = base_date + timedelta(days=3)
        elif current_status == "DISPATCHED":
            # 1-2 days for delivery
            estimated = base_date + timedelta(days=2)
        elif current_status in ["IN_TRANSIT", "REACHED_HUB"]:
            # Same day or next day
            estimated = base_date + timedelta(days=1)
        elif current_status == "OUT_FOR_DELIVERY":
            # Same day
            estimated = base_date
        else:
            return None
            
        return estimated.isoformat() + "Z"

    # ------------------------- Enhanced Validation -------------------------

    def validate_status_progression(self, card: Dict, new_status: str, new_stage: str) -> bool:
        """Validate that status progression is logical"""
        current_status = card.get("current_status", {}).get("status")
        
        if not current_status:
            return True  # First status, always valid
            
        # Check if we're going backwards in the same stage
        stage_statuses = STATUS_HIERARCHY.get(new_stage, [])
        if current_status in stage_statuses and new_status in stage_statuses:
            current_idx = stage_statuses.index(current_status)
            new_idx = stage_statuses.index(new_status)
            if new_idx < current_idx:
                self.logger.warning(f"Status going backwards: {current_status} -> {new_status}")
                return False
                
        return True

    def extract_fields(self, raw_data: Dict, field_mappings: Dict) -> Dict:
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
        errors = []
        required = REQUIRED_FIELDS.get(provider_type, [])
        for field in required:
            if not data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Enhanced mobile validation
        mobile = data.get("mobile")
        if mobile and not re.match(r'^\+91[6-9]\d{9}$', mobile):
            errors.append(f"Invalid mobile format: {mobile}")
            
        # Email validation
        email = data.get("email")
        if email and not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
            errors.append(f"Invalid email format: {email}")
            
        return errors

    # ------------------------- Notification System -------------------------

    def queue_notification(self, customer_info: Dict, card: Dict, event: Dict):
        """Queue notification for status changes"""
        notification = {
            "customer_id": customer_info.get("_id"),
            "customer_name": customer_info.get("customer_info", {}).get("name"),
            "mobile": customer_info.get("customer_info", {}).get("mobile"),
            "email": customer_info.get("customer_info", {}).get("email"),
            "card_id": card.get("card_id"),
            "card_type": card.get("card_info", {}).get("card_type"),
            "status": event.get("status"),
            "description": event.get("description"),
            "timestamp": event.get("timestamp"),
            "notification_type": self.get_notification_type(event.get("status")),
            "sent": False
        }
        self.notification_queue.append(notification)

    def get_notification_type(self, status: str) -> List[str]:
        """Determine notification channels based on status"""
        critical_statuses = ["APPLICATION_APPROVED", "APPLICATION_REJECTED", 
                           "DISPATCHED", "OUT_FOR_DELIVERY", "DELIVERED"]
        
        if status in critical_statuses:
            return ["sms", "email", "push"]
        else:
            return ["push"]

    def save_notifications(self):
        """Save notification queue to file"""
        if self.notification_queue:
            existing = self.load_json_file(NOTIFICATIONS_FILE) or []
            existing.extend(self.notification_queue)
            self.save_json_file(NOTIFICATIONS_FILE, existing)
            self.stats["notifications_sent"] += len(self.notification_queue)
            self.notification_queue = []

    # ------------------------- Event Creation -------------------------

    def create_timeline_event(self, data: Dict, template: Dict) -> Optional[Dict]:
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
        
        # Simplified timeline event - no metadata here
        return {
            "status": status_mapping["status"],
            "stage": status_mapping["stage"],
            "timestamp": self.normalize_date(timestamp),
            "description": status_mapping["description"],
            "location": data.get("location", data.get("facility_location", "Unknown")),
            "provider": template.get("provider_name")
        }

    def process_data(self, raw_data: Dict, template: Dict) -> Generator[Dict, None, None]:
        try:
            base_data = self.extract_fields(raw_data, template.get("field_mappings", {}))
            if "mobile" in base_data:
                base_data["mobile"] = self.normalize_phone_number(base_data["mobile"])
            
            history_field = template.get("history_field")
            if history_field and history_field in raw_data:
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
                timeline_event = self.create_timeline_event(base_data, template)
                if timeline_event:
                    base_data["timeline_event"] = timeline_event
                    yield base_data
        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            self.stats["errors"] += 1

    # ------------------------- Card/Customer Helpers -------------------------

    def move_card_to_customer(self, state, application_id, target_customer_id):
        if not application_id or target_customer_id is None:
            return None
        for cust_id, cust_doc in list(state.items()):
            for idx, card in enumerate(cust_doc.get("cards", [])):
                if card.get("tracking_ids", {}).get("application_id") == application_id:
                    if target_customer_id not in state:
                        state[target_customer_id] = {
                            "_id": target_customer_id,
                            "customer_info": {"name": "Unknown", "mobile": "", "email": ""},
                            "cards": [],
                            "metadata": {"created_at": datetime.now().isoformat()+"Z",
                                         "last_updated": datetime.now().isoformat()+"Z"}
                        }
                    moved = cust_doc["cards"].pop(idx)
                    state[target_customer_id]["cards"].append(moved)
                    if not cust_doc.get("cards") and str(cust_id).startswith("CUST_UNK_"):
                        state.pop(cust_id, None)
                    self.logger.info(f"Moved card {moved.get('card_id')} from {cust_id} to {target_customer_id}")
                    return moved
        return None

    def find_card_and_customer(self, state: Dict, template: Dict, data: Dict) -> tuple:
        lookup_key = template.get("lookup_key")
        lookup_value = data.get(lookup_key)
        if not lookup_value:
            return None, None
        
        provider_type = template.get("provider_type")
        if provider_type == "bank":
            customer_id = data.get("customer_id")
            if customer_id in state:
                application_id = data.get("application_id")
                for card in state[customer_id].get("cards", []):
                    if card.get("tracking_ids", {}).get("application_id") == application_id:
                        return card, customer_id
            return None, customer_id
        
        for customer_id, customer_doc in state.items():
            for card in customer_doc.get("cards", []):
                if card.get("tracking_ids", {}).get(lookup_key) == lookup_value:
                    return card, customer_id
        return None, None

    def create_new_card(self, data: Dict, template: Dict) -> Dict:
        timestamp = datetime.now().isoformat() + "Z"
        bank_label = (template.get("provider_name", "Bank") 
                     if template.get("provider_type") == "bank" 
                     else (data.get("bank_name") or "Bank"))
        
        return {
            "card_id": f"CARD_{data.get('application_id', data.get('logistics_tracking_number', 'UNK'))}_{int(datetime.now().timestamp())}",
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
            "delivery_info": {},
            "estimated_delivery": None,
            # Centralized metadata for the entire application
            "application_metadata": {
                "courier_partner": None,
                "current_tracking_number": None,
                "production_batch": None,
                "facility_location": None,
                "priority": "standard",
                "alerts": [],
                "processing_notes": []
            },
            "metadata": {
                "created_at": timestamp, 
                "last_updated": timestamp
            }
        }

    # ------------------------- Enhanced State Updates -------------------------

    def update_state(self, state: Dict, data: Dict, template: Dict) -> Dict:
        timeline_event = data.get("timeline_event")
        if not timeline_event:
            return state
        
        provider_type = template.get("provider_type")
        lookup_key = template.get("lookup_key")

        card, customer_id = self.find_card_and_customer(state, template, data)

        # Bank ingestion
        if provider_type == "bank":
            real_customer_id = data.get("customer_id")
            if not card and data.get("application_id"):
                migrated = self.move_card_to_customer(state, data.get("application_id"), real_customer_id)
                if migrated:
                    card, customer_id = migrated, real_customer_id
            
            if real_customer_id not in state:
                state[real_customer_id] = {
                    "_id": real_customer_id,
                    "customer_info": {
                        "name": data.get("customer_name", "Unknown"),
                        "mobile": data.get("mobile", ""),
                        "email": data.get("email", "")
                    },
                    "cards": [],
                    "metadata": {
                        "created_at": datetime.now().isoformat()+"Z",
                        "last_updated": datetime.now().isoformat()+"Z"
                    }
                }
            
            if not card:
                card = self.create_new_card(data, template)
                state[real_customer_id]["cards"].append(card)
                customer_id = real_customer_id

        # Manufacturer/Logistics ingestion before bank
        if provider_type in ("card_manufacturer", "logistics") and not card:
            placeholder_key = (data.get("application_id") or 
                             data.get("logistics_tracking_number") or 
                             data.get("tracking_number"))
            placeholder_customer_id = data.get("customer_id") or f"CUST_UNK_{placeholder_key}"
            
            if placeholder_customer_id not in state:
                state[placeholder_customer_id] = {
                    "_id": placeholder_customer_id,
                    "customer_info": {
                        "name": "Unknown", 
                        "mobile": data.get("recipient_mobile", ""), 
                        "email": ""
                    },
                    "cards": [],
                    "metadata": {
                        "created_at": datetime.now().isoformat()+"Z",
                        "last_updated": datetime.now().isoformat()+"Z",
                        "placeholder": True
                    }
                }
            
            card = self.create_new_card(data, template)
            state[placeholder_customer_id]["cards"].append(card)
            customer_id = placeholder_customer_id

        if not card:
            self.logger.warning(f"Could not find card for {lookup_key}: {data.get(lookup_key)}")
            self.stats["skipped"] += 1
            return state

        # Validate status progression
        if not self.validate_status_progression(card, timeline_event["status"], timeline_event["stage"]):
            self.stats["errors"] += 1
            return state

        # Timeline updates with enhanced deduplication
        stage = timeline_event["stage"]
        timeline_list = card["timeline"].setdefault(stage, [])
        new_timestamp = timeline_event["timestamp"]
        
        if timeline_list:
            last_event = timeline_list[-1]
            # More sophisticated deduplication
            if (new_timestamp <= last_event.get("timestamp", "") or 
                (last_event.get("status") == timeline_event["status"] and 
                 last_event.get("location") == timeline_event["location"])):
                self.stats["skipped"] += 1
                return state
        
        timeline_list.append(timeline_event)

        # Update current status
        card["current_status"] = {
            "status": timeline_event["status"],
            "stage": timeline_event["stage"],
            "location": timeline_event["location"],
            "last_updated": timeline_event["timestamp"],
            "description": timeline_event["description"]
        }

        # Update centralized application metadata
        if not card.get("application_metadata"):
            card["application_metadata"] = {
                "courier_partner": None,
                "current_tracking_number": None,
                "production_batch": None,
                "facility_location": None,
                "priority": "standard",
                "alerts": [],
                "processing_notes": []
            }

        # Update application-level metadata based on provider data
        app_metadata = card["application_metadata"]
        
        if data.get("courier_partner"):
            app_metadata["courier_partner"] = data["courier_partner"]
        if data.get("tracking_number"):
            app_metadata["current_tracking_number"] = data["tracking_number"]
        if data.get("production_batch"):
            app_metadata["production_batch"] = data["production_batch"]
        if data.get("facility_location") or data.get("location"):
            app_metadata["facility_location"] = data.get("facility_location") or data.get("location")

        # Update estimated delivery only when status changes meaningfully
        if timeline_event["status"] in ["APPLICATION_APPROVED", "PRODUCTION_QUEUED", 
                                       "CARD_PERSONALIZED", "DISPATCHED", "OUT_FOR_DELIVERY"]:
            estimated = self.calculate_estimated_delivery(
                timeline_event["status"], 
                timeline_event["location"]
            )
            if estimated:
                card["estimated_delivery"] = estimated

        # Update tracking IDs
        if provider_type == "card_manufacturer":
            if data.get("manufacturer_order_id"):
                card["tracking_ids"]["manufacturer_order_id"] = data["manufacturer_order_id"]
            if data.get("tracking_number"):
                card["tracking_ids"]["logistics_tracking_number"] = data["tracking_number"]
        elif provider_type == "logistics":
            if data.get("logistics_tracking_number"):
                card["tracking_ids"]["logistics_tracking_number"] = data["logistics_tracking_number"]

        # Queue notifications for important status changes
        if timeline_event["status"] in ["APPLICATION_APPROVED", "APPLICATION_REJECTED", 
                                       "DISPATCHED", "OUT_FOR_DELIVERY", "DELIVERED"]:
            self.queue_notification(state[customer_id], card, timeline_event)

        # Handle final statuses
        if timeline_event["status"] in ["DELIVERED", "APPLICATION_REJECTED", "RETURNED_TO_SENDER"]:
            card["tracking_status"] = "completed"

        # Update timestamps
        now = datetime.now().isoformat() + "Z"
        card["metadata"]["last_updated"] = now
        state[customer_id]["metadata"]["last_updated"] = now
        self.stats["processed"] += 1
        
        return state

    # ------------------------- Bulk Processor -------------------------

    def process_bulk_data(self, bulk_data: List[Dict], template: Dict) -> Dict:
        state = self.load_json_file(LOCAL_STATE_FILE) or {}
        provider_type = template.get("provider_type")
        
        for record in bulk_data:
            try:
                for processed_data in self.process_data(record, template):
                    validation_errors = self.validate_data(processed_data, provider_type)
                    if validation_errors:
                        self.logger.error(f"Validation errors: {validation_errors}")
                        self.stats["errors"] += 1
                        continue
                    state = self.update_state(state, processed_data, template)
            except Exception as e:
                self.logger.error(f"Error in record: {e}")
                self.stats["errors"] += 1
                continue
                
        # Save notifications after processing
        self.save_notifications()
        return state

    # ------------------------- Analytics & Reports -------------------------

    def generate_analytics(self, state: Dict) -> Dict:
        """Generate analytics from current state"""
        analytics = {
            "summary": {
                "total_customers": len(state),
                "total_cards": sum(len(customer.get("cards", [])) for customer in state.values()),
                "active_cards": 0,
                "completed_cards": 0
            },
            "status_breakdown": {},
            "stage_breakdown": {},
            "delivery_performance": {
                "on_time": 0,
                "delayed": 0,
                "avg_processing_days": 0
            },
            "bank_performance": {},
            "recent_activity": []
        }
        
        processing_times = []
        
        for customer in state.values():
            for card in customer.get("cards", []):
                # Card status counts
                status = card.get("tracking_status", "active")
                if status == "active":
                    analytics["summary"]["active_cards"] += 1
                else:
                    analytics["summary"]["completed_cards"] += 1
                
                # Current status breakdown
                current_status = card.get("current_status", {}).get("status", "Unknown")
                analytics["status_breakdown"][current_status] = analytics["status_breakdown"].get(current_status, 0) + 1
                
                # Stage breakdown
                current_stage = card.get("current_status", {}).get("stage", "Unknown")
                analytics["stage_breakdown"][current_stage] = analytics["stage_breakdown"].get(current_stage, 0) + 1
                
                # Bank performance
                bank = card.get("card_info", {}).get("bank_name", "Unknown")
                if bank not in analytics["bank_performance"]:
                    analytics["bank_performance"][bank] = {"total": 0, "completed": 0}
                analytics["bank_performance"][bank]["total"] += 1
                if status == "completed":
                    analytics["bank_performance"][bank]["completed"] += 1
                
                # Processing time calculation
                created = card.get("metadata", {}).get("created_at")
                last_updated = card.get("metadata", {}).get("last_updated")
                if created and last_updated:
                    try:
                        start = datetime.fromisoformat(created.replace('Z', '+00:00'))
                        end = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                        processing_times.append((end - start).days)
                    except:
                        pass
        
        if processing_times:
            analytics["delivery_performance"]["avg_processing_days"] = sum(processing_times) / len(processing_times)
        
        return analytics

    # ------------------------- CLI Summaries -------------------------

    def print_stats(self):
        print(f"\n📊 Processing Stats:")
        for key, value in self.stats.items():
            print(f"  {key.replace('_', ' ').title()}: {value}")

    def print_state_summary(self, state: Dict):
        if not state:
            print("\n📭 No tracking data found")
            return

        analytics = self.generate_analytics(state)
        summary = analytics["summary"]
        
        print(f"\n📈 Summary:")
        print(f"  Customers: {summary['total_customers']}")
        print(f"  Total Cards: {summary['total_cards']}")
        print(f"  Active: {summary['active_cards']}, Completed: {summary['completed_cards']}")
        
        print(f"\n📊 Current Status Breakdown:")
        for status, count in analytics["status_breakdown"].items():
            print(f"  {status}: {count}")
        
        print(f"\n🏦 Bank Performance:")
        for bank, perf in analytics["bank_performance"].items():
            completion_rate = (perf["completed"] / perf["total"] * 100) if perf["total"] > 0 else 0
            print(f"  {bank}: {perf['completed']}/{perf['total']} ({completion_rate:.1f}%)")
        
        avg_days = analytics["delivery_performance"]["avg_processing_days"]
        if avg_days > 0:
            print(f"\n⏱️  Average Processing Time: {avg_days:.1f} days")


# ------------------------- CLI Entry -------------------------

def main():
    parser = argparse.ArgumentParser(description="Enhanced Card Tracking Processor")
    parser.add_argument("input_file", nargs='?', help="Input JSON data file")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'], 
                       help="Type of data to process")
    parser.add_argument("--reset", action="store_true", help="Reset local state")
    parser.add_argument("--show-state", action="store_true", help="Show current tracking state")
    parser.add_argument("--analytics", action="store_true", help="Show detailed analytics")
    parser.add_argument("--notifications", action="store_true", help="Show pending notifications")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.reset:
        files_to_remove = [LOCAL_STATE_FILE, f"{LOCAL_STATE_FILE}.backup", 
                          NOTIFICATIONS_FILE, LOG_FILE]
        for file in files_to_remove:
            if os.path.exists(file):
                os.remove(file)
        print("✅ All state files reset")
        return

    processor = CardTrackingProcessor(debug=args.debug)

    if args.notifications:
        notifications = processor.load_json_file(NOTIFICATIONS_FILE) or []
        print(f"\n📱 {len(notifications)} notifications in queue")
        for notif in notifications[-5:]:  # Show last 5
            print(f"  {notif['customer_name']}: {notif['status']} - {notif['description']}")
        return

    if args.show_state or args.analytics:
        state = processor.load_json_file(LOCAL_STATE_FILE) or {}
        if args.analytics:
            analytics = processor.generate_analytics(state)
            print(json.dumps(analytics, indent=2, default=str))
        else:
            processor.print_state_summary(state)
        return

    if not args.input_file or not args.type:
        parser.print_help()
        return

    if not os.path.exists(args.input_file):
        print(f"❌ File not found: {args.input_file}")
        return

    template = processor.get_template(args.type)
    if not template:
        print("❌ Could not load template")
        return

    input_data = processor.load_json_file(args.input_file)
    if not input_data:
        return

    print(f"🚀 Processing {len(input_data)} records from {args.input_file}")
    final_state = processor.process_bulk_data(input_data, template)
    
    if processor.save_json_file(LOCAL_STATE_FILE, final_state):
        processor.print_stats()
        processor.print_state_summary(final_state)
        print(f"\n✅ Done. State saved to {LOCAL_STATE_FILE}")
        
        # Show notification summary
        if processor.stats["notifications_sent"] > 0:
            print(f"📱 {processor.stats['notifications_sent']} notifications queued")


if __name__ == "__main__":
    main()