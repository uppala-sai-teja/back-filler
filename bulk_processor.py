import json
import argparse
import logging
import re
import os
from datetime import datetime
from jsonpath_ng import parse
from typing import Dict, List, Any, Optional, Generator

# Configuration files
LOCAL_STATE_FILE = "local_db_state.json"
MASTER_CONFIG_FILE = "master_config.json"
LOG_FILE = "processor.log"

# Required fields for validation
REQUIRED_FIELDS = {
    "bank": ["customer_id", "application_id", "status"],
    "card_manufacturer": ["application_id"],
    "logistics": ["logistics_tracking_number"]
}

class CardTrackingProcessor:
    def __init__(self, debug=False):
        self.setup_logging(debug)
        self.stats = {"processed": 0, "errors": 0, "skipped": 0}
        
    def setup_logging(self, debug):
        """Setup logging for development"""
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

    def load_json_file(self, file_path: str) -> Optional[Dict]:
        """Load JSON file with error handling"""
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
        """Save JSON file with backup"""
        try:
            # Create backup if file exists
            if os.path.exists(file_path):
                backup_file = f"{file_path}.backup"
                os.rename(file_path, backup_file)
                self.logger.debug(f"Created backup: {backup_file}")
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            self.logger.info(f"Saved data to {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving {file_path}: {e}")
            return False

    def get_template(self, provider_type: str) -> Optional[Dict]:
        """Get default template for provider type"""
        config = self.load_json_file(MASTER_CONFIG_FILE)
        if not config:
            self.logger.error("Could not load master configuration")
            return None
        
        template = config.get(provider_type, {}).get("default")
        if not template:
            self.logger.error(f"No default template found for {provider_type}")
            return None
            
        self.logger.debug(f"Loaded template for {provider_type}")
        return template

    def normalize_phone_number(self, phone: str) -> str:
        """Normalize phone number to +91XXXXXXXXXX format"""
        if not phone:
            return ""
        
        # Remove all non-digits
        digits = re.sub(r'\D', '', str(phone))
        
        # Handle different input formats
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
        
        # Try common date patterns
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
        
        self.logger.warning(f"Could not parse date: {date_str}")
        return date_str

    def extract_fields(self, raw_data: Dict, field_mappings: Dict) -> Dict:
        """Extract fields using JSONPath"""
        extracted = {}
        
        for key, path in field_mappings.items():
            try:
                matches = [match.value for match in parse(path).find(raw_data)]
                if matches:
                    extracted[key] = matches[0]
                    self.logger.debug(f"Extracted {key}: {extracted[key]}")
            except Exception as e:
                self.logger.warning(f"Error extracting {key} from {path}: {e}")
        
        return extracted

    def validate_data(self, data: Dict, provider_type: str) -> List[str]:
        """Validate extracted data"""
        errors = []
        required = REQUIRED_FIELDS.get(provider_type, [])
        
        # Check required fields
        for field in required:
            if not data.get(field):
                errors.append(f"Missing required field: {field}")
        
        # Validate phone format (if present)
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
            self.logger.warning(f"No mapping for status: {raw_status}")
            return None
        
        # Get timestamp from various fields
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
            "raw_status": raw_status,
            "provider": template.get("provider_name"),
            "metadata": {
                "courier_partner": data.get("courier_partner"),
                "tracking_number": data.get("tracking_number"),
                "batch_number": data.get("production_batch")
            }
        }

    def process_data(self, raw_data: Dict, template: Dict) -> Generator[Dict, None, None]:
        """Process raw data and yield events"""
        try:
            # Extract base fields
            base_data = self.extract_fields(raw_data, template.get("field_mappings", {}))
            
            # Normalize common fields
            if "mobile" in base_data:
                base_data["mobile"] = self.normalize_phone_number(base_data["mobile"])
            
            # Handle history-based data (manufacturer/logistics)
            history_field = template.get("history_field")
            if history_field and history_field in raw_data:
                history_items = raw_data.get(history_field, [])
                history_mappings = template.get("history_mappings", {})
                
                self.logger.debug(f"Processing {len(history_items)} history events")
                
                for item in history_items:
                    processed = base_data.copy()
                    
                    # Extract history-specific fields
                    for hist_key, hist_path in history_mappings.items():
                        if hist_path in item:
                            processed[hist_key] = item[hist_path]
                    
                    # Create and yield timeline event
                    timeline_event = self.create_timeline_event(processed, template)
                    if timeline_event:
                        processed["timeline_event"] = timeline_event
                        yield processed
            else:
                # Single event processing (bank data)
                timeline_event = self.create_timeline_event(base_data, template)
                if timeline_event:
                    base_data["timeline_event"] = timeline_event
                    yield base_data
                    
        except Exception as e:
            self.logger.error(f"Error processing data: {e}")
            self.stats["errors"] += 1

    def find_card_and_customer(self, state: Dict, template: Dict, data: Dict) -> tuple:
        """Find existing card and customer"""
        lookup_key = template.get("lookup_key")
        lookup_value = data.get(lookup_key)
        
        if not lookup_value:
            return None, None
        
        provider_type = template.get("provider_type")
        
        # Bank data: lookup by customer_id first
        if provider_type == "bank":
            customer_id = data.get("customer_id")
            if customer_id in state:
                application_id = data.get("application_id")
                for card in state[customer_id].get("cards", []):
                    if card.get("tracking_ids", {}).get("application_id") == application_id:
                        return card, customer_id
            return None, customer_id
        
        # Manufacturer/Logistics: search across all customers
        for customer_id, customer_doc in state.items():
            for card in customer_doc.get("cards", []):
                tracking_ids = card.get("tracking_ids", {})
                if tracking_ids.get(lookup_key) == lookup_value:
                    return card, customer_id
        
        return None, None

    def create_new_card(self, data: Dict, template: Dict) -> Dict:
        """Create new card entry"""
        timestamp = datetime.now().isoformat() + "Z"
        
        return {
            "card_id": f"CARD_{data.get('application_id', 'UNK')}_{int(datetime.now().timestamp())}",
            "tracking_ids": {
                "application_id": data.get("application_id"),
                "customer_id": data.get("customer_id")
            },
            "tracking_status": "active",
            "card_info": {
                "bank_name": template.get("provider_name", "Unknown Bank"),
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
            "metadata": {
                "created_at": timestamp,
                "last_updated": timestamp,
                "priority": "standard"
            }
        }

    def update_state(self, state: Dict, data: Dict, template: Dict) -> Dict:
        """Update state with new timeline event"""
        timeline_event = data.get("timeline_event")
        if not timeline_event:
            return state
        
        # Find or create card
        card, customer_id = self.find_card_and_customer(state, template, data)
        
        # Create new customer/card if needed (bank data only)
        if not card and template.get("provider_type") == "bank":
            customer_id = data.get("customer_id")
            
            # Create customer if doesn't exist
            if customer_id not in state:
                state[customer_id] = {
                    "_id": customer_id,
                    "customer_info": {
                        "name": data.get("customer_name", "Unknown"),
                        "mobile": data.get("mobile", ""),
                        "email": data.get("email", "")
                    },
                    "cards": [],
                    "metadata": {
                        "created_at": datetime.now().isoformat() + "Z",
                        "last_updated": datetime.now().isoformat() + "Z"
                    }
                }
                self.logger.info(f"Created new customer: {customer_id}")
            
            # Create new card
            card = self.create_new_card(data, template)
            state[customer_id]["cards"].append(card)
            self.logger.info(f"Created new card: {card['card_id']}")
        
        if not card:
            self.logger.warning(f"Could not find card for {template.get('lookup_key')}: {data.get(template.get('lookup_key'))}")
            self.stats["skipped"] += 1
            return state
        
        # Add timeline event
        stage = timeline_event["stage"]
        timeline_list = card["timeline"].setdefault(stage, [])
        
        # Check for duplicates
        new_timestamp = timeline_event["timestamp"]
        if timeline_list and new_timestamp <= timeline_list[-1].get("timestamp", ""):
            self.logger.debug(f"Skipping duplicate/old event: {timeline_event['status']}")
            self.stats["skipped"] += 1
            return state
        
        # Add new event
        timeline_list.append(timeline_event)
        self.logger.info(f"Added event: {timeline_event['status']} to {card['card_id']}")
        
        # Update current status
        card["current_status"] = {
            "stage": timeline_event["status"],
            "location": timeline_event["location"],
            "last_updated": timeline_event["timestamp"],
            "description": timeline_event["description"]
        }
        
        # Update tracking IDs from manufacturer/logistics
        if template.get("provider_type") == "card_manufacturer":
            if data.get("manufacturer_order_id"):
                card["tracking_ids"]["manufacturer_order_id"] = data["manufacturer_order_id"]
            if data.get("tracking_number"):
                card["tracking_ids"]["logistics_tracking_number"] = data["tracking_number"]
        
        # Mark as completed for final statuses
        final_statuses = ["DELIVERED", "APPLICATION_REJECTED", "RETURNED_TO_SENDER"]
        if timeline_event["status"] in final_statuses:
            card["tracking_status"] = "completed"
        
        # Update timestamps
        now = datetime.now().isoformat() + "Z"
        card["metadata"]["last_updated"] = now
        state[customer_id]["metadata"]["last_updated"] = now
        
        self.stats["processed"] += 1
        return state

    def process_bulk_data(self, bulk_data: List[Dict], template: Dict) -> Dict:
        """Process bulk data with error handling"""
        self.logger.info(f"Starting processing of {len(bulk_data)} records")
        
        # Load existing state
        state = self.load_json_file(LOCAL_STATE_FILE) or {}
        provider_type = template.get("provider_type")
        
        for i, record in enumerate(bulk_data):
            self.logger.debug(f"Processing record {i+1}/{len(bulk_data)}")
            
            try:
                # Process each event from record
                for processed_data in self.process_data(record, template):
                    # Validate data
                    validation_errors = self.validate_data(processed_data, provider_type)
                    if validation_errors:
                        self.logger.warning(f"Validation errors in record {i+1}: {validation_errors}")
                        self.stats["errors"] += 1
                        continue
                    
                    # Update state
                    state = self.update_state(state, processed_data, template)
                    
            except Exception as e:
                self.logger.error(f"Error in record {i+1}: {e}")
                self.stats["errors"] += 1
                continue
        
        return state

    def print_stats(self):
        """Print processing statistics"""
        print(f"\n📊 Processing Statistics:")
        print(f"   ✅ Events Processed: {self.stats['processed']}")
        print(f"   ❌ Errors: {self.stats['errors']}")
        print(f"   ⏭️  Skipped: {self.stats['skipped']}")

    def print_state_summary(self, state: Dict):
        """Print current state summary"""
        if not state:
            print("\n📭 No tracking data found")
            return
        
        total_customers = len(state)
        total_cards = sum(len(customer.get("cards", [])) for customer in state.values())
        
        print(f"\n📈 Current State Summary:")
        print(f"   👥 Total Customers: {total_customers}")
        print(f"   💳 Total Cards: {total_cards}")
        
        # Show status breakdown
        status_counts = {}
        for customer in state.values():
            for card in customer.get("cards", []):
                status = card.get("current_status", {}).get("stage", "Unknown")
                status_counts[status] = status_counts.get(status, 0) + 1
        
        if status_counts:
            print(f"   📊 Status Breakdown:")
            for status, count in status_counts.items():
                print(f"      {status}: {count}")

    def print_detailed_state(self, state: Dict):
        """Print detailed state for debugging"""
        print(f"\n--- DETAILED STATE ---")
        
        for customer_id, customer_data in state.items():
            customer_info = customer_data.get("customer_info", {})
            print(f"\n👤 {customer_info.get('name', 'Unknown')} ({customer_id})")
            print(f"   📱 Mobile: {customer_info.get('mobile', 'N/A')}")
            print(f"   📧 Email: {customer_info.get('email', 'N/A')}")
            
            for i, card in enumerate(customer_data.get("cards", [])):
                card_info = card.get("card_info", {})
                current_status = card.get("current_status", {})
                tracking_ids = card.get("tracking_ids", {})
                
                print(f"\n   💳 Card {i+1}: {card_info.get('bank_name')} {card_info.get('card_variant')} {card_info.get('card_type')}")
                print(f"      Status: {current_status.get('stage', 'Unknown')}")
                print(f"      Location: {current_status.get('location', 'Unknown')}")
                print(f"      Updated: {current_status.get('last_updated', 'Unknown')}")
                print(f"      App ID: {tracking_ids.get('application_id', 'N/A')}")
                if tracking_ids.get("logistics_tracking_number"):
                    print(f"      Tracking: {tracking_ids.get('logistics_tracking_number')}")
                
                # Show timeline summary
                timeline = card.get("timeline", {})
                for stage, events in timeline.items():
                    if events:
                        print(f"      {stage.replace('_', ' ').title()}: {len(events)} events")

def create_test_files():
    """Create test data files for quick testing"""
    test_cases = {
        "test_bank_data.json": [
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
                "application_id": "APP_001",
                "status": "approved",
                "approval_date": "2025-08-15T14:30:00Z"
            }
        ],
        
        "test_manufacturer_data.json": [
            {
                "bank_reference": "APP_001",
                "order_id": "MFG_001",
                "batch_number": "BATCH_001",
                "facility": "Chennai Unit",
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
            }
        ],
        
        "test_logistics_data.json": [
            {
                "awb_number": "DTDC123456789",
                "recipient_name": "John Doe",
                "recipient_mobile": "+919876543210",
                "tracking_history": [
                    {
                        "status": "picked_up",
                        "timestamp": "2025-08-17T10:00:00Z",
                        "location": "Chennai Hub"
                    },
                    {
                        "status": "in_transit",
                        "timestamp": "2025-08-18T08:00:00Z",
                        "location": "Mumbai Hub"
                    },
                    {
                        "status": "out_for_delivery",
                        "timestamp": "2025-08-18T09:30:00Z",
                        "location": "Mumbai Local Hub"
                    },
                    {
                        "status": "delivered",
                        "timestamp": "2025-08-18T14:45:00Z",
                        "location": "Customer Address"
                    }
                ]
            }
        ]
    }
    
    for filename, data in test_cases.items():
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"✅ Created {filename}")

def main():
    parser = argparse.ArgumentParser(
        description="Card Tracking Template Processor - Hackathon Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
            python processor.py test_bank_data.json --type bank
            python processor.py test_manufacturer_data.json --type card_manufacturer  
            python processor.py test_logistics_data.json --type logistics
            python processor.py --show-state
            python processor.py --reset
            python processor.py --create-test-files
        """
    )
    
    parser.add_argument("input_file", nargs='?', help="Input JSON data file")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'],
                       help="Type of data to process")
    parser.add_argument("--reset", action="store_true", 
                       help="Reset local state (clear all data)")
    parser.add_argument("--show-state", action="store_true",
                       help="Show current tracking state")
    parser.add_argument("--detailed", action="store_true",
                       help="Show detailed state with timelines")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")
    parser.add_argument("--create-test-files", action="store_true",
                       help="Create sample test files")
    
    args = parser.parse_args()
    
    # Handle utility commands
    if args.create_test_files:
        create_test_files()
        return
    
    if args.reset:
        if os.path.exists(LOCAL_STATE_FILE):
            os.remove(LOCAL_STATE_FILE)
            print("✅ Local state reset successfully")
        if os.path.exists(f"{LOCAL_STATE_FILE}.backup"):
            os.remove(f"{LOCAL_STATE_FILE}.backup")
        return
    
    # Initialize processor
    processor = CardTrackingProcessor(debug=args.debug)
    
    if args.show_state:
        state = processor.load_json_file(LOCAL_STATE_FILE) or {}
        processor.print_state_summary(state)
        if args.detailed:
            processor.print_detailed_state(state)
        return
    
    # Process data
    if not args.input_file or not args.type:
        parser.print_help()
        print(f"\n💡 Tip: Run with --create-test-files to generate sample data")
        return
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"❌ Input file not found: {args.input_file}")
        if args.input_file.startswith("test_"):
            print("💡 Run with --create-test-files first to generate test data")
        return
    
    # Get template
    template = processor.get_template(args.type)
    if not template:
        print(f"❌ Could not load template for {args.type}")
        return
    
    # Load input data
    input_data = processor.load_json_file(args.input_file)
    if not input_data:
        return
    
    print(f"🚀 Processing {len(input_data)} records from {args.input_file}")
    print(f"📝 Using {template['provider_name']} template")
    
    # Process data
    final_state = processor.process_bulk_data(input_data, template)
    
    # Save results
    if processor.save_json_file(LOCAL_STATE_FILE, final_state):
        processor.print_stats()
        processor.print_state_summary(final_state)
        
        if args.debug or args.detailed:
            processor.print_detailed_state(final_state)
        
        print(f"\n✅ Processing complete! State saved to {LOCAL_STATE_FILE}")
        print(f"📄 Logs written to {LOG_FILE}")

if __name__ == "__main__":
    main()