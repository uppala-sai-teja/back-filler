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
                if os.path.exists(backup_file):   # Windows-safe overwrite
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

    # ------------------------- Normalizers -------------------------

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
            "%Y-%m-%d"
        ]
        for pattern in patterns:
            try:
                dt = datetime.strptime(date_str, pattern)
                return dt.isoformat() + "Z"
            except ValueError:
                continue
        return date_str

    # ------------------------- Extractors -------------------------

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
        mobile = data.get("mobile")
        if mobile and not re.match(r'^\+91[6-9]\d{9}$', mobile):
            errors.append(f"Invalid mobile format: {mobile}")
        return errors

    # ------------------------- Event creation -------------------------

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
        return {
            "status": status_mapping["status"],
            "stage": status_mapping["stage"],
            "timestamp": self.normalize_date(timestamp),
            "description": status_mapping["description"],
            "location": data.get("location", data.get("facility_location", "Unknown")),
            "provider": template.get("provider_name"),
            "metadata": {
                "courier_partner": data.get("courier_partner"),
                "tracking_number": data.get("tracking_number"),
                "batch_number": data.get("production_batch")
            }
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

    # ------------------------- Card/Customer helpers -------------------------

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
        bank_label = template.get("provider_name", "Bank") if template.get("provider_type") == "bank" else (data.get("bank_name") or "Bank")
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
            "metadata": {"created_at": timestamp, "last_updated": timestamp, "priority": "standard"}
        }

    # ------------------------- State Updates -------------------------

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
                    "customer_info": {"name": data.get("customer_name", "Unknown"),
                                      "mobile": data.get("mobile", ""),
                                      "email": data.get("email", "")},
                    "cards": [],
                    "metadata": {"created_at": datetime.now().isoformat()+"Z",
                                 "last_updated": datetime.now().isoformat()+"Z"}
                }
            if not card:
                card = self.create_new_card(data, template)
                state[real_customer_id]["cards"].append(card)
                customer_id = real_customer_id

        # Manufacturer/Logistics ingestion before bank
        if provider_type in ("card_manufacturer", "logistics") and not card:
            placeholder_key = data.get("application_id") or data.get("logistics_tracking_number") or data.get("tracking_number")
            placeholder_customer_id = data.get("customer_id") or f"CUST_UNK_{placeholder_key}"
            if placeholder_customer_id not in state:
                state[placeholder_customer_id] = {
                    "_id": placeholder_customer_id,
                    "customer_info": {"name": "Unknown", "mobile": data.get("recipient_mobile", ""), "email": ""},
                    "cards": [],
                    "metadata": {"created_at": datetime.now().isoformat()+"Z",
                                 "last_updated": datetime.now().isoformat()+"Z",
                                 "placeholder": True}
                }
            card = self.create_new_card(data, template)
            state[placeholder_customer_id]["cards"].append(card)
            customer_id = placeholder_customer_id

        if not card:
            self.logger.warning(f"Could not find card for {lookup_key}: {data.get(lookup_key)}")
            self.stats["skipped"] += 1
            return state

        # Timeline updates (dedupe identical consecutive logistics statuses)
        stage = timeline_event["stage"]
        timeline_list = card["timeline"].setdefault(stage, [])
        new_timestamp = timeline_event["timestamp"]
        if timeline_list:
            last_event = timeline_list[-1]
            if new_timestamp <= last_event.get("timestamp", "") or last_event.get("status") == timeline_event["status"]:
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

        # Update IDs
        if provider_type == "card_manufacturer":
            if data.get("manufacturer_order_id"):
                card["tracking_ids"]["manufacturer_order_id"] = data["manufacturer_order_id"]
            if data.get("tracking_number"):
                card["tracking_ids"]["logistics_tracking_number"] = data["tracking_number"]
        elif provider_type == "logistics":
            if data.get("logistics_tracking_number"):
                card["tracking_ids"]["logistics_tracking_number"] = data["logistics_tracking_number"]

        # Final statuses
        if timeline_event["status"] in ["DELIVERED", "APPLICATION_REJECTED", "RETURNED_TO_SENDER"]:
            card["tracking_status"] = "completed"

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
                        self.stats["errors"] += 1
                        continue
                    state = self.update_state(state, processed_data, template)
            except Exception as e:
                self.logger.error(f"Error in record: {e}")
                self.stats["errors"] += 1
                continue
        return state

    # ------------------------- CLI Summaries -------------------------

    def print_stats(self):
        print(f"\n📊 Stats: {self.stats}")

    def print_state_summary(self, state: Dict):
        if not state:
            print("\n📭 No tracking data found")
            return

        total_customers = len(state)
        total_cards = sum(len(customer.get("cards", [])) for customer in state.values())
        print(f"\n📈 Customers: {total_customers}, Cards: {total_cards}")

        # Current status breakdown (latest only)
        status_counts = {}
        # Timeline breakdown (all events)
        timeline_counts = {}

        for customer in state.values():
            for card in customer.get("cards", []):
                # Count current status
                status = card.get("current_status", {}).get("status", "Unknown")
                status_counts[status] = status_counts.get(status, 0) + 1

                # Count all timeline statuses
                for stage, events in card.get("timeline", {}).items():
                    for ev in events:
                        s = ev.get("status", "Unknown")
                        timeline_counts[s] = timeline_counts.get(s, 0) + 1

        print("📊 Current Status Breakdown:", status_counts)
        print("📜 Timeline Status Breakdown:", timeline_counts)


# ------------------------- CLI Entry -------------------------

def main():
    parser = argparse.ArgumentParser(description="Card Tracking Processor")
    parser.add_argument("input_file", nargs='?', help="Input JSON data file")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'], help="Type of data to process")
    parser.add_argument("--reset", action="store_true", help="Reset local state")
    parser.add_argument("--show-state", action="store_true", help="Show current tracking state")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.reset:
        if os.path.exists(LOCAL_STATE_FILE):
            os.remove(LOCAL_STATE_FILE)
            print("✅ State reset")
        if os.path.exists(f"{LOCAL_STATE_FILE}.backup"):
            os.remove(f"{LOCAL_STATE_FILE}.backup")
        return

    processor = CardTrackingProcessor(debug=args.debug)

    if args.show_state:
        state = processor.load_json_file(LOCAL_STATE_FILE) or {}
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


if __name__ == "__main__":
    main()
