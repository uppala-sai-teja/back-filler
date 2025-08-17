import json
import argparse
from datetime import datetime
from jsonpath_ng import parse
import os

LOCAL_STATE_FILE = "local_db_state.json"
MASTER_CONFIG_FILE = "master_config.json"

def load_json_file(file_path):
    try:
        with open(file_path, 'r') as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading {file_path}: {e}")
        return None

def process_data(raw_data, template):
    """
    A generator that yields a processed data object for EACH event found.
    This handles both single-event inputs and inputs with a history array.
    """
    base_data = {}
    for key, path in template.get("field_mappings", {}).items():
        matches = [match.value for match in parse(path).find(raw_data)]
        if matches: base_data[key] = matches[0]

    history_field = template.get("history_field")
    history_mappings = template.get("history_mappings", {})
    history_items = raw_data.get(history_field, [])

    if history_items: # Process each event in the history array
        for item in history_items:
            processed = base_data.copy()
            raw_status = item.get(history_mappings.get("status"))
            processed["status"] = raw_status
            processed["location"] = item.get(history_mappings.get("location"))
            processed["timestamp"] = item.get(history_mappings.get("timestamp"))
            
            status_map = template.get("status_mappings", {}).get(raw_status)
            if status_map:
                processed["timeline_event"] = {
                    "status": status_map.get("status"), "stage": status_map.get("stage"),
                    "timestamp": processed.get("timestamp"), "description": f"Status updated to {status_map.get('status')}"
                }
                yield processed
    else: # Fallback to single-event processing
        processed = base_data.copy()
        raw_status = processed.get("status")
        if raw_status:
            status_map = template.get("status_mappings", {}).get(raw_status)
            if status_map:
                ts = processed.get("approval_date") or processed.get("dispatch_date") or processed.get("received_date") or processed.get("production_end_date") or processed.get("application_date") or datetime.now().isoformat()
                processed["timeline_event"] = {
                    "status": status_map.get("status"), "stage": status_map.get("stage"),
                    "timestamp": ts, "description": f"Status updated to {status_map.get('status')}"
                }
                yield processed

def find_card_and_customer(state, template, data):
    # This logic remains the same
    lookup_key = template.get("lookup_key")
    lookup_value = data.get(lookup_key)
    if template.get("provider_type") == "bank":
        application_id = data.get("application_id")
        customer_doc = state.get(lookup_value, {})
        for card in customer_doc.get("cards", []):
            if card.get("tracking_ids", {}).get("application_id") == application_id:
                return card, lookup_value
        return None, lookup_value
    for cid, cust_doc in state.items():
        for card in cust_doc.get("cards", []):
            if card.get("tracking_ids", {}).get(lookup_key) == lookup_value:
                return card, cid
    return None, None


def update_local_state(current_state, data, template):
    # This logic remains the same
    timeline_event = data.get("timeline_event")
    if not timeline_event: return current_state
    card_to_update, _ = find_card_and_customer(current_state, template, data)
    new_status = timeline_event.get("status")
    if not card_to_update and template.get("provider_type") == "bank":
        customer_id = data.get("customer_id")
        if customer_id not in current_state:
            current_state[customer_id] = {"_id": customer_id, "customer_info": {"name": data.get("customer_name"), "mobile": data.get("mobile")}, "cards": []}
        card_to_update = {
            "tracking_ids": {"application_id": data.get("application_id")}, "tracking_status": "active",
            "card_info": { "bank_name": template.get("provider_name"), "card_type": data.get("card_type"), "card_variant": data.get("card_variant") },
            "current_status": {}, "timeline": {"application_and_approval": [], "card_production": [], "shipping_and_delivery": []}
        }
        current_state[customer_id]["cards"].append(card_to_update)
    if not card_to_update: return current_state
    stage = timeline_event['stage']
    timeline_for_stage = card_to_update["timeline"].setdefault(stage, [])
    if timeline_for_stage and timeline_event.get("timestamp", "") <= timeline_for_stage[-1].get("timestamp", ""):
        print(f"-> Ignoring old/duplicate event '{timeline_event.get('status')}'")
        return current_state
    print(f"-> Applying new event: {timeline_event.get('status')}")
    timeline_for_stage.append(timeline_event)
    card_to_update["current_status"] = {"stage": new_status, "location": data.get("location"), "last_updated": timeline_event.get("timestamp")}
    if template.get("provider_type") == "card_manufacturer":
        card_to_update["tracking_ids"]["manufacturer_order_id"] = data.get("manufacturer_order_id")
        card_to_update["tracking_ids"]["logistics_tracking_number"] = data.get("logistics_tracking_number")
    final_statuses = ["DELIVERED", "APPLICATION_REJECTED", "APPLICATION_CANCELLED", "RETURNED_TO_SENDER"]
    if new_status in final_statuses:
        card_to_update["tracking_status"] = "completed"
    return current_state

def pretty_print_json(data):
    print(json.dumps(data, indent=2, default=str))

def main():
    parser = argparse.ArgumentParser(description="Process bulk data files using a master configuration.")
    parser.add_argument("bulk_input_file", nargs='?', default=None, help="Path to the bulk input JSON data file.")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'], help="The type of data in the input file.")
    parser.add_argument("--reset", action="store_true", help="Reset the local state file to empty.")
    args = parser.parse_args()

    if args.reset:
        if os.path.exists(LOCAL_STATE_FILE): os.remove(LOCAL_STATE_FILE)
        print(f"✅ Local state file '{LOCAL_STATE_FILE}' has been reset.")
        return

    if not args.bulk_input_file or not args.type:
        parser.print_help(); return

    master_config = load_json_file(MASTER_CONFIG_FILE)
    if master_config is None: return
    template = next(iter(master_config.get(args.type, {}).values()), None)
    if not template:
        print(f"❌ Error: No template definition found under the '{args.type}' section.")
        return

    current_state = load_json_file(LOCAL_STATE_FILE) or {}
    bulk_data = load_json_file(args.bulk_input_file)
    if bulk_data is None: return

    print(f"\nProcessing {len(bulk_data)} records from {args.bulk_input_file} as type '{args.type}'...")

    for record in bulk_data:
        # The inner loop that processes each event from the record's history
        for processed_data in process_data(record, template):
            current_state = update_local_state(current_state, processed_data, template)

    with open(LOCAL_STATE_FILE, "w") as f:
        json.dump(current_state, f, indent=2)
    
    print("\n✅ Bulk processing complete. Final state saved.")
    print(f"\n--- FINAL STATE IN '{LOCAL_STATE_FILE}' ---")
    pretty_print_json(current_state)

if __name__ == "__main__":
    main()