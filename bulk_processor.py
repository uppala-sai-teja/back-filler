import json
import argparse
from datetime import datetime
from jsonpath_ng import parse
import os

LOCAL_STATE_FILE = "local_db_state.json"
MASTER_CONFIG_FILE = "master_config.json"

# --- All helper and logic functions remain the same ---
def load_json_file(file_path):
    try:
        with open(file_path, 'r') as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading {file_path}: {e}")
        return None

def process_data(raw_data, template):
    processed = {}
    for key, path in template.get("field_mappings", {}).items():
        matches = [match.value for match in parse(path).find(raw_data)]
        if matches: processed[key] = matches[0]
    raw_status = processed.get("status")
    if raw_status:
        status_map = template.get("status_mappings", {}).get(raw_status)
        if status_map:
            processed["status"] = status_map.get("status")
            ts = processed.get("approval_date") or processed.get("dispatch_date") or processed.get("received_date") or processed.get("production_end_date") or processed.get("last_updated") or processed.get("application_date") or datetime.now().isoformat()
            processed["timeline_event"] = {
                "status": status_map.get("status"), "stage": status_map.get("stage"),
                "timestamp": ts, "description": f"Status updated to {status_map.get('status')}"
            }
    return processed

def find_card_and_customer(state, template, data):
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
    timeline_event = data.get("timeline_event")
    if not timeline_event: return current_state
    card_to_update, _ = find_card_and_customer(current_state, template, data)
    new_status = data.get("status")
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
        return current_state
    timeline_for_stage.append(timeline_event)
    card_to_update["current_status"] = {"stage": new_status, "location": data.get("current_location"), "last_updated": timeline_event.get("timestamp")}
    if template.get("provider_type") == "card_manufacturer":
        card_to_update["tracking_ids"]["manufacturer_order_id"] = data.get("manufacturer_order_id")
        card_to_update["tracking_ids"]["logistics_tracking_number"] = data.get("logistics_tracking_number")
    final_statuses = ["DELIVERED", "APPLICATION_REJECTED", "APPLICATION_CANCELLED", "RETURNED_TO_SENDER"]
    if new_status in final_statuses:
        card_to_update["tracking_status"] = "completed"
    return current_state

def pretty_print_json(data):
    print(json.dumps(data, indent=2, default=str))

# --- **THE CORRECTED MAIN FUNCTION** ---
def main():
    parser = argparse.ArgumentParser(description="Process bulk data files using a master configuration.")
    # --- FIX: Make the main arguments optional so --reset can work alone ---
    parser.add_argument("bulk_input_file", nargs='?', default=None, help="Path to the bulk input JSON data file.")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'], help="The type of data in the input file.")
    parser.add_argument("--reset", action="store_true", help="Reset the local state file to empty.")
    args = parser.parse_args()

    # --- FIX: Handle the --reset action FIRST and then exit ---
    if args.reset:
        if os.path.exists(LOCAL_STATE_FILE):
            os.remove(LOCAL_STATE_FILE)
            print(f"✅ Local state file '{LOCAL_STATE_FILE}' has been reset.")
        else:
            print("⚪ No local state file to reset.")
        return # Exit the script immediately

    # --- FIX: If not resetting, THEN validate that the other arguments are present ---
    if not args.bulk_input_file or not args.type:
        print("❌ Error: For processing, you must provide both a bulk_input_file and the --type argument.")
        parser.print_help()
        return

    # --- The rest of the logic proceeds as normal ---
    master_config = load_json_file(MASTER_CONFIG_FILE)
    if master_config is None: return

    provider_templates = master_config.get(args.type)
    if not provider_templates:
        print(f"❌ Error: No templates found for type '{args.type}' in {MASTER_CONFIG_FILE}")
        return
    
    template = next(iter(provider_templates.values()), None)
    if not template:
        print(f"❌ Error: No template definition found under the '{args.type}' section.")
        return

    current_state = load_json_file(LOCAL_STATE_FILE) or {}
    bulk_data = load_json_file(args.bulk_input_file)
    if bulk_data is None: return

    print(f"\nProcessing {len(bulk_data)} records from {args.bulk_input_file} as type '{args.type}'...")

    for record in bulk_data:
        processed_data = process_data(record, template)
        current_state = update_local_state(current_state, processed_data, template)

    with open(LOCAL_STATE_FILE, "w") as f:
        json.dump(current_state, f, indent=2)
    
    print("\n✅ Bulk processing complete. Final state saved.")
    print(f"\n--- FINAL STATE IN '{LOCAL_STATE_FILE}' ---")
    pretty_print_json(current_state)

if __name__ == "__main__":
    main()