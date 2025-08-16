import json
import argparse
from datetime import datetime
from jsonpath_ng import parse
import os

LOCAL_STATE_FILE = "local_db_state.json"

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
    """Finds the card object and its parent customer ID."""
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
    if not timeline_event:
        print("-> No valid status event found in input. Skipping update.")
        return current_state

    card_to_update, customer_id = find_card_and_customer(current_state, template, data)
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
        print(f"-> Creating new card record for application: {data.get('application_id')}")

    if not card_to_update:
        print(f"❌ Error: Could not find a matching card record to update.")
        return current_state

    all_events = [event for stage_events in card_to_update["timeline"].values() for event in stage_events]
    if all_events:
        last_event_timestamp = max(event.get("timestamp", "") for event in all_events)
        if timeline_event.get("timestamp", "") <= last_event_timestamp:
            print(f"-> Ignoring old event '{new_status}'.")
            return current_state

    print(f"-> Applying new event: {new_status}")
    stage = timeline_event['stage']
    card_to_update["timeline"].setdefault(stage, []).append(timeline_event)
    card_to_update["current_status"] = {"stage": new_status, "location": data.get("current_location"), "last_updated": timeline_event.get("timestamp")}

    if template.get("provider_type") == "card_manufacturer":
        card_to_update["tracking_ids"]["manufacturer_order_id"] = data.get("manufacturer_order_id")
        card_to_update["tracking_ids"]["logistics_tracking_number"] = data.get("logistics_tracking_number")

    final_statuses = ["DELIVERED", "APPLICATION_REJECTED", "APPLICATION_CANCELLED", "RETURNED_TO_SENDER"]
    if new_status in final_statuses:
        card_to_update["tracking_status"] = "completed"
        
    return current_state

def format_state_for_display(state, customer_id_to_display):
    cust_doc = state.get(customer_id_to_display)
    if not cust_doc: return None
    formatted_customer = {"customer_id": cust_doc.get("_id"), "customer_info": cust_doc.get("customer_info"), "in_tracking": [], "done_tracking": []}
    final_statuses = ["DELIVERED", "APPLICATION_REJECTED", "APPLICATION_CANCELLED"]
    for card in cust_doc.get("cards", []):
        if card.get("current_status", {}).get("stage") in final_statuses: formatted_customer["done_tracking"].append(card)
        else: formatted_customer["in_tracking"].append(card)
    return formatted_customer

def pretty_print_json(data):
    print(json.dumps(data, indent=2, default=str))

def main():
    parser = argparse.ArgumentParser(description="Locally test the processing of data files.")
    parser.add_argument("input_file", nargs='?', default=None, help="Path to the input raw JSON data file.")
    parser.add_argument("template_file", nargs='?', default=None, help="Path to the JSON template file.")
    parser.add_argument("--reset", action="store_true", help="Reset the local state file to empty.")
    args = parser.parse_args()

    if args.reset:
        if os.path.exists(LOCAL_STATE_FILE): os.remove(LOCAL_STATE_FILE)
        print(f"✅ Local state file '{LOCAL_STATE_FILE}' has been reset.")
        return

    if not args.input_file or not args.template_file: parser.print_help(); return

    current_state = load_json_file(LOCAL_STATE_FILE) or {}
    print(f"\nProcessing file: {args.input_file}")
    raw_data = load_json_file(args.input_file); template = load_json_file(args.template_file)
    if raw_data is None or template is None: return

    processed_data = process_data(raw_data, template)
    _, customer_id_to_display = find_card_and_customer(current_state, template, processed_data)
    if not customer_id_to_display and template.get("provider_type") == "bank":
        customer_id_to_display = processed_data.get("customer_id")
        
    new_state = update_local_state(current_state, processed_data, template)

    with open(LOCAL_STATE_FILE, "w") as f: json.dump(new_state, f, indent=2)
    print("✅ Local state updated successfully.")
    
    if customer_id_to_display:
        print(f"\n--- FINAL DISPLAY OUTPUT FOR CUSTOMER: {customer_id_to_display} ---")
        display_output = format_state_for_display(new_state, customer_id_to_display)
        pretty_print_json(display_output)
    else:
        print("Could not determine which customer to display.")

if __name__ == "__main__":
    main()