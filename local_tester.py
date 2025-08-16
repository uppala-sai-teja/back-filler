import json
import argparse
from datetime import datetime
from jsonpath_ng import parse
import os

# The local file that will act as our mock database
LOCAL_STATE_FILE = "local_db_state.json"

def load_json_file(file_path):
    """Safely loads a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {file_path}")
        return None

def process_data(raw_data, template):
    """Processes raw data using a template to extract and normalize fields."""
    processed = {}
    for key, path in template.get("field_mappings", {}).items():
        matches = [match.value for match in parse(path).find(raw_data)]
        if matches:
            processed[key] = matches[0]

    raw_status = processed.get("status")
    if raw_status:
        status_map = template.get("status_mappings", {}).get(raw_status)
        if status_map:
            processed["status"] = status_map.get("status")
            ts = processed.get("approval_date") or processed.get("dispatch_date") or processed.get("received_date") or processed.get("production_end_date") or processed.get("last_updated") or processed.get("application_date") or datetime.now().isoformat()
            processed["timeline_event"] = {
                "status": status_map.get("status"),
                "stage": status_map.get("stage"),
                "timestamp": ts,
                "description": f"Status updated to {status_map.get('status')}"
            }
    return processed

def update_local_state(current_state, data, template):
    """Updates the local state dictionary, mimicking MongoDB logic."""
    provider_type = template.get("provider_type")
    
    if provider_type == "bank":
        customer_id = data.get("customer_id")
        application_id = data.get("application_id")
        
        # Ensure customer document exists (UPSERT)
        if customer_id not in current_state:
            current_state[customer_id] = {
                "_id": customer_id,
                "customer_info": {"name": data.get("customer_name"), "mobile": data.get("mobile")},
                "cards": []
            }
        
        customer_doc = current_state[customer_id]
        
        # Check if card already exists
        card_index = -1
        for i, card in enumerate(customer_doc.get("cards", [])):
            if card["tracking_ids"]["application_id"] == application_id:
                card_index = i
                break

        if card_index != -1: # Card exists, so update it
            customer_doc["cards"][card_index]["current_status"]["stage"] = data.get("status")
            customer_doc["cards"][card_index]["timeline"]["application_and_approval"].append(data.get("timeline_event"))
        else: # Card is new, so add it
            new_card = {
                "tracking_ids": {"application_id": application_id},
                "card_info": {"bank_name": template.get("provider_name")},
                "current_status": {"stage": data.get("status"), "last_updated": data.get("timeline_event",{}).get("timestamp")},
                "timeline": {"application_and_approval": [data.get("timeline_event")], "card_production": [], "shipping_and_delivery": []}
            }
            customer_doc["cards"].append(new_card)

    elif provider_type == "card_manufacturer":
        application_id = data.get("application_id")
        for cust_id, cust_doc in current_state.items():
            for i, card in enumerate(cust_doc.get("cards", [])):
                if card["tracking_ids"]["application_id"] == application_id:
                    # Found the card to update
                    card["tracking_ids"]["manufacturer_order_id"] = data.get("manufacturer_order_id")
                    card["tracking_ids"]["logistics_tracking_number"] = data.get("tracking_number")
                    card["card_info"]["last_four_digits"] = data.get("last_four_digits")
                    card.setdefault("delivery_info", {})["courier_partner"] = data.get("courier_partner")
                    card["current_status"] = {"stage": data.get("status"), "last_updated": data.get("timeline_event",{}).get("timestamp")}
                    stage = data['timeline_event']['stage']
                    card["timeline"].setdefault(stage, []).append(data.get("timeline_event"))
                    break

    elif provider_type == "logistics":
        tracking_number = data.get("tracking_number")
        for cust_id, cust_doc in current_state.items():
            for i, card in enumerate(cust_doc.get("cards", [])):
                if card.get("tracking_ids", {}).get("logistics_tracking_number") == tracking_number:
                    # Found the card to update
                    card["current_status"] = {"stage": data.get("status"), "location": data.get("current_location"), "last_updated": data.get("timeline_event",{}).get("timestamp")}
                    card["timeline"]["shipping_and_delivery"].append(data.get("timeline_event"))
                    break

    return current_state

def pretty_print_json(data):
    """Prints JSON data in a readable format."""
    print(json.dumps(data, indent=2, default=str))

def main():
    parser = argparse.ArgumentParser(description="Locally test the processing of data files.")
    parser.add_argument("input_file", nargs='?', default=None, help="Path to the input raw JSON data file.")
    parser.add_argument("template_file", nargs='?', default=None, help="Path to the JSON template file.")
    parser.add_argument("--reset", action="store_true", help="Reset the local state file to empty.")
    args = parser.parse_args()

    if args.reset:
        if os.path.exists(LOCAL_STATE_FILE):
            os.remove(LOCAL_STATE_FILE)
            print(f"✅ Local state file '{LOCAL_STATE_FILE}' has been reset.")
        else:
            print("⚪ No local state file to reset.")
        return

    if not args.input_file or not args.template_file:
        print("❌ Error: You must provide both an input_file and a template_file.")
        parser.print_help()
        return

    # Load current state from file, or start fresh if it doesn't exist
    current_state = load_json_file(LOCAL_STATE_FILE) or {}

    print(f"\nProcessing file: {args.input_file}")
    raw_data = load_json_file(args.input_file)
    template = load_json_file(args.template_file)
    if raw_data is None or template is None: return

    processed_data = process_data(raw_data, template)
    
    # Get the new state after updating
    new_state = update_local_state(current_state, processed_data, template)

    # Save the new state back to the file
    with open(LOCAL_STATE_FILE, "w") as f:
        json.dump(new_state, f, indent=2)
    
    print("✅ Local state updated successfully.")
    print(f"\n--- CURRENT STATE IN '{LOCAL_STATE_FILE}' ---")
    pretty_print_json(new_state)

if __name__ == "__main__":
    main()