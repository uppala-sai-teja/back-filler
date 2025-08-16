import json
import argparse
from datetime import datetime
from jsonpath_ng import parse
import os

# The local file that will act as our mock database
LOCAL_STATE_FILE = "local_db_state.json"

# --- Helper and Processing Functions (Mostly Unchanged) ---

def load_json_file(file_path):
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
                "status": status_map.get("status"), "stage": status_map.get("stage"),
                "timestamp": ts, "description": f"Status updated to {status_map.get('status')}"
            }
    return processed

# --- Core Logic for Updating State (Now Stores More Card Info) ---

def update_local_state(current_state, data, template):
    provider_type = template.get("provider_type")
    
    if provider_type == "bank":
        customer_id = data.get("customer_id")
        application_id = data.get("application_id")
        
        if customer_id not in current_state:
            current_state[customer_id] = {
                "_id": customer_id,
                "customer_info": {"name": data.get("customer_name"), "mobile": data.get("mobile")},
                "cards": []
            }
        
        customer_doc = current_state[customer_id]
        card_index = next((i for i, card in enumerate(customer_doc.get("cards", [])) if card["tracking_ids"]["application_id"] == application_id), -1)

        if card_index != -1:
            customer_doc["cards"][card_index]["current_status"]["stage"] = data.get("status")
            customer_doc["cards"][card_index]["timeline"]["application_and_approval"].append(data.get("timeline_event"))
        else:
            new_card = {
                "tracking_ids": {"application_id": application_id},
                "card_info": { # <-- **IMPROVEMENT 2: STORING MORE CARD DETAILS**
                    "bank_name": template.get("provider_name"),
                    "card_type": data.get("card_type"),
                    "card_variant": data.get("card_variant")
                },
                "current_status": {"stage": data.get("status"), "last_updated": data.get("timeline_event",{}).get("timestamp")},
                "timeline": {"application_and_approval": [data.get("timeline_event")], "card_production": [], "shipping_and_delivery": []}
            }
            customer_doc["cards"].append(new_card)

    elif provider_type == "card_manufacturer":
        application_id = data.get("application_id")
        for cust_doc in current_state.values():
            for card in cust_doc.get("cards", []):
                if card["tracking_ids"]["application_id"] == application_id:
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
        for cust_doc in current_state.values():
            for card in cust_doc.get("cards", []):
                if card.get("tracking_ids", {}).get("logistics_tracking_number") == tracking_number:
                    card["current_status"] = {"stage": data.get("status"), "location": data.get("current_location"), "last_updated": data.get("timeline_event",{}).get("timestamp")}
                    card["timeline"]["shipping_and_delivery"].append(data.get("timeline_event"))
                    break

    return current_state

# --- **IMPROVEMENT 1: CORRECTED FUNCTION TO FORMAT THE FINAL OUTPUT** ---

def format_state_for_display(state, customer_id_to_display):
    """
    Takes the raw state and a specific customer ID, then formats the output
    for that single customer with segregated card lists.
    """
    # Find the specific customer document from the state
    cust_doc = state.get(customer_id_to_display)
    
    if not cust_doc:
        print(f"Error: Could not find customer '{customer_id_to_display}' in the local state.")
        return None

    # This will be the final, single-customer JSON object
    formatted_customer = {
        "customer_id": cust_doc.get("_id"),
        "customer_info": cust_doc.get("customer_info"),
        "in_tracking": [],
        "done_tracking": []
    }
    
    final_statuses = ["DELIVERED", "APPLICATION_REJECTED", "APPLICATION_CANCELLED"]
    
    for card in cust_doc.get("cards", []):
        if card.get("current_status", {}).get("stage") in final_statuses:
            formatted_customer["done_tracking"].append(card)
        else:
            formatted_customer["in_tracking"].append(card)
    
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
        if os.path.exists(LOCAL_STATE_FILE):
            os.remove(LOCAL_STATE_FILE)
            print(f"✅ Local state file '{LOCAL_STATE_FILE}' has been reset.")
        else:
            print("⚪ No local state file to reset.")
        return

    if not args.input_file or not args.template_file:
        parser.print_help()
        return

    current_state = load_json_file(LOCAL_STATE_FILE) or {}
    print(f"\nProcessing file: {args.input_file}")
    raw_data = load_json_file(args.input_file)
    template = load_json_file(args.template_file)
    if raw_data is None or template is None: return

    processed_data = process_data(raw_data, template)
    
    # We need to know which customer was affected to display them correctly
    customer_id_to_display = processed_data.get("customer_id")
    if not customer_id_to_display:
        # If the input isn't from the bank, we need to find the customer ID
        lookup_key = template.get("lookup_key")
        lookup_value = processed_data.get(lookup_key)
        for cid, doc in current_state.items():
            for card in doc.get("cards", []):
                if card.get("tracking_ids", {}).get(lookup_key) == lookup_value:
                    customer_id_to_display = cid
                    break
            if customer_id_to_display:
                break
    
    new_state = update_local_state(current_state, processed_data, template)

    with open(LOCAL_STATE_FILE, "w") as f:
        json.dump(new_state, f, indent=2)
    
    print("✅ Local state updated successfully.")
    
    if customer_id_to_display:
        print(f"\n--- FINAL DISPLAY OUTPUT FOR CUSTOMER: {customer_id_to_display} ---")
        display_output = format_state_for_display(new_state, customer_id_to_display)
        pretty_print_json(display_output)
    else:
        print("Could not determine which customer to display.")

if __name__ == "__main__":
    main()