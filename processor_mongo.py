import json
import argparse
from datetime import datetime
from pymongo import MongoClient
from jsonpath_ng import parse

# --- MongoDB Configuration ---
# IMPORTANT: Replace with your MongoDB connection string if not running locally
MONGO_URI = ""
DB_NAME = "cardsDB"
COLLECTION_NAME = "cards"

def connect_to_mongo():
    """Establishes a connection to the MongoDB server."""
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ismaster')
        print("✅ MongoDB connection successful.")
        return client[DB_NAME]
    except Exception as e:
        print(f"❌ Could not connect to MongoDB: {e}")
        return None

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

def update_and_get_customer_id(collection, data, template):
    """
    Inserts or updates a record and returns the customer_id of the affected document.
    """
    provider_type = template.get("provider_type")
    lookup_key_field = template.get("lookup_key")
    lookup_value = data.get(lookup_key_field)
    
    if not lookup_value:
        print(f"❌ Error: Lookup key '{lookup_key_field}' not found in processed data.")
        return None

    customer_id = None

    if provider_type == "bank":
        customer_id = data.get("customer_id")
        application_id = data.get("application_id")
        card_exists = collection.find_one({"_id": customer_id, "cards.tracking_ids.application_id": application_id})
        if card_exists:
            print(f"-> Updating existing card '{application_id}' for customer '{customer_id}'...")
            query = {"_id": customer_id, "cards.tracking_ids.application_id": application_id}
            update = {
                "$set": {"cards.$.current_status.stage": data.get("status")},
                "$push": {"cards.$.timeline.application_and_approval": data.get("timeline_event")}
            }
            collection.update_one(query, update)
        else:
            print(f"-> Adding new card '{application_id}' for customer '{customer_id}'...")
            query = {"_id": customer_id}
            new_card = {
                "tracking_ids": {"application_id": application_id},
                "card_info": {"bank_name": template.get("provider_name"), "card_type": data.get("card_type"), "card_variant": data.get("card_variant")},
                "current_status": {"stage": data.get("status"), "last_updated": data.get("timeline_event",{}).get("timestamp")},
                "timeline": {"application_and_approval": [data.get("timeline_event")], "card_production": [], "shipping_and_delivery": []}
            }
            update = {
                "$set": {"customer_info": {"name": data.get("customer_name"), "mobile": data.get("mobile")}},
                "$push": {"cards": new_card}
            }
            collection.update_one(query, update, upsert=True)
        return customer_id

    elif provider_type in ["card_manufacturer", "logistics"]:
        # For these providers, we must first find the document to get the customer_id
        if provider_type == "card_manufacturer":
            query = {"cards.tracking_ids.application_id": lookup_value}
        else: # logistics
            query = {"cards.tracking_ids.logistics_tracking_number": lookup_value}
        
        doc = collection.find_one(query)
        if not doc:
            print(f"❌ Error: Could not find a matching document to update for {lookup_key_field} '{lookup_value}'.")
            return None
        
        customer_id = doc["_id"] # We found the customer ID!

        # Now, construct and apply the update
        if provider_type == "card_manufacturer":
            print(f"-> Updating manufacturer details for card '{lookup_value}'...")
            update = {
                "$set": {
                    "cards.$.tracking_ids.manufacturer_order_id": data.get("manufacturer_order_id"),
                    "cards.$.tracking_ids.logistics_tracking_number": data.get("tracking_number"),
                    "cards.$.card_info.last_four_digits": data.get("last_four_digits"),
                    "cards.$.delivery_info.courier_partner": data.get("courier_partner"),
                    "cards.$.current_status": {"stage": data.get("status"), "last_updated": data.get("timeline_event",{}).get("timestamp")}
                },
                "$push": {f"cards.$.timeline.{data['timeline_event']['stage']}": data.get("timeline_event")}
            }
        else: # logistics
            print(f"-> Updating logistics details for tracking# '{lookup_value}'...")
            update = {
                "$set": { "cards.$.current_status": {"stage": data.get("status"), "location": data.get("current_location"), "last_updated": data.get("timeline_event",{}).get("timestamp")} },
                "$push": {"cards.$.timeline.shipping_and_delivery": data.get("timeline_event")}
            }
        
        collection.update_one(query, update)
        return customer_id

    return None

def pretty_print_json(data):
    """Prints JSON data in a readable format."""
    print(json.dumps(data, indent=2, default=str))

def main():
    parser = argparse.ArgumentParser(description="Process data files, update MongoDB, and show the result.")
    parser.add_argument("input_file", help="Path to the input raw JSON data file.")
    parser.add_argument("template_file", help="Path to the JSON template file.")
    args = parser.parse_args()

    db = connect_to_mongo()
    if db is None: return

    collection = db[COLLECTION_NAME]
    
    print(f"\nProcessing file: {args.input_file}")
    raw_data = load_json_file(args.input_file)
    template = load_json_file(args.template_file)
    if raw_data is None or template is None: return

    processed_data = process_data(raw_data, template)
    
    # This function now returns the customer_id of the affected document
    customer_id = update_and_get_customer_id(collection, processed_data, template)

    if customer_id:
        print("✅ Database update complete.")
        print("\n--- FETCHING FINAL DOCUMENT ---")
        final_document = collection.find_one({"_id": customer_id})
        if final_document:
            pretty_print_json(final_document)
        else:
            print(f"❌ Error: Could not retrieve final document for customer '{customer_id}'.")
    else:
        print("❌ Database update failed or record not found.")

if __name__ == "__main__":
    main()