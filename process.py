import json
import argparse
from datetime import datetime
from pymongo import MongoClient
from jsonpath_ng import parse

# --- MongoDB Configuration ---
# IMPORTANT: Replace with your MongoDB connection string
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "card_tracker_db"
COLLECTION_NAME = "customers"

def connect_to_mongo():
    """Establishes a connection to the MongoDB server."""
    try:
        client = MongoClient(MONGO_URI)
        # The ismaster command is cheap and does not require auth.
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
    processed = {"raw_status": None, "timeline_event": {}}
    
    for key, path in template.get("field_mappings", {}).items():
        matches = [match.value for match in parse(path).find(raw_data)]
        if matches:
            processed[key] = matches[0]

    raw_status = processed.get("status")
    if raw_status:
        processed["raw_status"] = raw_status
        status_map = template.get("status_mappings", {}).get(raw_status)
        if status_map:
            processed["status"] = status_map.get("status")
            processed["timeline_event"] = {
                "status": status_map.get("status"),
                "stage": status_map.get("stage"),
                "timestamp": processed.get("approval_date") or processed.get("dispatch_date") or processed.get("last_updated", datetime.now().isoformat()),
                "description": f"Status updated to {status_map.get('status')}"
            }
    return processed

def update_customer_record(collection, data, template):
    """Inserts or updates a customer document in MongoDB."""
    provider_type = template.get("provider_type")
    
    if provider_type == "bank":
        # This is the entry point for a new card journey
        customer_id = data.get("customer_id")
        application_id = data.get("application_id")
        
        query = {"_id": customer_id}
        
        # Define the new card object to be added to the array
        new_card = {
            "tracking_ids": { "application_id": application_id },
            "card_info": {
                "bank_name": template.get("provider_name"),
                "card_type": data.get("card_type"),
                "card_variant": data.get("card_variant")
            },
            "current_status": { "stage": data.get("status") },
            "timeline": {
                "application_and_approval": [data.get("timeline_event")],
                "card_production": [],
                "shipping_and_delivery": []
            }
        }

        # Use $set for customer info and $push to add the new card
        update = {
            "$set": {
                "customer_info": { "name": data.get("customer_name"), "mobile": data.get("mobile"), "email": data.get("email") }
            },
            "$push": { "cards": new_card }
        }
        
        print(f"-> Upserting customer '{customer_id}' and adding card '{application_id}'...")
        collection.update_one(query, update, upsert=True)

    elif provider_type == "card_manufacturer":
        application_id = data.get("application_id")
        
        # Find the customer document that contains the card with this application_id
        query = {"cards.tracking_ids.application_id": application_id}
        
        # Use the positional operator '$' to update the specific card in the array
        update = {
            "$set": {
                "cards.$.tracking_ids.manufacturer_order_id": data.get("manufacturer_order_id"),
                "cards.$.tracking_ids.logistics_tracking_number": data.get("tracking_number"),
                "cards.$.card_info.last_four_digits": data.get("last_four_digits"),
                "cards.$.delivery_info.courier_partner": data.get("courier_partner"),
                "cards.$.current_status": {
                    "stage": data.get("status"),
                    "last_updated": data.get("timeline_event", {}).get("timestamp")
                }
            },
            "$push": { "cards.$.timeline.shipping_and_delivery": data.get("timeline_event") }
        }

        print(f"-> Updating manufacturer details for card '{application_id}'...")
        collection.update_one(query, update)

    elif provider_type == "logistics":
        tracking_number = data.get("tracking_number")
        
        # Find the customer document that contains the card with this tracking_number
        query = {"cards.tracking_ids.logistics_tracking_number": tracking_number}

        update = {
            "$set": {
                "cards.$.current_status": {
                    "stage": data.get("status"),
                    "location": data.get("current_location"),
                    "last_updated": data.get("timeline_event", {}).get("timestamp")
                },
                "cards.$.delivery_info.estimated_delivery": data.get("estimated_delivery")
            },
            "$push": { "cards.$.timeline.shipping_and_delivery": data.get("timeline_event") }
        }
        
        print(f"-> Updating logistics details for tracking# '{tracking_number}'...")
        collection.update_one(query, update)
        
    print("✅ Database update complete.")

def main():
    # I have commented out your testing code and provided the corrected version.
    
    # --- YOUR ORIGINAL TESTING CODE (with the bug) ---
    # input_file = "sample_data/bank_input_1_submitted.json"
    # template_file = "sample_data/bank_input_1_submitted.json" # Incorrect: Using data as the template

    # --- CORRECTED TESTING CODE ---
    input_file = "sample_data/bank_input_1_submitted.json"
    template_file = "templates/bank_template.json" # Correct: Using the actual template file

    # The rest of your testing setup is perfect.
    
    print(f"\nProcessing file: {input_file}")
    raw_data = load_json_file(input_file)
    template = load_json_file(template_file)
    if not raw_data or not template:
        return

    # This will now work correctly
    processed_data = process_data(raw_data, template)
    
    print("\n✅ Processing successful. Writing output to processed_data.json")
    with open("processed_data.json", "w") as f:
        json.dump(processed_data, f, indent=4)

if __name__ == "__main__":
    main()