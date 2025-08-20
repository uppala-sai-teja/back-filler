# main.py - Enhanced with Debug Logistics
#!/usr/bin/env python3
"""
Enhanced Card Tracking System with API Fetching and Track Sheet Generation
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from core.card_processor import CardTrackingProcessor
from jsonpath_ng import parse

# Global stage order definition
STAGE_ORDER = ["application_and_approval", "card_production", "shipping_and_delivery"]

class EnhancedCardTrackingSystem:
    def __init__(self, debug=False):
        self.processor = CardTrackingProcessor(debug)
        self.debug = debug
        self.last_poll_times = self.load_poll_times()
        
    def load_poll_times(self) -> Dict:
        """Load last poll times for incremental fetching"""
        try:
            with open('config/last_poll_times.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {
                "bank": None,
                "manufacturer": None,
                "logistics": None
            }
    
    def save_poll_times(self):
        """Save current poll times"""
        os.makedirs('config', exist_ok=True)
        with open('config/last_poll_times.json', 'w') as f:
            json.dump(self.last_poll_times, f, indent=2)
    
    def calculate_pending_stages(self, current_stage: str) -> List[str]:
        """Calculate pending stages based on current stage"""
        try:
            current_index = STAGE_ORDER.index(current_stage)
            return STAGE_ORDER[current_index + 1:]
        except (ValueError, IndexError):
            return STAGE_ORDER.copy()
    
    def update_card_pending_stages(self, card: Dict) -> Dict:
        """Update pending stages for a card"""
        current_stage = card.get("current_status", {}).get("stage", "")
        card["pending_stages"] = self.calculate_pending_stages(current_stage)
        return card
    
    def update_all_pending_stages(self):
        """Update pending stages for all cards in the database"""
        customers = list(self.processor.db_manager.customers_collection.find())
        
        for customer in customers:
            updated = False
            for i, card in enumerate(customer.get("cards", [])):
                old_pending = card.get("pending_stages", [])
                customer["cards"][i] = self.update_card_pending_stages(card)
                new_pending = customer["cards"][i].get("pending_stages", [])
                
                if old_pending != new_pending:
                    updated = True
            
            if updated:
                self.processor.db_manager.upsert_customer(customer)
    
    def auto_generate_track_sheet(self, source: str = "auto") -> bool:
        """Automatically generate and save track sheet after data changes"""
        try:
            track_sheet = {}
            customers = list(self.processor.db_manager.customers_collection.find())
            
            for customer in customers:
                customer_name = customer.get("customer_info", {}).get("name", "Unknown")
                
                for card in customer.get("cards", []):
                    application_id = card.get("tracking_ids", {}).get("application_id")
                    if not application_id:
                        continue
                    
                    current_status_info = card.get("current_status", {})
                    
                    track_sheet[application_id] = {
                        "customer": customer_name,
                        "current_stage": current_status_info.get("stage", "unknown"),
                        "current_status": current_status_info.get("status", "UNKNOWN"),
                        "pending_stages": card.get("pending_stages", STAGE_ORDER.copy()),
                        "last_updated": current_status_info.get("last_updated", card.get("metadata", {}).get("last_updated", "")),
                        "card_type": card.get("card_info", {}).get("card_type", "Unknown"),
                        "card_variant": card.get("card_info", {}).get("card_variant", "Unknown"),
                        "estimated_delivery": card.get("estimated_delivery"),
                        "tracking_ids": card.get("tracking_ids", {}),
                        "application_metadata": card.get("application_metadata", {})
                    }
            
            # Save to JSON file (always update the current track sheet)
            with open('track_sheet.json', 'w') as f:
                json.dump(track_sheet, f, indent=2, default=str)
            
            # Save to database with source information
            self.processor.db_manager.save_track_sheet(track_sheet, f"auto_{source}")
            
            print(f"📋 Auto-updated track sheet with {len(track_sheet)} applications (source: {source})")
            
            return True
            
        except Exception as e:
            print(f"❌ Error auto-generating track sheet: {e}")
            return False
    
    def debug_database_state(self):
        """Debug helper to show current database state"""
        print("\n🔍 === DEBUG: Current Database State ===")
        customers = list(self.processor.db_manager.customers_collection.find())
        
        for customer in customers:
            print(f"\nCustomer: {customer['_id']} - {customer['customer_info']['name']}")
            for card in customer.get('cards', []):
                print(f"  Card: {card['card_id']}")
                print(f"  Application ID: {card['tracking_ids'].get('application_id')}")
                print(f"  Manufacturer Order ID: {card['tracking_ids'].get('manufacturer_order_id')}")
                print(f"  Logistics Tracking: {card['tracking_ids'].get('logistics_tracking_number')}")
                print(f"  Current Status: {card.get('current_status', {}).get('status', 'None')}")
    
    def debug_logistics_requirements(self, input_file: str, template: Dict):
        """Debug logistics data requirements"""
        print("\n🔍 === DEBUG: Logistics Processing Requirements ===")
        
        with open(input_file, 'r') as f:
            input_data = json.load(f)
        
        lookup_key = template.get("lookup_key")
        print(f"Looking for tracking field: {lookup_key}")
        
        print(f"\n📋 Logistics data wants to find:")
        for record in input_data:
            lookup_value = None
            for key, path in template.get("field_mappings", {}).items():
                if key == lookup_key:
                    try:
                        matches = [match.value for match in parse(path).find(record)]
                        if matches:
                            lookup_value = matches[0]
                            break
                    except:
                        pass
            
            print(f"  - {lookup_key}: {lookup_value}")
            
            # Check if this tracking number exists in database
            card, customer_id = self.processor.db_manager.find_card_by_tracking_id(lookup_key, lookup_value)
            if card:
                print(f"    ✅ Found card: {card['card_id']} (Customer: {customer_id})")
            else:
                print(f"    ❌ No card found with {lookup_key}: {lookup_value}")
        
        print(f"\n📦 Available tracking numbers in database:")
        customers = list(self.processor.db_manager.customers_collection.find())
        found_any = False
        for customer in customers:
            for card in customer.get('cards', []):
                tracking_ids = card.get('tracking_ids', {})
                if tracking_ids.get('logistics_tracking_number'):
                    print(f"  ✅ {tracking_ids.get('logistics_tracking_number')} (Card: {card['card_id']})")
                    found_any = True
        
        if not found_any:
            print("  ❌ No logistics tracking numbers found in database")
            print("  💡 Hint: Process manufacturer data first to create tracking numbers")
    
    def process_file(self, input_file: str, provider_type: str) -> bool:
        """Process input file and auto-update track sheet with enhanced debugging"""
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return False

        template = self.processor.get_template(provider_type)
        if not template:
            print(f"❌ Could not load template for {provider_type}")
            return False

        # Enhanced debugging for logistics
        if provider_type == "logistics" and self.debug:
            self.debug_database_state()
            self.debug_logistics_requirements(input_file, template)

        with open(input_file, 'r') as f:
            input_data = json.load(f)

        print(f"🚀 Processing {len(input_data)} records from {input_file}")
        
        success = self.processor.process_bulk_data(input_data, template)
        
        if success:
            # Update all cards with pending stages
            self.update_all_pending_stages()
            
            # Auto-generate track sheet after processing
            self.auto_generate_track_sheet(f"file_{provider_type}")
            
            self.processor.print_stats()
            self.processor.print_analytics()
            print(f"✅ Processing completed successfully!")
            return True
        else:
            print(f"❌ Processing failed!")
            return False
    
    # ADD ALL THE EXISTING SIMULATION METHODS HERE (unchanged)
    def simulate_bank_api_call(self, since: Optional[str] = None) -> List[Dict]:
        """Simulate GET /applications?status=submitted&since=last_poll_time"""
        print(f"Simulating bank API call (since: {since})...")
        
        # Simulate new applications - in real implementation, this would be an HTTP call
        simulated_new_applications = [
            {
                "customer_id": f"CUST_{datetime.now().strftime('%Y%m%d_%H%M%S')}_001",
                "customer_name": "Alice Johnson",
                "mobile": "9876543333",
                "email": "alice@example.com",
                "application_id": f"APP_{datetime.now().strftime('%Y%m%d_%H%M%S')}_001",
                "application_date": datetime.now().isoformat() + "Z",
                "card_type": "credit_card",
                "card_variant": "standard",
                "status": "submitted"
            },
            {
                "customer_id": f"CUST_{datetime.now().strftime('%Y%m%d_%H%M%S')}_002",
                "customer_name": "Bob Wilson",
                "mobile": "9876543334",
                "email": "bob@example.com",
                "application_id": f"APP_{datetime.now().strftime('%Y%m%d_%H%M%S')}_002",
                "application_date": datetime.now().isoformat() + "Z",
                "card_type": "debit_card",
                "card_variant": "gold",
                "status": "submitted"
            }
        ]
        
        print(f"Found {len(simulated_new_applications)} new applications")
        return simulated_new_applications
    
    def simulate_bank_api_call_existing(self, application_ids: List[str]) -> List[Dict]:
        """Simulate GET /applications/bulk for existing applications"""
        print(f"Simulating bank API call for {len(application_ids)} existing applications...")
        
        # Simulate updated statuses for existing applications
        simulated_updates = []
        for app_id in application_ids:
            # Randomly simulate some status updates
            import random
            statuses = ["submitted", "under_review", "approved", "rejected"]
            current_status = random.choice(statuses)
            
            simulated_updates.append({
                "application_id": app_id,
                "status": current_status,
                "last_updated": datetime.now().isoformat() + "Z",
                "approval_date": datetime.now().isoformat() + "Z" if current_status == "approved" else None
            })
        
        print(f"Retrieved status updates for {len(simulated_updates)} existing applications")
        return simulated_updates
    
    def simulate_manufacturer_api_call(self, application_ids: List[str]) -> List[Dict]:
        """Simulate POST /production-status with bulk application IDs"""
        print(f"Simulating manufacturer API call for {len(application_ids)} applications...")
        
        # Simulate manufacturer responses - in real implementation, this would be an HTTP call
        simulated_responses = []
        for app_id in application_ids:
            simulated_responses.append({
                "bank_reference": app_id,
                "order_id": f"MFG_{app_id}_{int(datetime.now().timestamp())}",
                "batch_number": f"BATCH_{datetime.now().strftime('%Y_%m_%d')}_001",
                "facility": "Chennai Production Unit",
                "production_history": [
                    {
                        "status": "received",
                        "timestamp": datetime.now().isoformat() + "Z",
                        "location": "Chennai Production Unit"
                    },
                    {
                        "status": "in_production",
                        "timestamp": (datetime.now() + timedelta(hours=2)).isoformat() + "Z",
                        "location": "Chennai Production Unit"
                    }
                ]
            })
        
        print(f"Retrieved production status for {len(simulated_responses)} applications")
        return simulated_responses
    
    def simulate_logistics_api_call(self, tracking_numbers: List[str]) -> List[Dict]:
        """Simulate POST /tracking with bulk AWB numbers"""
        print(f"Simulating logistics API call for {len(tracking_numbers)} packages...")
        
        # Simulate logistics responses - in real implementation, this would be an HTTP call
        simulated_responses = []
        for awb in tracking_numbers:
            simulated_responses.append({
                "awb_number": awb,
                "tracking_history": [
                    {
                        "status": "in_transit",
                        "timestamp": datetime.now().isoformat() + "Z",
                        "location": "Delhi Hub",
                        "description": "Package in transit"
                    },
                    {
                        "status": "out_for_delivery",
                        "timestamp": (datetime.now() + timedelta(hours=1)).isoformat() + "Z",
                        "location": "Local Delivery Hub",
                        "description": "Package out for delivery"
                    }
                ]
            })
        
        print(f"Retrieved tracking updates for {len(simulated_responses)} packages")
        return simulated_responses
    
    # ... (add all other existing methods: fetch_bank_applications, fetch_manufacturer_data, 
    #      fetch_logistics_data, generate_track_sheet - they remain unchanged)
    
    def fetch_bank_applications(self, include_existing: bool = True) -> bool:
        """Fetch new bank applications and optionally update existing ones"""
        print("Fetching bank applications...")
        
        try:
            success_count = 0
            
            # 1. Fetch new applications
            since = self.last_poll_times.get("bank")
            new_applications = self.simulate_bank_api_call(since)
            
            if new_applications:
                template = self.processor.get_template("bank")
                if not template:
                    print("Could not load bank template")
                    return False
                
                success = self.processor.process_bulk_data(new_applications, template)
                if success:
                    success_count += len(new_applications)
                    print(f"Processed {len(new_applications)} new applications")
            
            # 2. Update existing applications
            if include_existing:
                existing_app_ids = self.processor.db_manager.get_all_application_ids()
                if existing_app_ids:
                    existing_updates = self.simulate_bank_api_call_existing(existing_app_ids)
                    
                    template = self.processor.get_template("bank")
                    updated_count = 0
                    
                    for update in existing_updates:
                        card, customer_id = self.processor.db_manager.find_card_by_tracking_id(
                            "application_id", update["application_id"]
                        )
                        
                        if card and customer_id:
                            customer = self.processor.db_manager.get_customer(customer_id)
                            
                            timeline_event = self.processor.create_timeline_event(update, template)
                            if timeline_event:
                                processed_data = update.copy()
                                processed_data["provider_type"] = "bank"
                                processed_data["timeline_event"] = timeline_event
                                
                                if self.processor.update_card_with_event(customer, card, processed_data, timeline_event):
                                    updated_count += 1
                    
                    success_count += updated_count
                    print(f"Updated {updated_count} existing applications")
            
            if success_count > 0:
                # Update all cards with pending stages
                self.update_all_pending_stages()
                
                # Auto-generate track sheet after fetching
                self.auto_generate_track_sheet("fetch_bank")
                
                # Update last poll time
                self.last_poll_times["bank"] = datetime.now().isoformat() + "Z"
                self.save_poll_times()
                
                print(f"Successfully processed {success_count} total applications")
                self.processor.print_stats()
                return True
            else:
                print("No new data to process")
                return True
                
        except Exception as e:
            print(f"Error fetching bank applications: {e}")
            return False

    # ... (include all other existing methods unchanged)

def main():
    parser = argparse.ArgumentParser(description="Enhanced Card Tracking System with API Fetching")
    
    # Existing file processing arguments
    parser.add_argument("input_file", nargs='?', help="Input JSON data file")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'], 
                       help="Type of data to process")
    
    # Enhanced API fetching arguments
    parser.add_argument("--fetch-bank", action="store_true", 
                       help="Fetch new bank applications and update existing ones")
    parser.add_argument("--fetch-bank-new-only", action="store_true", 
                       help="Fetch only new bank applications (not existing)")
    parser.add_argument("--fetch-manufacturer", action="store_true", 
                       help="Fetch manufacturer data for pending cards")
    parser.add_argument("--fetch-logistics", action="store_true", 
                       help="Fetch logistics data for undelivered packages")
    parser.add_argument("--track-sheet", action="store_true", 
                       help="Generate track_sheet.json summary and save to database")
    parser.add_argument("--track-sheet-file-only", action="store_true", 
                       help="Generate track_sheet.json file only (don't save to database)")
    
    # Debug arguments
    parser.add_argument("--debug-db", action="store_true", 
                       help="Show current database state")
    
    # Utility arguments
    parser.add_argument("--analytics", action="store_true", help="Show analytics dashboard")
    parser.add_argument("--test-connection", action="store_true", help="Test MongoDB connection")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()

    try:
        # Test connection mode
        if args.test_connection:
            from core.mongodb_manager import MongoDBManager
            db_manager = MongoDBManager(debug=True)
            if db_manager.connect():
                print("MongoDB connection successful!")
                db_manager.disconnect()
            else:
                print("MongoDB connection failed!")
            return

        # Initialize enhanced system
        system = EnhancedCardTrackingSystem(debug=args.debug)

        # Debug database state
        if args.debug_db:
            system.debug_database_state()
            return

        # Handle enhanced fetch operations
        if args.fetch_bank:
            success = system.fetch_bank_applications(include_existing=True)
            system.processor.print_analytics()
            sys.exit(0 if success else 1)
        
        # ... (rest of the existing main function unchanged)

        # File processing mode (existing functionality)
        if args.input_file and args.type:
            success = system.process_file(args.input_file, args.type)
            sys.exit(0 if success else 1)
        
        # If no specific action, show help
        parser.print_help()

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()