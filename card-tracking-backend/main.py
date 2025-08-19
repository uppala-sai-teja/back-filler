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
    
    def simulate_bank_api_call(self, since: Optional[str] = None) -> List[Dict]:
        """Simulate GET /applications?status=submitted&since=last_poll_time"""
        print(f"🔍 Simulating bank API call (since: {since})...")
        
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
        
        print(f"✅ Found {len(simulated_new_applications)} new applications")
        return simulated_new_applications
    
    def simulate_manufacturer_api_call(self, application_ids: List[str]) -> List[Dict]:
        """Simulate POST /production-status with bulk application IDs"""
        print(f"🔍 Simulating manufacturer API call for {len(application_ids)} applications...")
        
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
        
        print(f"✅ Retrieved production status for {len(simulated_responses)} applications")
        return simulated_responses
    
    def simulate_logistics_api_call(self, tracking_numbers: List[str]) -> List[Dict]:
        """Simulate POST /tracking with bulk AWB numbers"""
        print(f"🔍 Simulating logistics API call for {len(tracking_numbers)} packages...")
        
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
        
        print(f"✅ Retrieved tracking updates for {len(simulated_responses)} packages")
        return simulated_responses
    
    def fetch_bank_applications(self) -> bool:
        """Fetch new bank applications and process them"""
        print("🏦 Fetching new bank applications...")
        
        try:
            # Get new applications from simulated API
            since = self.last_poll_times.get("bank")
            new_applications = self.simulate_bank_api_call(since)
            
            if not new_applications:
                print("📭 No new applications found")
                return True
            
            # Process new applications
            template = self.processor.get_template("bank")
            if not template:
                print("❌ Could not load bank template")
                return False
            
            success = self.processor.process_bulk_data(new_applications, template)
            
            if success:
                # Update all cards with pending stages
                self.update_all_pending_stages()
                
                # Update last poll time
                self.last_poll_times["bank"] = datetime.now().isoformat() + "Z"
                self.save_poll_times()
                
                print(f"✅ Successfully processed {len(new_applications)} new applications")
                self.processor.print_stats()
                return True
            else:
                print("❌ Failed to process bank applications")
                return False
                
        except Exception as e:
            print(f"❌ Error fetching bank applications: {e}")
            return False
    
    def fetch_manufacturer_data(self) -> bool:
        """Fetch manufacturer data for cards without manufacturer_order_id"""
        print("🏭 Fetching manufacturer data...")
        
        try:
            # Find cards that need manufacturer updates
            cards_needing_updates = []
            customers = list(self.processor.db_manager.customers_collection.find())
            
            for customer in customers:
                for card in customer.get("cards", []):
                    tracking_ids = card.get("tracking_ids", {})
                    if (tracking_ids.get("application_id") and 
                        not tracking_ids.get("manufacturer_order_id")):
                        cards_needing_updates.append(tracking_ids["application_id"])
            
            if not cards_needing_updates:
                print("📭 No cards need manufacturer updates")
                return True
            
            print(f"🔍 Found {len(cards_needing_updates)} cards needing manufacturer updates")
            
            # Get manufacturer data from simulated API
            manufacturer_data = self.simulate_manufacturer_api_call(cards_needing_updates)
            
            # Process manufacturer data
            template = self.processor.get_template("card_manufacturer")
            if not template:
                print("❌ Could not load manufacturer template")
                return False
            
            success = self.processor.process_bulk_data(manufacturer_data, template)
            
            if success:
                # Update all cards with pending stages
                self.update_all_pending_stages()
                
                # Update last poll time
                self.last_poll_times["manufacturer"] = datetime.now().isoformat() + "Z"
                self.save_poll_times()
                
                print(f"✅ Successfully processed manufacturer data for {len(manufacturer_data)} cards")
                self.processor.print_stats()
                return True
            else:
                print("❌ Failed to process manufacturer data")
                return False
                
        except Exception as e:
            print(f"❌ Error fetching manufacturer data: {e}")
            return False
    
    def fetch_logistics_data(self) -> bool:
        """Fetch logistics data for cards that are not yet delivered"""
        print("🚚 Fetching logistics data...")
        
        try:
            # Find cards that need logistics updates
            tracking_numbers_needing_updates = []
            customers = list(self.processor.db_manager.customers_collection.find())
            
            for customer in customers:
                for card in customer.get("cards", []):
                    current_status = card.get("current_status", {}).get("status")
                    tracking_number = card.get("tracking_ids", {}).get("logistics_tracking_number")
                    
                    if (tracking_number and 
                        current_status not in ["DELIVERED", "RETURNED_TO_SENDER"]):
                        tracking_numbers_needing_updates.append(tracking_number)
            
            if not tracking_numbers_needing_updates:
                print("📭 No packages need logistics updates")
                return True
            
            print(f"🔍 Found {len(tracking_numbers_needing_updates)} packages needing logistics updates")
            
            # Get logistics data from simulated API
            logistics_data = self.simulate_logistics_api_call(tracking_numbers_needing_updates)
            
            # Process logistics data
            template = self.processor.get_template("logistics")
            if not template:
                print("❌ Could not load logistics template")
                return False
            
            success = self.processor.process_bulk_data(logistics_data, template)
            
            if success:
                # Update all cards with pending stages
                self.update_all_pending_stages()
                
                # Update last poll time
                self.last_poll_times["logistics"] = datetime.now().isoformat() + "Z"
                self.save_poll_times()
                
                print(f"✅ Successfully processed logistics data for {len(logistics_data)} packages")
                self.processor.print_stats()
                return True
            else:
                print("❌ Failed to process logistics data")
                return False
                
        except Exception as e:
            print(f"❌ Error fetching logistics data: {e}")
            return False
    
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
    
    def generate_track_sheet(self) -> bool:
        """Generate track_sheet.json with summarized tracking data"""
        print("📋 Generating track sheet...")
        
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
            
            # Save track sheet
            with open('track_sheet.json', 'w') as f:
                json.dump(track_sheet, f, indent=2, default=str)
            
            print(f"✅ Generated track sheet with {len(track_sheet)} applications")
            print(f"📁 Saved to: track_sheet.json")
            
            # Print summary
            stage_summary = {}
            for app_data in track_sheet.values():
                stage = app_data["current_stage"]
                stage_summary[stage] = stage_summary.get(stage, 0) + 1
            
            print(f"\n📊 Track Sheet Summary:")
            for stage, count in stage_summary.items():
                print(f"  {stage}: {count} applications")
            
            return True
            
        except Exception as e:
            print(f"❌ Error generating track sheet: {e}")
            return False
    
    def process_file(self, input_file: str, provider_type: str) -> bool:
        """Process input file (existing functionality)"""
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return False

        template = self.processor.get_template(provider_type)
        if not template:
            print(f"❌ Could not load template for {provider_type}")
            return False

        with open(input_file, 'r') as f:
            input_data = json.load(f)

        print(f"🚀 Processing {len(input_data)} records from {input_file}")
        
        success = self.processor.process_bulk_data(input_data, template)
        
        if success:
            # Update all cards with pending stages
            self.update_all_pending_stages()
            
            self.processor.print_stats()
            self.processor.print_analytics()
            print(f"✅ Processing completed successfully!")
            return True
        else:
            print(f"❌ Processing failed!")
            return False


def main():
    parser = argparse.ArgumentParser(description="Enhanced Card Tracking System with API Fetching")
    
    # Existing file processing arguments
    parser.add_argument("input_file", nargs='?', help="Input JSON data file")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'], 
                       help="Type of data to process")
    
    # New API fetching arguments
    parser.add_argument("--fetch-bank", action="store_true", 
                       help="Fetch new bank applications")
    parser.add_argument("--fetch-manufacturer", action="store_true", 
                       help="Fetch manufacturer data for pending cards")
    parser.add_argument("--fetch-logistics", action="store_true", 
                       help="Fetch logistics data for undelivered packages")
    parser.add_argument("--track-sheet", action="store_true", 
                       help="Generate track_sheet.json summary")
    
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
                print("✅ MongoDB connection successful!")
                db_manager.disconnect()
            else:
                print("❌ MongoDB connection failed!")
            return

        # Initialize enhanced system
        system = EnhancedCardTrackingSystem(debug=args.debug)

        # Handle fetch operations
        if args.fetch_bank:
            success = system.fetch_bank_applications()
            system.processor.print_analytics()
            sys.exit(0 if success else 1)
        
        if args.fetch_manufacturer:
            success = system.fetch_manufacturer_data()
            system.processor.print_analytics()
            sys.exit(0 if success else 1)
        
        if args.fetch_logistics:
            success = system.fetch_logistics_data()
            system.processor.print_analytics()
            sys.exit(0 if success else 1)
        
        if args.track_sheet:
            success = system.generate_track_sheet()
            sys.exit(0 if success else 1)

        # Analytics mode
        if args.analytics:
            system.processor.print_analytics()
            return

        # File processing mode (existing functionality)
        if args.input_file and args.type:
            success = system.process_file(args.input_file, args.type)
            sys.exit(0 if success else 1)
        
        # If no specific action, show help
        parser.print_help()

    except KeyboardInterrupt:
        print("\n⚠️ Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()