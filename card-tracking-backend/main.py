# main.py
#!/usr/bin/env python3
"""
Card Tracking System - Main Entry Point
"""

import argparse
import json
import os
import sys
from core.card_processor import CardTrackingProcessor

def main():
    parser = argparse.ArgumentParser(description="Card Tracking System with MongoDB")
    parser.add_argument("input_file", nargs='?', help="Input JSON data file")
    parser.add_argument("--type", choices=['bank', 'card_manufacturer', 'logistics'], 
                       help="Type of data to process")
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

        # Initialize processor
        processor = CardTrackingProcessor(debug=args.debug)

        # Analytics mode
        if args.analytics:
            processor.print_analytics()
            return

        # Processing mode
        if not args.input_file or not args.type:
            parser.print_help()
            return

        if not os.path.exists(args.input_file):
            print(f"❌ File not found: {args.input_file}")
            return

        # Load template
        template = processor.get_template(args.type)
        if not template:
            print(f"❌ Could not load template for {args.type}")
            return

        # Load and process data
        with open(args.input_file, 'r') as f:
            input_data = json.load(f)

        print(f"🚀 Processing {len(input_data)} records from {args.input_file}")
        
        if processor.process_bulk_data(input_data, template):
            processor.print_stats()
            processor.print_analytics()
            print(f"✅ Processing completed successfully!")
        else:
            print(f"❌ Processing failed!")

    except KeyboardInterrupt:
        print("\n⚠️ Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()