
# README.md
# Card Tracking System - Backend

A Python-based card tracking system with MongoDB integration for tracking credit/debit card applications from submission to delivery.

## 🚀 Quick Start

### 1. Setup Environment
```bash
# Clone/create project directory
mkdir card-tracking-backend
cd card-tracking-backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# OR
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure MongoDB
```bash
# Copy environment template
cp config/.env.template .env

# Edit .env with your MongoDB settings
# For local MongoDB:
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE=card_tracking

# For MongoDB Atlas:
# MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/
```

### 3. Test Connection
```bash
python main.py --test-connection
```

### 4. Create Sample Data
```bash
python scripts/setup_sample_data.py
```

### 5. Process Data
```bash
# Process bank data
python main.py data/sample_bank_data.json --type bank

# Process manufacturer data
python main.py data/sample_manufacturer_data.json --type card_manufacturer

# Process logistics data
python main.py data/sample_logistics_data.json --type logistics

# View analytics
python main.py --analytics
```

## 📁 File Structure
```
card-tracking-backend/
├── core/
│   ├── __init__.py
│   ├── mongodb_manager.py      # MongoDB operations
│   └── card_processor.py       # Main processing logic
├── config/
│   ├── master_config.json      # Provider configurations
│   └── .env.template          # Environment template
├── data/                      # Sample data files
├── scripts/                   # Utility scripts
├── logs/                      # Log files (auto-created)
├── requirements.txt           # Python dependencies
├── .env                      # Environment variables
├── main.py                   # Main entry point
└── README.md                 # This file
```

## 🔧 Usage

### Command Line Interface
```bash
# Test MongoDB connection
python main.py --test-connection

# Process data files
python main.py <data_file.json> --type <bank|card_manufacturer|logistics>

# View analytics dashboard
python main.py --analytics

# Enable debug logging
python main.py <data_file.json> --type bank --debug
```

### Data Flow
1. **Bank Data** → Customer creation + Application status
2. **Manufacturer Data** → Production timeline
3. **Logistics Data** → Shipping and delivery updates

## 📊 Data Structure

### Customer Document
```json
{
  "_id": "CUST_001",
  "customer_info": {
    "name": "John Doe",
    "mobile": "+919876543210",
    "email": "john@example.com"
  },
  "cards": [
    {
      "card_id": "CARD_APP_001_12345",
      "tracking_ids": {
        "application_id": "APP_001",
        "manufacturer_order_id": "MFG_ORDER_001",
        "logistics_tracking_number": "DTDC123456789"
      },
      "current_status": {
        "status": "DELIVERED",
        "stage": "shipping_and_delivery",
        "location": "Customer Address"
      },
      "timeline": {
        "application_and_approval": [...],
        "card_production": [...],
        "shipping_and_delivery": [...]
      },
      "application_metadata": {
        "courier_partner": "DTDC",
        "production_batch": "BATCH_2025_08_001"
      }
    }
  ]
}
```

## 🔍 Monitoring

### Log Files
- `logs/processor.log` - Processing events
- MongoDB logs via MongoDB tools

### Analytics
- Status distribution
- Bank performance metrics
- Processing statistics

## 🛠 Development

### Adding New Providers
1. Add configuration to `config/master_config.json`
2. Update field mappings and status mappings
3. Test with sample data

### Extending Functionality
- Modify `core/card_processor.py` for new business logic
- Update `core/mongodb_manager.py` for new database operations

## 📋 Requirements
- Python 3.8+
- MongoDB 4.4+ (local or Atlas)
- Required packages in `requirements.txt`

## 🚀 Next Steps
After backend setup:
1. Build REST API layer (Node.js)
2. Create React frontend
3. Add real-time notifications
4. Setup monitoring and alerts