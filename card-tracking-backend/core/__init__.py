# core/__init__.py
"""
Card Tracking System - Core Module

This module contains the core functionality for the card tracking system:
- MongoDBManager: Database operations and connection management
- CardTrackingProcessor: Main data processing and business logic
"""

from .mongodb_manager import MongoDBManager
from .card_processor import CardTrackingProcessor

__version__ = "1.0.0"
__author__ = "Card Tracking Team"

# Export main classes
__all__ = [
    "MongoDBManager",
    "CardTrackingProcessor"
]