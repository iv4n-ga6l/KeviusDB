"""
Basic usage example for KeviusDB.
Demonstrates core functionality and features.
"""

import os
import sys

# Add the parent directory to the path so we can import keviusdb
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from keviusdb import KeviusDB, create_memory_database
from keviusdb.comparison import ReverseComparison, NumericComparison


def basic_operations_example():
    """Demonstrate basic put, get, delete operations."""
    print("=== Basic Operations Example ===")
    
    # Create in-memory database
    db = create_memory_database()
    
    # Put some data
    db.put("name", "Alice")
    db.put("age", "30")
    db.put("city", "New York")
    
    # Get data
    print(f"Name: {db.get('name')}")
    print(f"Age: {db.get('age')}")
    print(f"City: {db.get('city')}")
    
    # Check if key exists
    print(f"Has 'email': {db.contains('email')}")
    
    # Delete data
    deleted = db.delete("age")
    print(f"Deleted 'age': {deleted}")
    print(f"Age after deletion: {db.get('age')}")
    
    # Dictionary-like access
    db["country"] = "USA"
    print(f"Country: {db['country']}")
    print(f"Database size: {len(db)}")
    
    db.close()


def batch_operations_example():
    """Demonstrate atomic batch operations."""
    print("\n=== Batch Operations Example ===")
    
    db = create_memory_database()
    
    # Initial data
    db.put("balance", "1000")
    print(f"Initial balance: {db.get('balance')}")
    
    # Atomic batch operation
    try:
        with db.batch() as batch:
            batch.put("balance", "1500")
            batch.put("last_transaction", "deposit_500")
            batch.put("transaction_time", "2025-06-09T10:00:00")
            # All operations committed together
        
        print(f"After batch - Balance: {db.get('balance')}")
        print(f"Last transaction: {db.get('last_transaction')}")
    except Exception as e:
        print(f"Batch failed: {e}")
    
    db.close()


def iteration_example():
    """Demonstrate iteration capabilities."""
    print("\n=== Iteration Example ===")
    
    db = create_memory_database()
    
    # Add some data
    fruits = {
        "apple": "red",
        "banana": "yellow", 
        "cherry": "red",
        "date": "brown",
        "elderberry": "purple"
    }
    
    for fruit, color in fruits.items():
        db.put(fruit, color)
    
    # Forward iteration
    print("Forward iteration:")
    for key, value in db.iterate():
        print(f"  {key}: {value}")
    
    # Reverse iteration
    print("\nReverse iteration:")
    for key, value in db.iterate(reverse=True):
        print(f"  {key}: {value}")
    
    # Range iteration
    print("\nRange iteration (banana to date):")
    for key, value in db.iterate(start_key="banana", end_key="date"):
        print(f"  {key}: {value}")
    
    # Prefix iteration
    print("\nPrefix iteration (starts with 'e'):")
    for key, value in db.iterate_prefix("e"):
        print(f"  {key}: {value}")
    
    # Keys only
    print("\nKeys only:")
    for key in db.iterate_keys():
        print(f"  {key}")
    
    db.close()


def snapshot_example():
    """Demonstrate snapshot functionality."""
    print("\n=== Snapshot Example ===")
    
    db = create_memory_database()
    
    # Initial data
    db.put("version", "1.0")
    db.put("status", "active")
    
    # Create snapshot
    snapshot = db.snapshot()
    
    # Modify original database
    db.put("version", "1.1")
    db.put("status", "updated")
    db.put("new_feature", "enabled")
    
    print("Original database after changes:")
    for key, value in db.iterate():
        print(f"  {key}: {value}")
    
    print("\nSnapshot (unchanged):")
    for key, value in snapshot.iterate():
        print(f"  {key}: {value}")
    
    db.close()


def custom_comparison_example():
    """Demonstrate custom comparison functions."""
    print("\n=== Custom Comparison Example ===")
    
    # Reverse order database
    print("Reverse order comparison:")
    db_reverse = KeviusDB(comparison_func=ReverseComparison(), in_memory=True)
    
    numbers = ["1", "2", "3", "10", "20"]
    for num in numbers:
        db_reverse.put(num, f"value_{num}")
    
    for key, value in db_reverse.iterate():
        print(f"  {key}: {value}")
    
    # Numeric comparison
    print("\nNumeric comparison:")
    db_numeric = KeviusDB(comparison_func=NumericComparison(), in_memory=True)
    
    for num in numbers:
        db_numeric.put(num, f"value_{num}")
    
    for key, value in db_numeric.iterate():
        print(f"  {key}: {value}")
    
    db_reverse.close()
    db_numeric.close()


def persistent_storage_example():
    """Demonstrate persistent storage with compression."""
    print("\n=== Persistent Storage Example ===")
    
    db_file = "example.kvdb"
    
    # Create persistent database
    db = KeviusDB(db_file)
    
    # Add data
    for i in range(5):
        db.put(f"key_{i:03d}", f"This is value number {i} with some text to compress")
    
    print(f"Added {len(db)} items to persistent database")
    
    # Close and reopen
    db.close()
    
    # Reopen and verify data persists
    db2 = KeviusDB(db_file)
    print(f"Reopened database has {len(db2)} items")
    
    for key, value in db2.iterate():
        print(f"  {key}: {value[:30]}...")
    
    db2.close()
    
    # Cleanup
    if os.path.exists(db_file):
        os.remove(db_file)


def performance_example():
    """Demonstrate performance with larger dataset."""
    print("\n=== Performance Example ===")
    
    db = create_memory_database()
    
    # Insert many items
    import time
    start_time = time.time()
    
    num_items = 10000
    for i in range(num_items):
        db.put(f"key_{i:06d}", f"value_{i}")
    
    insert_time = time.time() - start_time
    print(f"Inserted {num_items} items in {insert_time:.3f} seconds")
    print(f"Rate: {num_items/insert_time:.0f} inserts/second")
    
    # Read all items
    start_time = time.time()
    count = 0
    for key, value in db.iterate():
        count += 1
    
    read_time = time.time() - start_time
    print(f"Read {count} items in {read_time:.3f} seconds")
    print(f"Rate: {count/read_time:.0f} reads/second")
    
    db.close()


if __name__ == "__main__":
    basic_operations_example()
    batch_operations_example()
    iteration_example()
    snapshot_example()
    custom_comparison_example()
    persistent_storage_example()
    performance_example()
    
    print("\n=== All examples completed successfully! ===")
