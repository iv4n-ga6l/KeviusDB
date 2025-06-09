import os
import sys

# Add the parent directory to the path so we can import keviusdb
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from keviusdb import KeviusDB

# Create database
db = KeviusDB("mydb.kvdb")

# Basic operations
db.put("key1", "value1")
value = db.get("key1")
db.delete("key1")

# Batch operations
with db.batch() as batch:
    batch.put("key2", "value2")
    batch.put("key3", "value3")

# Snapshots
snapshot = db.snapshot()

db.put("key2", "new_value2")

for key, value in snapshot.iterate():
    print(f"{key}: {value}")

# Iteration
for key, value in db.iterate():
    print(f"{key}: {value}")