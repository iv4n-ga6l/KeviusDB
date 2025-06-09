"""
Advanced usage examples for KeviusDB.
"""

import os
import sys

# Add the parent directory to the path so we can import keviusdb
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from keviusdb import KeviusDB
from keviusdb.interfaces import FileSystemInterface, CompressionInterface
from keviusdb.iteration import IteratorFactory


class CustomFileSystem(FileSystemInterface):
    """Custom filesystem that logs all operations."""
    
    def __init__(self):
        self.operations = []
    
    def read_file(self, path: str) -> bytes:
        self.operations.append(f"READ: {path}")
        try:
            with open(path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {path}")
    
    def write_file(self, path: str, data: bytes) -> None:
        self.operations.append(f"WRITE: {path} ({len(data)} bytes)")
        directory = os.path.dirname(path)
        if directory:
            self.create_directory(directory)
        with open(path, 'wb') as f:
            f.write(data)
    
    def file_exists(self, path: str) -> bool:
        self.operations.append(f"EXISTS: {path}")
        return os.path.exists(path)
    
    def delete_file(self, path: str) -> None:
        self.operations.append(f"DELETE: {path}")
        if self.file_exists(path):
            os.remove(path)
    
    def create_directory(self, path: str) -> None:
        self.operations.append(f"MKDIR: {path}")
        if path and not os.path.exists(path):
            os.makedirs(path, exist_ok=True)


class NoCompressionInterface(CompressionInterface):
    """Custom compression that doesn't actually compress."""
    
    def compress(self, data: bytes) -> bytes:
        # Add a simple header to identify uncompressed data
        return b"UNCOMPRESSED:" + data
    
    def decompress(self, compressed_data: bytes) -> bytes:
        if compressed_data.startswith(b"UNCOMPRESSED:"):
            return compressed_data[13:]  # Remove header
        return compressed_data


def custom_filesystem_example():
    """Demonstrate custom filesystem interface."""
    print("=== Custom Filesystem Example ===")
    
    custom_fs = CustomFileSystem()
    db_file = "custom_fs_test.kvdb"
    
    # Create database with custom filesystem
    db = KeviusDB(db_file, filesystem=custom_fs)
    
    # Perform operations
    db.put("test1", "value1")
    db.put("test2", "value2")
    db.flush()  # Force write to filesystem
    
    value = db.get("test1")
    print(f"Retrieved: {value}")
    
    db.close()
    
    # Show filesystem operations
    print("\nFilesystem operations:")
    for op in custom_fs.operations:
        print(f"  {op}")
    
    # Cleanup
    if os.path.exists(db_file):
        os.remove(db_file)


def custom_compression_example():
    """Demonstrate custom compression interface."""
    print("\n=== Custom Compression Example ===")
    
    db_file = "no_compression_test.kvdb"
    
    # Create database with no compression
    db = KeviusDB(db_file, compression=NoCompressionInterface())
    
    # Add some data
    db.put("message", "This data will not be compressed")
    db.put("data", "A" * 1000)  # Repetitive data that would compress well
    
    db.close()
    
    # Check file size (should be larger without compression)
    if os.path.exists(db_file):
        file_size = os.path.getsize(db_file)
        print(f"Database file size without compression: {file_size} bytes")
        
        # Compare with compressed version
        db2 = KeviusDB("compressed_test.kvdb")  # Default LZ4 compression
        db2.put("message", "This data will not be compressed")
        db2.put("data", "A" * 1000)
        db2.close()
        
        compressed_size = os.path.getsize("compressed_test.kvdb")
        print(f"Database file size with compression: {compressed_size} bytes")
        print(f"Compression ratio: {file_size/compressed_size:.2f}x")
        
        # Cleanup
        os.remove(db_file)
        os.remove("compressed_test.kvdb")


def advanced_iteration_example():
    """Demonstrate advanced iteration features."""
    print("\n=== Advanced Iteration Example ===")
    
    db = KeviusDB(in_memory=True)
    
    # Add test data
    data = {
        f"user_{i:03d}": f"User {i} data"
        for i in range(100)
    }
    
    for key, value in data.items():
        db.put(key, value)
    
    # Get iterator factory
    from keviusdb.storage import MemoryStorage
    iterator_factory = IteratorFactory(db._storage)
    
    # Range iterator with limit and skip
    print("Range iterator (skip 10, limit 5):")
    range_iter = iterator_factory.range_iterator(
        start_key=b"user_010",
        limit=5,
        skip=10
    )
    
    for key_bytes, value_bytes in range_iter:
        key = key_bytes.decode('utf-8')
        value = value_bytes.decode('utf-8')
        print(f"  {key}: {value}")
    
    # Prefix iterator
    print("\nPrefix iterator (user_05*):")
    prefix_iter = iterator_factory.prefix_iterator(b"user_05")
    
    for key_bytes, value_bytes in prefix_iter:
        key = key_bytes.decode('utf-8')
        value = value_bytes.decode('utf-8')
        print(f"  {key}: {value}")
    
    db.close()


def savepoint_example():
    """Demonstrate savepoint functionality."""
    print("\n=== Savepoint Example ===")
    
    db = KeviusDB(in_memory=True)
    
    # Initial data
    db.put("balance", "1000")
    db.put("account", "checking")
    
    # Create advanced batch with savepoints
    from keviusdb.transaction import AdvancedBatch
    batch = AdvancedBatch(db._storage)
    
    try:
        # First set of operations
        batch.put(b"balance", b"900")
        batch.put(b"transaction1", b"withdraw_100")
        
        # Create savepoint
        savepoint1 = batch.create_savepoint()
        
        # More operations
        batch.put(b"balance", b"800")
        batch.put(b"transaction2", b"withdraw_100")
        
        # Create another savepoint
        savepoint2 = batch.create_savepoint()
        
        # Problematic operation
        batch.put(b"balance", b"700")
        batch.put(b"transaction3", b"withdraw_100")
        
        # Simulate error - rollback to savepoint2
        print("Rolling back to savepoint2...")
        batch.rollback_to_savepoint(savepoint2)
        
        # Continue with different operations
        batch.put(b"balance", b"850")
        batch.put(b"transaction3", b"deposit_50")
        
        # Commit the batch
        batch.commit()
        
        print("Final state after savepoint rollback:")
        for key, value in db.iterate():
            print(f"  {key}: {value}")
            
    except Exception as e:
        print(f"Batch failed: {e}")
        batch.rollback()
    
    db.close()


def concurrent_snapshot_example():
    """Demonstrate multiple snapshots for consistency."""
    print("\n=== Concurrent Snapshot Example ===")
    
    db = KeviusDB(in_memory=True)
    
    # Initial state
    db.put("counter", "0")
    db.put("status", "initialized")
    
    # Create first snapshot
    snapshot1 = db.snapshot()
    
    # Modify database
    db.put("counter", "10")
    db.put("status", "processing")
    
    # Create second snapshot
    snapshot2 = db.snapshot()
    
    # More modifications
    db.put("counter", "20")
    db.put("status", "completed")
    
    # Create third snapshot
    snapshot3 = db.snapshot()
    
    print("Current database state:")
    for key, value in db.iterate():
        print(f"  {key}: {value}")
    
    print("\nSnapshot1 (initial state):")
    for key, value in snapshot1.iterate():
        print(f"  {key}: {value}")
    
    print("\nSnapshot2 (intermediate state):")
    for key, value in snapshot2.iterate():
        print(f"  {key}: {value}")
    
    print("\nSnapshot3 (near-final state):")
    for key, value in snapshot3.iterate():
        print(f"  {key}: {value}")
    
    db.close()


def bulk_operations_example():
    """Demonstrate bulk operations and performance considerations."""
    print("\n=== Bulk Operations Example ===")
    
    db = KeviusDB(in_memory=True)
    
    import time
    
    # Individual puts (slower)
    start_time = time.time()
    for i in range(1000):
        db.put(f"individual_{i:04d}", f"value_{i}")
    individual_time = time.time() - start_time
    
    # Batch puts (faster)
    start_time = time.time()
    with db.batch() as batch:
        for i in range(1000):
            batch.put(f"batch_{i:04d}", f"value_{i}")
    batch_time = time.time() - start_time
    
    print(f"Individual puts: {individual_time:.3f} seconds")
    print(f"Batch puts: {batch_time:.3f} seconds")
    print(f"Batch is {individual_time/batch_time:.1f}x faster")
    
    print(f"Total items in database: {len(db)}")
    
    db.close()


def database_migration_example():
    """Demonstrate database migration between different configurations."""
    print("\n=== Database Migration Example ===")
    
    # Create source database with default comparison
    source_db = KeviusDB("source.kvdb")
    
    # Add data
    items = ["zebra", "apple", "banana", "cherry"]
    for item in items:
        source_db.put(item, f"data_for_{item}")
    
    print("Source database (default lexicographic order):")
    for key, value in source_db.iterate():
        print(f"  {key}: {value}")
    
    # Create target database with reverse comparison
    from keviusdb.comparison import ReverseComparison
    target_db = KeviusDB("target.kvdb", comparison_func=ReverseComparison())
    
    # Migrate data
    with target_db.batch() as batch:
        for key, value in source_db.iterate():
            batch.put(key, value)
    
    print("\nTarget database (reverse order):")
    for key, value in target_db.iterate():
        print(f"  {key}: {value}")
    
    source_db.close()
    target_db.close()
    
    # Cleanup
    for file in ["source.kvdb", "target.kvdb"]:
        if os.path.exists(file):
            os.remove(file)


if __name__ == "__main__":
    custom_filesystem_example()
    custom_compression_example()
    advanced_iteration_example()
    savepoint_example()
    concurrent_snapshot_example()
    bulk_operations_example()
    database_migration_example()
    
    print("\n=== All advanced examples completed successfully! ===")
