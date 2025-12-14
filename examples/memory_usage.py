"""
AgentMemory usage examples.
"""

import os
import sys
from datetime import datetime, timedelta

# Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from keviusdb.memory import (
    AgentMemory,
    MemoryType,
    MessageRole,
    RecencyStrategy,
    ImportanceStrategy,
    RecentImportantStrategy,
    ContextWindowStrategy,
    TokenCounter,
    ContextWindowManager
)


def basic_usage_example():
    """Demonstrate basic agent memory usage."""
    print("=" * 60)
    print("BASIC AGENT MEMORY USAGE")
    print("=" * 60)
    
    # Create agent memory (in-memory for this example)
    memory = AgentMemory()
    
    print("\n1. Adding messages to memory...")
    
    # Add user message
    memory.add_message(
        role=MessageRole.USER,
        content="What's the weather like today?",
        session_id="conversation_1"
    )
    
    # Add assistant response
    memory.add_message(
        role=MessageRole.ASSISTANT,
        content="I don't have real-time weather data, but I can help you find it!",
        session_id="conversation_1"
    )
    
    # Add another exchange
    memory.add_message(
        role=MessageRole.USER,
        content="Remember, my favorite color is blue",
        importance=8.0,  # High importance for facts
        memory_type=MemoryType.SEMANTIC
    )
    
    memory.add_message(
        role=MessageRole.ASSISTANT,
        content="Got it! I'll remember that your favorite color is blue.",
        session_id="conversation_1"
    )
    
    print(f"✓ Added 4 messages to memory")
    
    # Retrieve recent messages
    print("\n2. Retrieving recent messages...")
    recent = memory.get_recent(session_id="conversation_1", limit=2)
    
    for msg in recent:
        print(f"  [{msg.role.value}]: {msg.content}")
    
    # Get conversation statistics
    print("\n3. Memory statistics...")
    session = memory.get_session("conversation_1")
    print(f"  Total messages: {session.metadata.message_count}")
    print(f"  Total tokens: {session.metadata.total_tokens}")
    print(f"  Created: {session.metadata.created_at}")
    
    memory.close()


def memory_types_example():
    """Demonstrate different memory types."""
    print("\n" + "=" * 60)
    print("MEMORY TYPES EXAMPLE")
    print("=" * 60)
    
    memory = AgentMemory()
    
    print("\n1. Adding messages of different types...")
    
    # Short-term memory (working memory, recent context)
    memory.add_message(
        role=MessageRole.USER,
        content="I'm working on a Python project",
        memory_type=MemoryType.SHORT_TERM
    )
    
    # Long-term memory (episodic, historical)
    memory.add_message(
        role=MessageRole.USER,
        content="Last week I completed the database module",
        memory_type=MemoryType.LONG_TERM
    )
    
    # Semantic memory (facts, knowledge)
    memory.add_message(
        role=MessageRole.SYSTEM,
        content="User prefers tabs over spaces",
        memory_type=MemoryType.SEMANTIC
    )
    
    print("✓ Added short-term, long-term, and semantic memories")
    
    # Retrieve by memory type
    print("\n2. Retrieving by memory type...")
    
    short_term = memory.get_all(memory_type=MemoryType.SHORT_TERM)
    long_term = memory.get_all(memory_type=MemoryType.LONG_TERM)
    semantic = memory.get_all(memory_type=MemoryType.SEMANTIC)
    
    print(f"  Short-term memories: {len(short_term)}")
    print(f"  Long-term memories: {len(long_term)}")
    print(f"  Semantic memories: {len(semantic)}")
    
    for msg in semantic:
        print(f"    → {msg.content}")
    
    memory.close()


def session_management_example():
    """Demonstrate session management."""
    print("\n" + "=" * 60)
    print("SESSION MANAGEMENT EXAMPLE")
    print("=" * 60)
    
    memory = AgentMemory()
    
    print("\n1. Creating multiple conversation sessions...")
    
    # Session 1: User Alice
    memory.add_message(
        role=MessageRole.USER,
        content="Hi, I'm Alice. I need help with Python.",
        session_id="user_alice"
    )
    memory.add_message(
        role=MessageRole.ASSISTANT,
        content="Hello Alice! I'd be happy to help with Python.",
        session_id="user_alice"
    )
    
    # Session 2: User Bob
    memory.add_message(
        role=MessageRole.USER,
        content="Hey, I'm Bob. I need help with JavaScript.",
        session_id="user_bob"
    )
    memory.add_message(
        role=MessageRole.ASSISTANT,
        content="Hi Bob! Let's work on JavaScript together.",
        session_id="user_bob"
    )
    
    print("✓ Created sessions for Alice and Bob")
    
    # List all sessions
    print("\n2. Listing all sessions...")
    sessions = memory.list_sessions()
    
    for session in sessions:
        print(f"\n  Session: {session.session_id}")
        print(f"    Messages: {session.metadata.message_count}")
        print(f"    Tokens: {session.metadata.total_tokens}")
        print(f"    Last updated: {session.metadata.updated_at}")
    
    # Retrieve session-specific messages
    print("\n3. Retrieving messages for Alice's session...")
    alice_messages = memory.get_all(session_id="user_alice")
    
    for msg in alice_messages:
        print(f"  [{msg.role.value}]: {msg.content}")
    
    memory.close()


def retrieval_strategies_example():
    """Demonstrate advanced retrieval strategies."""
    print("\n" + "=" * 60)
    print("RETRIEVAL STRATEGIES EXAMPLE")
    print("=" * 60)
    
    memory = AgentMemory()
    
    print("\n1. Adding messages with varying importance...")
    
    # Add messages with different importance levels
    messages_data = [
        ("Normal conversation message", 5.0),
        ("Important user preference", 9.0),
        ("Casual greeting", 3.0),
        ("Critical error information", 10.0),
        ("Another normal message", 5.0),
        ("Key fact about user", 8.5),
    ]
    
    for content, importance in messages_data:
        memory.add_message(
            role=MessageRole.USER,
            content=content,
            importance=importance
        )
    
    print(f"✓ Added {len(messages_data)} messages")
    
    # Recency-based retrieval
    print("\n2. Recency-based retrieval (most recent 3)...")
    recent = memory.get_recent(limit=3)
    for msg in recent:
        print(f"  • {msg.content}")
    
    # Importance-based retrieval
    print("\n3. Importance-based retrieval (importance >= 8.0)...")
    important = memory.get_all(min_importance=8.0)
    for msg in important:
        print(f"  • {msg.content} (importance: {msg.importance})")
    
    # Using retrieval strategies
    print("\n4. Using ImportanceStrategy with time decay...")
    all_messages = memory.get_all()
    strategy = ImportanceStrategy(min_importance=5.0, decay_factor=0.1)
    selected = strategy.retrieve(all_messages, limit=3)
    
    for msg in selected:
        print(f"  • {msg.content}")
    
    memory.close()


def context_window_example():
    """Demonstrate context window management."""
    print("\n" + "=" * 60)
    print("CONTEXT WINDOW MANAGEMENT EXAMPLE")
    print("=" * 60)
    
    memory = AgentMemory()
    
    print("\n1. Simulating a long conversation...")
    
    # Add many messages to simulate a long conversation
    for i in range(20):
        memory.add_message(
            role=MessageRole.USER,
            content=f"This is message number {i} with some content that takes up tokens."
        )
        memory.add_message(
            role=MessageRole.ASSISTANT,
            content=f"Response to message {i} with additional context and information."
        )
    
    print(f"✓ Added 40 messages to conversation")
    
    # Create context window manager
    print("\n2. Managing context window (max 1000 tokens)...")
    manager = ContextWindowManager(max_tokens=1000, reserve_tokens=200)
    
    print(f"  Available tokens: {manager.get_available_tokens()}")
    
    # Get all messages and convert to dict format
    all_messages = memory.get_all()
    messages_dict = [
        {
            'role': msg.role.value,
            'content': msg.content
        }
        for msg in all_messages
    ]
    
    # Get usage stats
    stats = manager.get_usage_stats(messages_dict)
    print(f"\n3. Context usage statistics:")
    print(f"  Total messages: {len(messages_dict)}")
    print(f"  Used tokens: {stats['used_tokens']}")
    print(f"  Remaining tokens: {stats['remaining_tokens']}")
    print(f"  Utilization: {stats['utilization_percent']:.1f}%")
    print(f"  Fits in context: {stats['fits_in_context']}")
    
    # Truncate if needed
    if not stats['fits_in_context']:
        print("\n4. Truncating to fit context window...")
        truncated = manager.truncate(messages_dict, keep_system=True)
        print(f"  Truncated to {len(truncated)} messages")
        
        new_stats = manager.get_usage_stats(truncated)
        print(f"  New token count: {new_stats['used_tokens']}")
        print(f"  Now fits: {new_stats['fits_in_context']}")
    
    # Using ContextWindowStrategy
    print("\n5. Using ContextWindowStrategy retrieval...")
    strategy = ContextWindowStrategy(max_tokens=1000, reserve_tokens=200)
    windowed = strategy.retrieve(all_messages, limit=50)
    
    print(f"  Retrieved {len(windowed)} messages that fit in context")
    total_tokens = sum(msg.tokens for msg in windowed)
    print(f"  Total tokens: {total_tokens}")
    
    memory.close()


def token_counting_example():
    """Demonstrate token counting utilities."""
    print("\n" + "=" * 60)
    print("TOKEN COUNTING EXAMPLE")
    print("=" * 60)
    
    print("\n1. Estimating tokens for different texts...")
    
    texts = [
        "Hello!",
        "This is a longer sentence with more words.",
        "The quick brown fox jumps over the lazy dog. " * 10
    ]
    
    for text in texts:
        tokens = TokenCounter.estimate_tokens(text)
        print(f"  Text length: {len(text)} chars → {tokens} tokens")
    
    print("\n2. Comparing token counts across models...")
    test_text = "This is a test message for different AI models."
    
    for model in ['gpt-4', 'gpt-3.5-turbo', 'claude', 'default']:
        tokens = TokenCounter.estimate_tokens(test_text, model=model)
        print(f"  {model:15} → {tokens} tokens")
    
    print("\n3. Counting conversation tokens...")
    conversation = [
        {'role': 'user', 'content': 'What is Python?'},
        {'role': 'assistant', 'content': 'Python is a high-level programming language.'},
        {'role': 'user', 'content': 'What are its main features?'},
        {'role': 'assistant', 'content': 'Python features include simplicity, readability, and versatility.'}
    ]
    
    total = TokenCounter.count_conversation_tokens(conversation)
    print(f"  Total conversation tokens: {total}")
    


def persistent_storage_example():
    """Demonstrate persistent storage."""
    print("\n" + "=" * 60)
    print("PERSISTENT STORAGE EXAMPLE")
    print("=" * 60)
    
    db_path = "agent_memory_demo.db"
    
    print("\n1. Creating memory with persistent storage...")
    
    # Create memory and add data
    memory1 = AgentMemory(db_path=db_path)
    memory1.add_message(
        role=MessageRole.USER,
        content="This message will persist across sessions",
        importance=9.0
    )
    memory1.add_message(
        role=MessageRole.ASSISTANT,
        content="I'll remember this even after closing!"
    )
    
    print(f"✓ Added messages to {db_path}")
    memory1.flush()
    memory1.close()
    
    print("\n2. Reopening memory from disk...")
    
    # Open new instance from same file
    memory2 = AgentMemory(db_path=db_path)
    messages = memory2.get_all()
    
    print(f"✓ Loaded {len(messages)} messages from disk")
    for msg in messages:
        print(f"  [{msg.role.value}]: {msg.content}")
    
    memory2.close()
    
    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"\n✓ Cleaned up {db_path}")
    


def main():
    """Run all examples."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 58 + "║")
    print("║" + "  KeviusDB Agent Memory - Examples  ".center(58) + "║")
    print("║" + " " * 58 + "║")
    print("╚" + "=" * 58 + "╝")
    
    try:
        basic_usage_example()
        memory_types_example()
        session_management_example()
        retrieval_strategies_example()
        context_window_example()
        token_counting_example()
        persistent_storage_example()
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
