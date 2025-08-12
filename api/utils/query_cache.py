"""
Simple query caching mechanism to ensure deterministic results for identical analytical questions.
"""

import hashlib
import json
import time
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class QueryCache:
    """
    Simple in-memory cache for analytical query results.
    
    This cache stores the results of analytical queries to ensure that
    identical questions return identical results, improving consistency
    and reducing API calls to the AI service.
    """
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600):
        """
        Initialize the query cache.
        
        Args:
            max_size: Maximum number of cached entries
            ttl_seconds: Time-to-live for cached entries in seconds (default: 1 hour)
        """
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        
    def _generate_cache_key(self, question: str, db_path: str) -> str:
        """
        Generate a cache key from the question and database path.
        
        Args:
            question: The analytical question
            db_path: Path to the database file
            
        Returns:
            SHA256 hash of the normalized question and db path
        """
        # Normalize the question (lowercase, strip whitespace)
        normalized_question = question.lower().strip()
        
        # Create a string combining question and db path
        cache_input = f"{normalized_question}|{db_path}"
        
        # Generate SHA256 hash
        return hashlib.sha256(cache_input.encode('utf-8')).hexdigest()
    
    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        """
        Check if a cache entry is expired.
        
        Args:
            entry: Cache entry with timestamp
            
        Returns:
            True if expired, False otherwise
        """
        current_time = time.time()
        entry_time = entry.get('timestamp', 0)
        return (current_time - entry_time) > self.ttl_seconds
    
    def _cleanup_expired(self):
        """Remove expired entries from the cache."""
        current_time = time.time()
        expired_keys = []
        
        for key, entry in self.cache.items():
            if self._is_expired(entry):
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.cache[key]
            
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def _enforce_size_limit(self):
        """Remove oldest entries if cache exceeds max size."""
        if len(self.cache) <= self.max_size:
            return
            
        # Sort by timestamp and remove oldest entries
        sorted_entries = sorted(
            self.cache.items(),
            key=lambda x: x[1].get('timestamp', 0)
        )
        
        entries_to_remove = len(self.cache) - self.max_size
        for i in range(entries_to_remove):
            key = sorted_entries[i][0]
            del self.cache[key]
            
        logger.info(f"Removed {entries_to_remove} entries to enforce cache size limit")
    
    def get(self, question: str, db_path: str) -> Optional[Dict[str, Any]]:
        """
        Get cached result for a question.
        
        Args:
            question: The analytical question
            db_path: Path to the database file
            
        Returns:
            Cached result if found and not expired, None otherwise
        """
        # Clean up expired entries periodically
        self._cleanup_expired()
        
        cache_key = self._generate_cache_key(question, db_path)
        
        if cache_key not in self.cache:
            return None
            
        entry = self.cache[cache_key]
        
        if self._is_expired(entry):
            del self.cache[cache_key]
            return None
            
        logger.info(f"Cache hit for question: {question[:50]}...")
        return entry['result']
    
    def put(self, question: str, db_path: str, result: Dict[str, Any]):
        """
        Store result in cache.
        
        Args:
            question: The analytical question
            db_path: Path to the database file
            result: The result to cache
        """
        cache_key = self._generate_cache_key(question, db_path)
        
        entry = {
            'result': result,
            'timestamp': time.time(),
            'question': question[:100],  # Store truncated question for debugging
            'db_path': db_path
        }
        
        self.cache[cache_key] = entry
        
        # Enforce size limits
        self._enforce_size_limit()
        
        logger.info(f"Cached result for question: {question[:50]}...")
    
    def clear(self):
        """Clear all cached entries."""
        self.cache.clear()
        logger.info("Cache cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        current_time = time.time()
        expired_count = sum(
            1 for entry in self.cache.values()
            if self._is_expired(entry)
        )
        
        return {
            'total_entries': len(self.cache),
            'expired_entries': expired_count,
            'active_entries': len(self.cache) - expired_count,
            'max_size': self.max_size,
            'ttl_seconds': self.ttl_seconds
        }

# Global cache instance
query_cache = QueryCache() 