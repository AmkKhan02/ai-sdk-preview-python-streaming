"""
File registry for tracking uploaded DuckDB files.

This module provides functionality to track uploaded DuckDB files
so that they can be referenced in analytical queries.
"""

import os
import json
import time
import logging
from typing import Dict, List, Optional
from pathlib import Path
import threading

logger = logging.getLogger(__name__)


class FileRegistry:
    """
    Registry for tracking uploaded DuckDB files.
    
    This class maintains a registry of uploaded files with their metadata
    so that analytical queries can reference them by name or path.
    """
    
    def __init__(self, max_files: int = 50, file_timeout: int = 7200):  # 2 hours default
        """
        Initialize file registry.
        
        Args:
            max_files: Maximum number of files to track
            file_timeout: File timeout in seconds (default: 2 hours)
        """
        self.files: Dict[str, Dict] = {}
        self.max_files = max_files
        self.file_timeout = file_timeout
        self._lock = threading.Lock()
        
        logger.info(f"Initialized file registry (max_files={max_files}, timeout={file_timeout}s)")
    
    def register_file(self, filename: str, db_path: str, metadata: Dict) -> str:
        """
        Register an uploaded file.
        
        Args:
            filename: Original filename
            db_path: Full path to the database file
            metadata: File metadata (columns, tables, etc.)
            
        Returns:
            File ID for referencing the file
        """
        with self._lock:
            # Clean up expired files
            self._cleanup_expired_files()
            
            # Check file limit
            if len(self.files) >= self.max_files:
                # Remove oldest file
                oldest_id = min(self.files.keys(), key=lambda x: self.files[x]['registered_at'])
                self._remove_file(oldest_id)
                logger.info(f"Removed oldest file {oldest_id} due to limit")
            
            # Generate file ID
            file_id = f"{int(time.time())}_{filename}"
            
            # Register file
            self.files[file_id] = {
                'filename': filename,
                'db_path': db_path,
                'metadata': metadata,
                'registered_at': time.time(),
                'last_accessed': time.time()
            }
            
            logger.info(f"Registered file {file_id}: {filename} -> {db_path}")
            return file_id
    
    def get_file_info(self, file_identifier: str) -> Optional[Dict]:
        """
        Get file information by ID or filename.
        
        Args:
            file_identifier: File ID or filename
            
        Returns:
            File information dictionary or None if not found
        """
        with self._lock:
            # Try exact match by file ID
            if file_identifier in self.files:
                file_info = self.files[file_identifier]
                file_info['last_accessed'] = time.time()
                return file_info
            
            # Try match by filename
            for file_id, file_info in self.files.items():
                if file_info['filename'].lower() == file_identifier.lower():
                    file_info['last_accessed'] = time.time()
                    return file_info
            
            return None
    
    def list_files(self) -> List[Dict]:
        """
        List all registered files.
        
        Returns:
            List of file information dictionaries
        """
        with self._lock:
            self._cleanup_expired_files()
            
            files_list = []
            for file_id, file_info in self.files.items():
                files_list.append({
                    'file_id': file_id,
                    'filename': file_info['filename'],
                    'db_path': file_info['db_path'],
                    'tables': file_info['metadata'].get('all_tables', []),
                    'columns': file_info['metadata'].get('columns', []),
                    'file_size': file_info['metadata'].get('file_size', 0),
                    'registered_at': file_info['registered_at'],
                    'last_accessed': file_info['last_accessed']
                })
            
            # Sort by registration time (newest first)
            files_list.sort(key=lambda x: x['registered_at'], reverse=True)
            return files_list
    
    def remove_file(self, file_identifier: str) -> bool:
        """
        Remove a file from the registry.
        
        Args:
            file_identifier: File ID or filename
            
        Returns:
            True if file was removed, False if not found
        """
        with self._lock:
            # Try exact match by file ID
            if file_identifier in self.files:
                return self._remove_file(file_identifier)
            
            # Try match by filename
            for file_id, file_info in self.files.items():
                if file_info['filename'].lower() == file_identifier.lower():
                    return self._remove_file(file_id)
            
            return False
    
    def _remove_file(self, file_id: str) -> bool:
        """
        Internal method to remove a file (assumes lock is held).
        """
        file_info = self.files.pop(file_id, None)
        if file_info:
            # Clean up the actual file
            try:
                if os.path.exists(file_info['db_path']):
                    os.unlink(file_info['db_path'])
                    logger.info(f"Cleaned up file: {file_info['db_path']}")
            except Exception as e:
                logger.warning(f"Failed to cleanup file {file_info['db_path']}: {e}")
            
            logger.info(f"Removed file {file_id} from registry")
            return True
        return False
    
    def _cleanup_expired_files(self):
        """
        Clean up expired files (assumes lock is held).
        """
        current_time = time.time()
        expired_files = [
            file_id for file_id, file_info in self.files.items()
            if current_time - file_info['last_accessed'] > self.file_timeout
        ]
        
        for file_id in expired_files:
            self._remove_file(file_id)
        
        if expired_files:
            logger.info(f"Cleaned up {len(expired_files)} expired files")
    
    def cleanup_all(self):
        """
        Clean up all files.
        """
        with self._lock:
            file_ids = list(self.files.keys())
            for file_id in file_ids:
                self._remove_file(file_id)
            
            logger.info("Cleaned up all files from registry")


# Global file registry instance
file_registry = FileRegistry()