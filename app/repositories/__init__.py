"""
Repository Layer Package

This package contains all repository classes for data access operations.
Repositories provide an abstraction layer between the service layer and the database,
implementing the Repository pattern for clean architecture.
"""

from .base_repository import BaseRepository

__all__ = ["BaseRepository"]