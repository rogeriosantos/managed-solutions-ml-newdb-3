"""
API Dependencies Module

This module contains dependency functions for FastAPI endpoints,
including database session management and common utilities.
"""

from typing import AsyncGenerator
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.config.database import get_database_session


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Database session dependency for FastAPI endpoints.
    
    Yields:
        AsyncSession: Database session for use in API endpoints
    """
    async for session in get_database_session():
        yield session


# Additional dependencies will be added in future tasks
# Example: authentication, rate limiting, etc.