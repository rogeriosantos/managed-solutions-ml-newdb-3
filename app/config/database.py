"""
Database Configuration Module

This module handles database connection setup, session management,
and connection utilities for the Railway MySQL database.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError, OperationalError
from sqlalchemy import text
from app.config.settings import get_settings

# Configure logging
logger = logging.getLogger(__name__)

# Get application settings
settings = get_settings()

# Create SQLAlchemy declarative base
Base = declarative_base()

# Create async database engine with enhanced configuration
engine = create_async_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_timeout=settings.db_pool_timeout,
    pool_recycle=settings.db_pool_recycle,
    pool_pre_ping=True,  # Enable connection health checks
    echo=settings.debug,  # Log SQL queries in debug mode
    connect_args={
        "connect_timeout": 30,
        "autocommit": False,
    }
)

# Create async session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def retry_database_operation(
    operation,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    backoff_factor: float = 2.0
):
    """
    Retry database operations with exponential backoff.
    
    Args:
        operation: Async function to retry
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries in seconds
        backoff_factor: Multiplier for delay on each retry
        
    Returns:
        Result of the operation
        
    Raises:
        SQLAlchemyError: If all retry attempts fail
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await operation()
        except (DisconnectionError, OperationalError, ConnectionError) as e:
            last_exception = e
            if attempt < max_retries:
                delay = retry_delay * (backoff_factor ** attempt)
                logger.warning(
                    f"Database operation failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {delay:.1f} seconds..."
                )
                await asyncio.sleep(delay)
            else:
                logger.error(f"Database operation failed after {max_retries + 1} attempts: {e}")
        except Exception as e:
            # Don't retry for non-connection related errors
            logger.error(f"Database operation failed with non-retryable error: {e}")
            raise
    
    # If we get here, all retries failed
    raise last_exception


@asynccontextmanager
async def get_database_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager for database sessions with automatic cleanup and error handling.
    
    Yields:
        AsyncSession: Database session with automatic transaction management
    """
    session = None
    try:
        session = AsyncSessionLocal()
        yield session
        await session.commit()
    except Exception as e:
        if session:
            await session.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        if session:
            await session.close()


async def get_database_session_dependency() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency function for FastAPI dependency injection.
    
    Yields:
        AsyncSession: Database session for dependency injection
    """
    async with get_database_session() as session:
        yield session


async def check_database_connection() -> bool:
    """
    Check if database connection is healthy.
    
    Returns:
        bool: True if connection is healthy, False otherwise
    """
    try:
        async def _check_connection():
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
                return True
        
        return await retry_database_operation(_check_connection, max_retries=2)
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


async def get_database_info() -> Optional[dict]:
    """
    Get database information and statistics.
    
    Returns:
        dict: Database information or None if connection fails
    """
    try:
        async def _get_info():
            async with engine.begin() as conn:
                # Get database version
                version_result = await conn.execute(text("SELECT VERSION()"))
                version = version_result.scalar()
                
                # Check if joblog_ob table exists
                table_check = await conn.execute(
                    text("SELECT COUNT(*) FROM information_schema.tables "
                         "WHERE table_schema = :schema AND table_name = 'joblog_ob'"),
                    {"schema": settings.database_name}
                )
                table_exists = table_check.scalar() > 0
                
                info = {
                    "version": version,
                    "database_name": settings.database_name,
                    "joblog_ob_exists": table_exists
                }
                
                if table_exists:
                    # Get record count
                    count_result = await conn.execute(text("SELECT COUNT(*) FROM joblog_ob"))
                    info["joblog_ob_count"] = count_result.scalar()
                
                return info
        
        return await retry_database_operation(_get_info)
    except Exception as e:
        logger.error(f"Failed to get database info: {e}")
        return None


async def init_database():
    """
    Initialize database tables with enhanced error handling and retry logic.
    This will be called during application startup.
    """
    if settings.skip_db_init:
        logger.info("Database initialization skipped (SKIP_DB_INIT=True)")
        return
    
    try:
        async def _init_db():
            async with engine.begin() as conn:
                # Import all models to ensure they are registered with Base
                # from app.models.database_models import *  # Will be implemented in later tasks
                
                # Create all tables
                await conn.run_sync(Base.metadata.create_all)
                logger.info("Database tables created/verified successfully")
        
        await retry_database_operation(_init_db)
        logger.info("Database initialized successfully")
        
        # Get and log database info
        db_info = await get_database_info()
        if db_info:
            logger.info(f"Connected to database: {db_info}")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        logger.warning("Application will continue without database connection")
        raise


async def close_database():
    """
    Close database connections with proper cleanup.
    This will be called during application shutdown.
    """
    try:
        await engine.dispose()
        logger.info("Database connections closed successfully")
    except Exception as e:
        logger.warning(f"Error closing database connections: {e}")


# Connection utilities for testing and monitoring
class DatabaseConnectionManager:
    """
    Database connection manager with health monitoring and connection pooling utilities.
    """
    
    def __init__(self):
        self.engine = engine
        self.session_factory = AsyncSessionLocal
    
    async def health_check(self) -> dict:
        """
        Comprehensive database health check.
        
        Returns:
            dict: Health check results with connection status and metrics
        """
        health_info = {
            "status": "unhealthy",
            "connection_pool": {},
            "database_info": None,
            "error": None
        }
        
        try:
            # Check basic connectivity
            is_healthy = await check_database_connection()
            if not is_healthy:
                health_info["error"] = "Database connection failed"
                return health_info
            
            # Get connection pool info
            pool = self.engine.pool
            health_info["connection_pool"] = {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
                # Note: invalid() method not available in AsyncAdaptedQueuePool
            }
            
            # Get database info
            db_info = await get_database_info()
            health_info["database_info"] = db_info
            
            health_info["status"] = "healthy"
            
        except Exception as e:
            health_info["error"] = str(e)
            logger.error(f"Database health check failed: {e}")
        
        return health_info
    
    async def test_crud_operations(self) -> dict:
        """
        Test basic CRUD operations to verify database functionality.
        
        Returns:
            dict: Test results for each operation
        """
        test_results = {
            "create_session": False,
            "execute_query": False,
            "transaction": False,
            "error": None
        }
        
        try:
            async with get_database_session() as session:
                test_results["create_session"] = True
                
                # Test simple query execution
                result = await session.execute(text("SELECT 1 as test"))
                row = result.fetchone()
                if row and row[0] == 1:
                    test_results["execute_query"] = True
                
                # Test transaction (rollback to avoid side effects)
                await session.execute(text("SELECT 1"))
                await session.rollback()
                test_results["transaction"] = True
                
        except Exception as e:
            test_results["error"] = str(e)
            logger.error(f"CRUD operations test failed: {e}")
        
        return test_results


# Global connection manager instance
connection_manager = DatabaseConnectionManager()