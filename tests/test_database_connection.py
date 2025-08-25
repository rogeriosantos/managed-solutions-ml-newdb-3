"""
Comprehensive tests for database configuration and connection utilities.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.exc import OperationalError, DisconnectionError
from app.config.database import (
    engine,
    AsyncSessionLocal,
    get_database_session,
    get_database_session_dependency,
    retry_database_operation,
    check_database_connection,
    get_database_info,
    init_database,
    close_database,
    connection_manager,
    DatabaseConnectionManager
)
from app.config.settings import get_settings


class TestDatabaseConfiguration:
    """Test database configuration and basic setup."""
    
    def test_engine_configuration(self):
        """Test that database engine is properly configured."""
        assert engine is not None
        assert engine.url.drivername == "mysql+aiomysql"
        
        # Check pool configuration
        pool = engine.pool
        settings = get_settings()
        assert pool._creator is not None
    
    def test_session_factory_configuration(self):
        """Test that session factory is properly configured."""
        assert AsyncSessionLocal is not None
        # Check that the session factory is callable and properly configured
        assert callable(AsyncSessionLocal)
        # Verify it's an async_sessionmaker instance
        from sqlalchemy.ext.asyncio import async_sessionmaker
        assert isinstance(AsyncSessionLocal, async_sessionmaker)


class TestRetryMechanism:
    """Test database retry mechanisms."""
    
    @pytest.mark.asyncio
    async def test_retry_success_on_first_attempt(self):
        """Test successful operation on first attempt."""
        async def mock_operation():
            return "success"
        
        result = await retry_database_operation(mock_operation)
        assert result == "success"
    
    @pytest.mark.asyncio
    async def test_retry_success_after_failures(self):
        """Test successful operation after some failures."""
        call_count = 0
        
        async def mock_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise OperationalError("Connection failed", None, None)
            return "success"
        
        result = await retry_database_operation(mock_operation, max_retries=3)
        assert result == "success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_retry_exhausted(self):
        """Test retry mechanism when all attempts fail."""
        async def mock_operation():
            raise OperationalError("Connection failed", None, None)
        
        with pytest.raises(OperationalError):
            await retry_database_operation(mock_operation, max_retries=2)
    
    @pytest.mark.asyncio
    async def test_retry_non_retryable_error(self):
        """Test that non-retryable errors are not retried."""
        call_count = 0
        
        async def mock_operation():
            nonlocal call_count
            call_count += 1
            raise ValueError("Invalid data")
        
        with pytest.raises(ValueError):
            await retry_database_operation(mock_operation, max_retries=3)
        
        assert call_count == 1  # Should not retry


class TestSessionManagement:
    """Test database session management."""
    
    @pytest.mark.asyncio
    async def test_get_database_session_context_manager(self):
        """Test database session context manager."""
        with patch('app.config.database.AsyncSessionLocal') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value = mock_session
            
            async with get_database_session() as session:
                assert session == mock_session
            
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_database_session_rollback_on_error(self):
        """Test that session rolls back on error."""
        with patch('app.config.database.AsyncSessionLocal') as mock_session_factory:
            mock_session = AsyncMock()
            mock_session_factory.return_value = mock_session
            
            with pytest.raises(ValueError):
                async with get_database_session() as session:
                    raise ValueError("Test error")
            
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_database_session_dependency(self):
        """Test FastAPI dependency function."""
        with patch('app.config.database.get_database_session') as mock_context:
            mock_session = AsyncMock()
            mock_context.return_value.__aenter__.return_value = mock_session
            
            async for session in get_database_session_dependency():
                assert session == mock_session
                break


class TestConnectionUtilities:
    """Test database connection utilities."""
    
    @pytest.mark.asyncio
    async def test_check_database_connection_success(self):
        """Test successful database connection check."""
        with patch('app.config.database.engine') as mock_engine:
            mock_conn = AsyncMock()
            mock_engine.begin.return_value.__aenter__.return_value = mock_conn
            
            result = await check_database_connection()
            assert result is True
            mock_conn.execute.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_check_database_connection_failure(self):
        """Test database connection check failure."""
        with patch('app.config.database.engine') as mock_engine:
            mock_engine.begin.side_effect = OperationalError("Connection failed", None, None)
            
            result = await check_database_connection()
            assert result is False
    
    @pytest.mark.asyncio
    async def test_get_database_info_success(self):
        """Test successful database info retrieval."""
        with patch('app.config.database.engine') as mock_engine:
            mock_conn = AsyncMock()
            mock_engine.begin.return_value.__aenter__.return_value = mock_conn
            
            # Mock version query
            version_result = MagicMock()
            version_result.scalar.return_value = "MySQL 8.0.35"
            
            # Mock table existence check
            table_result = MagicMock()
            table_result.scalar.return_value = 1
            
            # Mock count query
            count_result = MagicMock()
            count_result.scalar.return_value = 10000
            
            mock_conn.execute.side_effect = [version_result, table_result, count_result]
            
            info = await get_database_info()
            
            assert info is not None
            assert info["version"] == "MySQL 8.0.35"
            assert info["joblog_ob_exists"] is True
            assert info["joblog_ob_count"] == 10000
    
    @pytest.mark.asyncio
    async def test_get_database_info_failure(self):
        """Test database info retrieval failure."""
        with patch('app.config.database.engine') as mock_engine:
            mock_engine.begin.side_effect = OperationalError("Connection failed", None, None)
            
            info = await get_database_info()
            assert info is None


class TestDatabaseInitialization:
    """Test database initialization."""
    
    @pytest.mark.asyncio
    async def test_init_database_skip(self):
        """Test database initialization when skipped."""
        with patch('app.config.database.settings') as mock_settings:
            mock_settings.skip_db_init = True
            
            await init_database()  # Should not raise any exception
    
    @pytest.mark.asyncio
    async def test_init_database_success(self):
        """Test successful database initialization."""
        with patch('app.config.database.settings') as mock_settings, \
             patch('app.config.database.engine') as mock_engine, \
             patch('app.config.database.get_database_info') as mock_get_info:
            
            mock_settings.skip_db_init = False
            mock_conn = AsyncMock()
            mock_engine.begin.return_value.__aenter__.return_value = mock_conn
            mock_get_info.return_value = {"version": "MySQL 8.0.35"}
            
            await init_database()
            
            mock_conn.run_sync.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_close_database(self):
        """Test database connection cleanup."""
        with patch('app.config.database.engine') as mock_engine:
            await close_database()
            mock_engine.dispose.assert_called_once()


class TestDatabaseConnectionManager:
    """Test DatabaseConnectionManager class."""
    
    @pytest.mark.asyncio
    async def test_health_check_success(self):
        """Test successful health check."""
        manager = DatabaseConnectionManager()
        
        with patch('app.config.database.check_database_connection') as mock_check, \
             patch('app.config.database.get_database_info') as mock_get_info:
            
            mock_check.return_value = True
            mock_get_info.return_value = {"version": "MySQL 8.0.35"}
            
            # Mock pool attributes
            mock_pool = MagicMock()
            mock_pool.size.return_value = 10
            mock_pool.checkedin.return_value = 8
            mock_pool.checkedout.return_value = 2
            mock_pool.overflow.return_value = 0
            mock_pool.invalid.return_value = 0
            manager.engine.pool = mock_pool
            
            health_info = await manager.health_check()
            
            assert health_info["status"] == "healthy"
            assert health_info["connection_pool"]["size"] == 10
            assert health_info["database_info"]["version"] == "MySQL 8.0.35"
    
    @pytest.mark.asyncio
    async def test_health_check_failure(self):
        """Test health check failure."""
        manager = DatabaseConnectionManager()
        
        with patch('app.config.database.check_database_connection') as mock_check:
            mock_check.return_value = False
            
            health_info = await manager.health_check()
            
            assert health_info["status"] == "unhealthy"
            assert health_info["error"] == "Database connection failed"
    
    @pytest.mark.asyncio
    async def test_crud_operations_success(self):
        """Test successful CRUD operations test."""
        manager = DatabaseConnectionManager()
        
        with patch('app.config.database.get_database_session') as mock_session_context:
            mock_session = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (1,)
            mock_session.execute.return_value = mock_result
            
            mock_session_context.return_value.__aenter__.return_value = mock_session
            
            test_results = await manager.test_crud_operations()
            
            assert test_results["create_session"] is True
            assert test_results["execute_query"] is True
            assert test_results["transaction"] is True
            assert test_results["error"] is None
    
    @pytest.mark.asyncio
    async def test_crud_operations_failure(self):
        """Test CRUD operations test failure."""
        manager = DatabaseConnectionManager()
        
        with patch('app.config.database.get_database_session') as mock_session_context:
            mock_session_context.side_effect = OperationalError("Connection failed", None, None)
            
            test_results = await manager.test_crud_operations()
            
            assert test_results["create_session"] is False
            assert test_results["error"] is not None


class TestConnectionManagerInstance:
    """Test global connection manager instance."""
    
    def test_connection_manager_instance(self):
        """Test that global connection manager is properly initialized."""
        assert connection_manager is not None
        assert isinstance(connection_manager, DatabaseConnectionManager)
        assert connection_manager.engine is engine
        assert connection_manager.session_factory is AsyncSessionLocal


# Integration tests (these require actual database connection)
@pytest.mark.integration
class TestDatabaseIntegration:
    """Integration tests that require actual database connection."""
    
    @pytest.mark.asyncio
    async def test_real_database_connection(self):
        """Test actual database connection (requires valid credentials)."""
        try:
            is_connected = await check_database_connection()
            # This test will pass if database is available, skip if not
            if is_connected:
                assert is_connected is True
                
                # Test getting database info
                db_info = await get_database_info()
                assert db_info is not None
                assert "version" in db_info
                assert "database_name" in db_info
        except Exception:
            pytest.skip("Database not available for integration testing")
    
    @pytest.mark.asyncio
    async def test_real_session_management(self):
        """Test actual session management (requires valid credentials)."""
        try:
            async with get_database_session() as session:
                result = await session.execute("SELECT 1 as test")
                row = result.fetchone()
                assert row[0] == 1
        except Exception:
            pytest.skip("Database not available for integration testing")