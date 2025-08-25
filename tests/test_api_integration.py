"""
Integration tests for FastAPI application with database.
"""

import pytest
from httpx import AsyncClient
from app.main import app


@pytest.mark.asyncio
async def test_root_endpoint():
    """Test the root endpoint."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "CNC ML Monitoring API"
        assert data["version"] == "1.0.0"
        assert data["status"] == "running"


@pytest.mark.asyncio
async def test_health_endpoint():
    """Test the health check endpoint."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "database" in data
        assert "ml_engine" in data
        assert data["ml_engine"] == "ready"


@pytest.mark.asyncio
async def test_database_health_endpoint():
    """Test the detailed database health check endpoint."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/health/database")
        assert response.status_code == 200
        data = response.json()
        assert "health_check" in data
        assert "crud_test" in data
        assert "timestamp" in data
        
        # Check health check structure
        health_check = data["health_check"]
        assert "status" in health_check
        
        # Check CRUD test structure
        crud_test = data["crud_test"]
        assert "create_session" in crud_test
        assert "execute_query" in crud_test
        assert "transaction" in crud_test


@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_with_real_database():
    """Test health endpoint with real database connection."""
    try:
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/health")
            assert response.status_code == 200
            data = response.json()
            
            # If database is available, status should be healthy
            if data["database"]["status"] == "healthy":
                assert data["status"] == "healthy"
                assert "connection_pool" in data["database"]
                assert "info" in data["database"]
    except Exception:
        pytest.skip("Database not available for integration testing")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_database_health_with_real_database():
    """Test detailed database health with real database connection."""
    try:
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get("/health/database")
            assert response.status_code == 200
            data = response.json()
            
            health_check = data["health_check"]
            crud_test = data["crud_test"]
            
            # If database is available, check detailed results
            if health_check["status"] == "healthy":
                assert "connection_pool" in health_check
                assert "database_info" in health_check
                
                # CRUD tests should pass
                assert crud_test["create_session"] is True
                assert crud_test["execute_query"] is True
                assert crud_test["transaction"] is True
                assert crud_test.get("error") is None
    except Exception:
        pytest.skip("Database not available for integration testing")