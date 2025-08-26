"""
Basic tests for machine API endpoints.
"""

import pytest
from datetime import datetime, timedelta
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch

from app.main import app
from app.config.database import get_database_session_dependency
from app.models.database_models import Machine


@pytest.mark.asyncio
async def test_list_machines_empty():
    """Test listing machines when database is empty."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        with patch('app.services.machine_service.MachineService.get_all_machines') as mock_get_all:
            mock_get_all.return_value = []
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/machines")
                
                assert response.status_code == 200
                data = response.json()
                assert isinstance(data, list)
                assert len(data) == 0
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_create_machine_success():
    """Test successful machine creation."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        machine_data = {
            "machine_id": "TEST_001",
            "machine_name": "Test Machine",
            "machine_type": "CNC_MILL",
            "status": "ACTIVE"
        }
        
        sample_machine = Machine(
            machine_id="TEST_001",
            machine_name="Test Machine",
            machine_type="CNC_MILL",
            status="ACTIVE",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        with patch('app.services.machine_service.MachineService.create_machine') as mock_create:
            mock_create.return_value = sample_machine
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post("/api/v1/machines", json=machine_data)
                
                assert response.status_code == 201
                data = response.json()
                assert data["machine_id"] == machine_data["machine_id"]
                assert data["machine_name"] == machine_data["machine_name"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_success():
    """Test successful machine retrieval."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        sample_machine = Machine(
            machine_id="TEST_001",
            machine_name="Test Machine",
            machine_type="CNC_MILL",
            status="ACTIVE",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        with patch('app.services.machine_service.MachineService.get_machine_by_id') as mock_get:
            mock_get.return_value = sample_machine
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/machines/TEST_001")
                
                assert response.status_code == 200
                data = response.json()
                assert data["machine_id"] == sample_machine.machine_id
                assert data["machine_name"] == sample_machine.machine_name
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_not_found():
    """Test retrieving non-existent machine."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        with patch('app.services.machine_service.MachineService.get_machine_by_id') as mock_get:
            mock_get.return_value = None
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/machines/NONEXISTENT")
                
                assert response.status_code == 404
                assert "not found" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_create_machine_validation_error():
    """Test creating machine with validation error."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        machine_data = {
            "machine_name": "Test Machine"
            # Missing required machine_id and machine_type
        }
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.post("/api/v1/machines", json=machine_data)
            
            assert response.status_code == 422  # Validation error
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_update_machine_success():
    """Test successful machine update."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        update_data = {
            "machine_name": "Updated Machine Name",
            "status": "MAINTENANCE"
        }
        
        updated_machine = Machine(
            machine_id="TEST_001",
            machine_name="Updated Machine Name",
            machine_type="CNC_MILL",
            status="MAINTENANCE",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        with patch('app.services.machine_service.MachineService.update_machine') as mock_update:
            mock_update.return_value = updated_machine
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.put("/api/v1/machines/TEST_001", json=update_data)
                
                assert response.status_code == 200
                data = response.json()
                assert data["machine_name"] == update_data["machine_name"]
                assert data["status"] == update_data["status"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_delete_machine_success():
    """Test successful machine deletion."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        with patch('app.services.machine_service.MachineService.delete_machine') as mock_delete:
            mock_delete.return_value = True
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.delete("/api/v1/machines/TEST_001")
                
                assert response.status_code == 204
    finally:
        app.dependency_overrides.clear()