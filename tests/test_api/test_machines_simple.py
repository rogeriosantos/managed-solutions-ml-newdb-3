"""
Simple integration tests for machine API endpoints.

This module contains basic tests for machine CRUD operations
to verify the API endpoints are working correctly.
"""

import pytest
from datetime import datetime, timedelta
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.main import app
from app.config.database import get_database_session_dependency
from app.models.database_models import Machine
from app.repositories.base_repository import PaginatedResult


class TestMachineAPIBasic:
    """Basic test class for machine API endpoints."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Create mock database session."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def sample_machine(self):
        """Create sample machine data."""
        return Machine(
            machine_id="TEST_001",
            machine_name="Test Machine",
            machine_type="CNC_MILL",
            manufacturer="Test Corp",
            model="TC-100",
            status="ACTIVE",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
    
    @pytest.fixture
    async def client(self, mock_db_session):
        """Create test client with mocked database."""
        app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
        async with AsyncClient(app=app, base_url="http://test") as ac:
            yield ac
        app.dependency_overrides.clear()
    
    @pytest.mark.asyncio
    async def test_list_machines_empty(self, client):
        """Test listing machines when database is empty."""
        with patch('app.services.machine_service.MachineService.get_all_machines') as mock_get_all:
            mock_get_all.return_value = []
            
            response = await client.get("/api/v1/machines")
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            assert len(data) == 0
    
    @pytest.mark.asyncio
    async def test_list_machines_with_data(self, client, sample_machine):
        """Test listing machines with data."""
        with patch('app.services.machine_service.MachineService.get_all_machines') as mock_get_all:
            mock_get_all.return_value = [sample_machine]
            
            response = await client.get("/api/v1/machines")
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            assert len(data) == 1
            assert data[0]["machine_id"] == sample_machine.machine_id
            assert data[0]["machine_name"] == sample_machine.machine_name
    
    @pytest.mark.asyncio
    async def test_create_machine_success(self, client: AsyncClient, sample_machine):
        """Test successful machine creation."""
        machine_data = {
            "machine_id": "TEST_001",
            "machine_name": "Test Machine",
            "machine_type": "CNC_MILL",
            "manufacturer": "Test Corp",
            "model": "TC-100",
            "status": "ACTIVE"
        }
        
        with patch('app.services.machine_service.MachineService.create_machine') as mock_create:
            mock_create.return_value = sample_machine
            
            response = await client.post("/api/v1/machines", json=machine_data)
            
            assert response.status_code == 201
            data = response.json()
            assert data["machine_id"] == machine_data["machine_id"]
            assert data["machine_name"] == machine_data["machine_name"]
    
    @pytest.mark.asyncio
    async def test_create_machine_validation_error(self, client: AsyncClient):
        """Test creating machine with validation error."""
        machine_data = {
            "machine_name": "Test Machine"
            # Missing required machine_id and machine_type
        }
        
        response = await client.post("/api/v1/machines", json=machine_data)
        
        assert response.status_code == 422  # Validation error
    
    @pytest.mark.asyncio
    async def test_create_machine_duplicate_error(self, client: AsyncClient):
        """Test creating machine with duplicate ID."""
        machine_data = {
            "machine_id": "TEST_001",
            "machine_name": "Test Machine",
            "machine_type": "CNC_MILL",
            "status": "ACTIVE"
        }
        
        with patch('app.services.machine_service.MachineService.create_machine') as mock_create:
            mock_create.side_effect = ValueError("Machine with ID 'TEST_001' already exists")
            
            response = await client.post("/api/v1/machines", json=machine_data)
            
            assert response.status_code == 400
            assert "already exists" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_get_machine_success(self, client: AsyncClient, sample_machine):
        """Test successful machine retrieval."""
        with patch('app.services.machine_service.MachineService.get_machine_by_id') as mock_get:
            mock_get.return_value = sample_machine
            
            response = await client.get(f"/api/v1/machines/{sample_machine.machine_id}")
            
            assert response.status_code == 200
            data = response.json()
            assert data["machine_id"] == sample_machine.machine_id
            assert data["machine_name"] == sample_machine.machine_name
    
    @pytest.mark.asyncio
    async def test_get_machine_not_found(self, client: AsyncClient):
        """Test retrieving non-existent machine."""
        with patch('app.services.machine_service.MachineService.get_machine_by_id') as mock_get:
            mock_get.return_value = None
            
            response = await client.get("/api/v1/machines/NONEXISTENT")
            
            assert response.status_code == 404
            assert "not found" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_update_machine_success(self, client: AsyncClient, sample_machine):
        """Test successful machine update."""
        update_data = {
            "machine_name": "Updated Machine Name",
            "status": "MAINTENANCE"
        }
        
        # Create updated machine
        updated_machine = Machine(**{**sample_machine.__dict__, **update_data})
        
        with patch('app.services.machine_service.MachineService.update_machine') as mock_update:
            mock_update.return_value = updated_machine
            
            response = await client.put(f"/api/v1/machines/{sample_machine.machine_id}", json=update_data)
            
            assert response.status_code == 200
            data = response.json()
            assert data["machine_name"] == update_data["machine_name"]
            assert data["status"] == update_data["status"]
    
    @pytest.mark.asyncio
    async def test_update_machine_not_found(self, client: AsyncClient):
        """Test updating non-existent machine."""
        update_data = {"machine_name": "Updated Name"}
        
        with patch('app.services.machine_service.MachineService.update_machine') as mock_update:
            mock_update.return_value = None
            
            response = await client.put("/api/v1/machines/NONEXISTENT", json=update_data)
            
            assert response.status_code == 404
            assert "not found" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_delete_machine_success(self, client: AsyncClient, sample_machine):
        """Test successful machine deletion."""
        with patch('app.services.machine_service.MachineService.delete_machine') as mock_delete:
            mock_delete.return_value = True
            
            response = await client.delete(f"/api/v1/machines/{sample_machine.machine_id}")
            
            assert response.status_code == 204
    
    @pytest.mark.asyncio
    async def test_delete_machine_not_found(self, client: AsyncClient):
        """Test deleting non-existent machine."""
        with patch('app.services.machine_service.MachineService.delete_machine') as mock_delete:
            mock_delete.return_value = False
            
            response = await client.delete("/api/v1/machines/NONEXISTENT")
            
            assert response.status_code == 404
            assert "not found" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_get_machine_data_success(self, client: AsyncClient, sample_machine):
        """Test successful machine data retrieval."""
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = datetime.utcnow()
        
        # Mock paginated result
        mock_result = PaginatedResult(
            data=[],
            total_count=0,
            page=1,
            page_size=100,
            total_pages=0
        )
        
        with patch('app.services.machine_service.MachineService.get_machine_data') as mock_get_data:
            mock_get_data.return_value = mock_result
            
            response = await client.get(
                f"/api/v1/machines/{sample_machine.machine_id}/data"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            
            assert response.status_code == 200
            data = response.json()
            assert "data" in data
            assert "total_count" in data
            assert "page" in data
            assert "page_size" in data
            assert "total_pages" in data
    
    @pytest.mark.asyncio
    async def test_get_machine_data_invalid_date_range(self, client: AsyncClient, sample_machine):
        """Test machine data retrieval with invalid date range."""
        start_date = datetime.utcnow()
        end_date = datetime.utcnow() - timedelta(days=1)  # End before start
        
        response = await client.get(
            f"/api/v1/machines/{sample_machine.machine_id}/data"
            f"?start_date={start_date.isoformat()}"
            f"&end_date={end_date.isoformat()}"
        )
        
        assert response.status_code == 400
        assert "Start date must be before end date" in response.json()["detail"]
    
    @pytest.mark.asyncio
    async def test_get_machine_downtime_analysis(self, client: AsyncClient, sample_machine):
        """Test machine downtime analysis."""
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = datetime.utcnow()
        
        mock_analysis = {
            'machine_id': sample_machine.machine_id,
            'analysis_period': {
                'start_date': start_date,
                'end_date': end_date
            },
            'downtime_summary': {
                'total_downtime': 1800,
                'downtime_breakdown': {
                    'setup_time': 600,
                    'maintenance_time': 1200
                }
            },
            'downtime_insights': {
                'recommendations': ['Optimize setup procedures']
            }
        }
        
        with patch('app.services.machine_service.MachineService.analyze_machine_downtime') as mock_analyze:
            mock_analyze.return_value = mock_analysis
            
            response = await client.get(
                f"/api/v1/machines/{sample_machine.machine_id}/downtime"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            
            assert response.status_code == 200
            data = response.json()
            assert data["machine_id"] == sample_machine.machine_id
            assert "analysis_period" in data
            assert "total_downtime" in data
            assert "downtime_by_category" in data
            assert "recommendations" in data
    
    @pytest.mark.asyncio
    async def test_get_machine_oee(self, client: AsyncClient, sample_machine):
        """Test machine OEE calculation."""
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = datetime.utcnow()
        
        mock_oee_data = {
            'oee_components': {
                'availability': 0.85,
                'performance': 0.90,
                'quality': 0.95
            },
            'oee_score': 0.726
        }
        
        with patch('app.services.machine_service.MachineService.calculate_machine_oee') as mock_oee:
            mock_oee.return_value = mock_oee_data
            
            response = await client.get(
                f"/api/v1/machines/{sample_machine.machine_id}/oee"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            
            assert response.status_code == 200
            data = response.json()
            assert "availability" in data
            assert "performance" in data
            assert "quality" in data
            assert "oee" in data
            
            # Verify OEE components are between 0 and 1
            assert 0 <= data["availability"] <= 1
            assert 0 <= data["performance"] <= 1
            assert 0 <= data["quality"] <= 1
            assert 0 <= data["oee"] <= 1