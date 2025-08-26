"""
Integration tests for machine API endpoints.

This module contains comprehensive tests for machine CRUD operations
and data retrieval endpoints with various scenarios and edge cases.
"""

import pytest
from datetime import datetime, timedelta
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from app.main import app
from app.config.database import get_database_session_dependency
from app.models.database_models import Machine, JobLogOB


class TestMachineAPI:
    """Test class for machine API endpoints."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Create mock database session."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def test_machine_data(self):
        """Create test machine data."""
        machine = Machine(
            machine_id="TEST_MACHINE_001",
            machine_name="Test CNC Machine",
            machine_type="CNC_MILL",
            manufacturer="Test Manufacturer",
            model="TM-100",
            year_installed=2020,
            max_spindle_speed=8000,
            max_feed_rate=1000.0,
            work_envelope_x=500.0,
            work_envelope_y=400.0,
            work_envelope_z=300.0,
            maintenance_schedule_hours=200,
            status="ACTIVE",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Create test job logs
        base_time = datetime.utcnow() - timedelta(days=7)
        job_logs = []
        
        for i in range(5):
            job_log = JobLogOB(
                id=i+1,
                machine="TEST_MACHINE_001",
                start_time=base_time + timedelta(hours=i*2),
                end_time=base_time + timedelta(hours=i*2+1),
                job_number=f"JOB_{i+1:03d}",
                state="COMPLETED",
                part_number=f"PART_{i+1:03d}",
                emp_id=f"EMP_{i+1:03d}",
                operator_name=f"Operator {i+1}",
                op_number=10,
                parts_produced=10 + i,
                job_duration=3600,  # 1 hour
                running_time=3000,  # 50 minutes
                setup_time=300,     # 5 minutes
                maintenance_time=180 if i == 2 else 0,  # Maintenance on 3rd job
                idle_time=120       # 2 minutes
            )
            job_logs.append(job_log)
        
        return {
            'machine': machine,
            'job_logs': job_logs
        }
    
    @pytest.fixture
    async def client(self, mock_db_session):
        """Create test client with mocked database."""
        app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
        async with AsyncClient(app=app, base_url="http://test") as ac:
            yield ac
        app.dependency_overrides.clear()
    
    async def test_list_machines_empty(self, client: AsyncClient, mock_db_session):
        """Test listing machines when database is empty."""
        with patch('app.services.machine_service.MachineService.get_all_machines') as mock_get_all:
            mock_get_all.return_value = []
            
            response = await client.get("/api/v1/machines")
            
            assert response.status_code == 200
            data = response.json()
            assert isinstance(data, list)
            assert len(data) == 0
    
    async def test_create_machine_success(self, client: AsyncClient, db_session: AsyncSession):
        """Test successful machine creation."""
        machine_data = {
            "machine_id": "TEST_001",
            "machine_name": "Test Machine",
            "machine_type": "CNC_LATHE",
            "manufacturer": "Test Corp",
            "model": "TC-200",
            "year_installed": 2021,
            "max_spindle_speed": 6000,
            "max_feed_rate": 800.0,
            "work_envelope_x": 400.0,
            "work_envelope_y": 300.0,
            "work_envelope_z": 250.0,
            "maintenance_schedule_hours": 150,
            "status": "ACTIVE"
        }
        
        response = await client.post("/api/v1/machines", json=machine_data)
        
        assert response.status_code == 201
        data = response.json()
        
        assert data["machine_id"] == machine_data["machine_id"]
        assert data["machine_name"] == machine_data["machine_name"]
        assert data["machine_type"] == machine_data["machine_type"]
        assert data["status"] == machine_data["status"]
        assert "created_at" in data
        assert "updated_at" in data
    
    async def test_create_machine_duplicate_id(self, client: AsyncClient, test_machine_data):
        """Test creating machine with duplicate ID."""
        machine_data = {
            "machine_id": "TEST_MACHINE_001",  # Same as test data
            "machine_name": "Duplicate Machine",
            "machine_type": "CNC_MILL",
            "status": "ACTIVE"
        }
        
        response = await client.post("/api/v1/machines", json=machine_data)
        
        assert response.status_code == 400
        assert "already exists" in response.json()["detail"]
    
    async def test_create_machine_missing_required_fields(self, client: AsyncClient):
        """Test creating machine with missing required fields."""
        machine_data = {
            "machine_name": "Incomplete Machine"
            # Missing machine_id and machine_type
        }
        
        response = await client.post("/api/v1/machines", json=machine_data)
        
        assert response.status_code == 422  # Validation error
    
    async def test_get_machine_success(self, client: AsyncClient, test_machine_data):
        """Test successful machine retrieval."""
        machine = test_machine_data['machine']
        
        response = await client.get(f"/api/v1/machines/{machine.machine_id}")
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["machine_id"] == machine.machine_id
        assert data["machine_name"] == machine.machine_name
        assert data["machine_type"] == machine.machine_type
        assert data["status"] == machine.status
    
    async def test_get_machine_not_found(self, client: AsyncClient):
        """Test retrieving non-existent machine."""
        response = await client.get("/api/v1/machines/NONEXISTENT")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    async def test_list_machines_with_data(self, client: AsyncClient, test_machine_data):
        """Test listing machines with data."""
        response = await client.get("/api/v1/machines")
        
        assert response.status_code == 200
        data = response.json()
        
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["machine_id"] == test_machine_data['machine'].machine_id
    
    async def test_list_machines_filter_by_type(self, client: AsyncClient, test_machine_data):
        """Test listing machines filtered by type."""
        # Test with matching type
        response = await client.get("/api/v1/machines?machine_type=CNC_MILL")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        
        # Test with non-matching type
        response = await client.get("/api/v1/machines?machine_type=CNC_LATHE")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 0
    
    async def test_update_machine_success(self, client: AsyncClient, test_machine_data):
        """Test successful machine update."""
        machine = test_machine_data['machine']
        
        update_data = {
            "machine_name": "Updated Machine Name",
            "max_spindle_speed": 9000,
            "status": "MAINTENANCE"
        }
        
        response = await client.put(f"/api/v1/machines/{machine.machine_id}", json=update_data)
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["machine_name"] == update_data["machine_name"]
        assert data["max_spindle_speed"] == update_data["max_spindle_speed"]
        assert data["status"] == update_data["status"]
        assert data["machine_id"] == machine.machine_id  # Should not change
    
    async def test_update_machine_not_found(self, client: AsyncClient):
        """Test updating non-existent machine."""
        update_data = {"machine_name": "Updated Name"}
        
        response = await client.put("/api/v1/machines/NONEXISTENT", json=update_data)
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    async def test_update_machine_empty_data(self, client: AsyncClient, test_machine_data):
        """Test updating machine with empty data."""
        machine = test_machine_data['machine']
        
        response = await client.put(f"/api/v1/machines/{machine.machine_id}", json={})
        
        assert response.status_code == 400
        assert "No valid fields" in response.json()["detail"]
    
    async def test_delete_machine_success(self, client: AsyncClient, test_machine_data):
        """Test successful machine deletion (soft delete)."""
        machine = test_machine_data['machine']
        
        response = await client.delete(f"/api/v1/machines/{machine.machine_id}")
        
        assert response.status_code == 204
        
        # Verify machine is soft deleted (status changed to RETIRED)
        response = await client.get(f"/api/v1/machines/{machine.machine_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "RETIRED"
    
    async def test_delete_machine_not_found(self, client: AsyncClient):
        """Test deleting non-existent machine."""
        response = await client.delete("/api/v1/machines/NONEXISTENT")
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    async def test_get_machine_data_success(self, client: AsyncClient, test_machine_data):
        """Test successful machine data retrieval."""
        machine = test_machine_data['machine']
        start_date = datetime.utcnow() - timedelta(days=8)
        end_date = datetime.utcnow()
        
        response = await client.get(
            f"/api/v1/machines/{machine.machine_id}/data"
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
        
        assert data["total_count"] == 5  # 5 job logs created in test data
        assert len(data["data"]) == 5
        
        # Verify job log structure
        job_log = data["data"][0]
        assert "machine" in job_log
        assert "start_time" in job_log
        assert "total_downtime" in job_log
        assert "downtime_breakdown" in job_log
        assert "efficiency" in job_log
    
    async def test_get_machine_data_pagination(self, client: AsyncClient, test_machine_data):
        """Test machine data retrieval with pagination."""
        machine = test_machine_data['machine']
        start_date = datetime.utcnow() - timedelta(days=8)
        end_date = datetime.utcnow()
        
        response = await client.get(
            f"/api/v1/machines/{machine.machine_id}/data"
            f"?start_date={start_date.isoformat()}"
            f"&end_date={end_date.isoformat()}"
            f"&page=1&page_size=2"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["page"] == 1
        assert data["page_size"] == 2
        assert data["total_count"] == 5
        assert data["total_pages"] == 3
        assert len(data["data"]) == 2
    
    async def test_get_machine_data_invalid_date_range(self, client: AsyncClient, test_machine_data):
        """Test machine data retrieval with invalid date range."""
        machine = test_machine_data['machine']
        start_date = datetime.utcnow()
        end_date = datetime.utcnow() - timedelta(days=1)  # End before start
        
        response = await client.get(
            f"/api/v1/machines/{machine.machine_id}/data"
            f"?start_date={start_date.isoformat()}"
            f"&end_date={end_date.isoformat()}"
        )
        
        assert response.status_code == 400
        assert "Start date must be before end date" in response.json()["detail"]
    
    async def test_get_machine_data_not_found(self, client: AsyncClient):
        """Test machine data retrieval for non-existent machine."""
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = datetime.utcnow()
        
        response = await client.get(
            f"/api/v1/machines/NONEXISTENT/data"
            f"?start_date={start_date.isoformat()}"
            f"&end_date={end_date.isoformat()}"
        )
        
        assert response.status_code == 400
        assert "not found" in response.json()["detail"]
    
    async def test_get_machine_downtime_analysis(self, client: AsyncClient, test_machine_data):
        """Test machine downtime analysis."""
        machine = test_machine_data['machine']
        start_date = datetime.utcnow() - timedelta(days=8)
        end_date = datetime.utcnow()
        
        response = await client.get(
            f"/api/v1/machines/{machine.machine_id}/downtime"
            f"?start_date={start_date.isoformat()}"
            f"&end_date={end_date.isoformat()}"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["machine_id"] == machine.machine_id
        assert "analysis_period" in data
        assert "total_downtime" in data
        assert "downtime_by_category" in data
        assert "recommendations" in data
        
        # Verify analysis period
        assert "start_date" in data["analysis_period"]
        assert "end_date" in data["analysis_period"]
    
    async def test_get_machine_oee(self, client: AsyncClient, test_machine_data):
        """Test machine OEE calculation."""
        machine = test_machine_data['machine']
        start_date = datetime.utcnow() - timedelta(days=8)
        end_date = datetime.utcnow()
        
        response = await client.get(
            f"/api/v1/machines/{machine.machine_id}/oee"
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
    
    async def test_get_machine_oee_without_dates(self, client: AsyncClient, test_machine_data):
        """Test machine OEE calculation without date parameters."""
        machine = test_machine_data['machine']
        
        response = await client.get(f"/api/v1/machines/{machine.machine_id}/oee")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "availability" in data
        assert "performance" in data
        assert "quality" in data
        assert "oee" in data


@pytest.mark.asyncio
class TestMachineAPIIntegration:
    """Integration tests for machine API with database operations."""
    
    async def test_full_machine_lifecycle(self, db_session: AsyncSession):
        """Test complete machine lifecycle through API."""
        async with AsyncClient(app=app, base_url="http://test") as client:
            # 1. Create machine
            machine_data = {
                "machine_id": "LIFECYCLE_001",
                "machine_name": "Lifecycle Test Machine",
                "machine_type": "CNC_MILL",
                "status": "ACTIVE"
            }
            
            response = await client.post("/api/v1/machines", json=machine_data)
            assert response.status_code == 201
            created_machine = response.json()
            
            # 2. Retrieve machine
            response = await client.get(f"/api/v1/machines/{machine_data['machine_id']}")
            assert response.status_code == 200
            retrieved_machine = response.json()
            assert retrieved_machine["machine_id"] == created_machine["machine_id"]
            
            # 3. Update machine
            update_data = {"machine_name": "Updated Lifecycle Machine"}
            response = await client.put(
                f"/api/v1/machines/{machine_data['machine_id']}", 
                json=update_data
            )
            assert response.status_code == 200
            updated_machine = response.json()
            assert updated_machine["machine_name"] == update_data["machine_name"]
            
            # 4. List machines (should include our machine)
            response = await client.get("/api/v1/machines")
            assert response.status_code == 200
            machines = response.json()
            machine_ids = [m["machine_id"] for m in machines]
            assert machine_data["machine_id"] in machine_ids
            
            # 5. Delete machine
            response = await client.delete(f"/api/v1/machines/{machine_data['machine_id']}")
            assert response.status_code == 204
            
            # 6. Verify soft delete (machine still exists but status is RETIRED)
            response = await client.get(f"/api/v1/machines/{machine_data['machine_id']}")
            assert response.status_code == 200
            deleted_machine = response.json()
            assert deleted_machine["status"] == "RETIRED"
    
    async def test_machine_data_endpoints_integration(self, db_session: AsyncSession):
        """Test integration between machine creation and data endpoints."""
        async with AsyncClient(app=app, base_url="http://test") as client:
            # Create machine
            machine_data = {
                "machine_id": "DATA_TEST_001",
                "machine_name": "Data Test Machine",
                "machine_type": "CNC_LATHE",
                "status": "ACTIVE"
            }
            
            response = await client.post("/api/v1/machines", json=machine_data)
            assert response.status_code == 201
            
            # Test data endpoints (should work even with no job logs)
            start_date = datetime.utcnow() - timedelta(days=1)
            end_date = datetime.utcnow()
            
            # Test machine data endpoint
            response = await client.get(
                f"/api/v1/machines/{machine_data['machine_id']}/data"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            assert response.status_code == 200
            data = response.json()
            assert data["total_count"] == 0
            assert len(data["data"]) == 0
            
            # Test downtime analysis endpoint
            response = await client.get(
                f"/api/v1/machines/{machine_data['machine_id']}/downtime"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            assert response.status_code == 200
            
            # Test OEE endpoint
            response = await client.get(f"/api/v1/machines/{machine_data['machine_id']}/oee")
            assert response.status_code == 200