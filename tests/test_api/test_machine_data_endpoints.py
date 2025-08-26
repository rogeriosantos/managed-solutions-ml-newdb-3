"""
Tests for machine data retrieval endpoints.
"""

import pytest
from datetime import datetime, timedelta
from httpx import AsyncClient
from unittest.mock import AsyncMock, patch

from app.main import app
from app.config.database import get_database_session_dependency
from app.repositories.base_repository import PaginatedResult
from app.models.database_models import JobLogOB


@pytest.mark.asyncio
async def test_get_machine_data_success():
    """Test successful machine data retrieval with filtering."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = datetime.utcnow()
        
        # Create mock job logs
        mock_job_logs = []
        for i in range(3):
            job_log = JobLogOB(
                id=i+1,
                machine="TEST_001",
                start_time=start_date + timedelta(hours=i),
                end_time=start_date + timedelta(hours=i+1),
                job_number=f"JOB_{i+1:03d}",
                state="COMPLETED",
                part_number=f"PART_{i+1:03d}",
                emp_id=f"EMP_{i+1:03d}",
                operator_name=f"Operator {i+1}",
                op_number=10,
                parts_produced=10 + i,
                job_duration=3600,
                running_time=3000,
                setup_time=300,
                maintenance_time=180 if i == 1 else 0,
                idle_time=120
            )
            mock_job_logs.append(job_log)
        
        # Mock paginated result
        from app.repositories.base_repository import PaginationParams
        pagination_params = PaginationParams(skip=0, limit=100)
        mock_result = PaginatedResult(
            items=mock_job_logs,
            total_count=3,
            pagination=pagination_params
        )
        
        with patch('app.services.machine_service.MachineService.get_machine_data') as mock_get_data:
            mock_get_data.return_value = mock_result
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(
                    f"/api/v1/machines/TEST_001/data"
                    f"?start_date={start_date.isoformat()}"
                    f"&end_date={end_date.isoformat()}"
                )
                
                assert response.status_code == 200
                data = response.json()
                
                # Verify response structure
                assert "data" in data
                assert "total_count" in data
                assert "page" in data
                assert "page_size" in data
                assert "total_pages" in data
                
                assert data["total_count"] == 3
                assert len(data["data"]) == 3
                assert data["page"] == 1
                assert data["page_size"] == 100
                assert data["total_pages"] == 1
                
                # Verify job log structure
                job_log = data["data"][0]
                assert "machine" in job_log
                assert "start_time" in job_log
                assert "total_downtime" in job_log
                assert "downtime_breakdown" in job_log
                assert "efficiency" in job_log
                
                # Verify calculated fields
                assert job_log["total_downtime"] == 420  # setup_time + idle_time (first job has no maintenance_time)
                assert "setup_time" in job_log["downtime_breakdown"]
                assert "idle_time" in job_log["downtime_breakdown"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_data_with_pagination():
    """Test machine data retrieval with pagination parameters."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = datetime.utcnow()
        
        # Mock paginated result with pagination
        from app.repositories.base_repository import PaginationParams
        pagination_params = PaginationParams(skip=10, limit=10)  # Page 2 with page_size 10
        mock_result = PaginatedResult(
            items=[],
            total_count=50,
            pagination=pagination_params
        )
        
        with patch('app.services.machine_service.MachineService.get_machine_data') as mock_get_data:
            mock_get_data.return_value = mock_result
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(
                    f"/api/v1/machines/TEST_001/data"
                    f"?start_date={start_date.isoformat()}"
                    f"&end_date={end_date.isoformat()}"
                    f"&page=2&page_size=10"
                )
                
                assert response.status_code == 200
                data = response.json()
                
                assert data["total_count"] == 50
                assert data["page"] == 2
                assert data["page_size"] == 10
                assert data["total_pages"] == 5
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_data_invalid_date_range():
    """Test machine data retrieval with invalid date range."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow()
        end_date = datetime.utcnow() - timedelta(days=1)  # End before start
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                f"/api/v1/machines/TEST_001/data"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            
            assert response.status_code == 400
            assert "Start date must be before end date" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_data_machine_not_found():
    """Test machine data retrieval for non-existent machine."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = datetime.utcnow()
        
        with patch('app.services.machine_service.MachineService.get_machine_data') as mock_get_data:
            mock_get_data.side_effect = ValueError("Machine NONEXISTENT not found")
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(
                    f"/api/v1/machines/NONEXISTENT/data"
                    f"?start_date={start_date.isoformat()}"
                    f"&end_date={end_date.isoformat()}"
                )
                
                assert response.status_code == 400
                assert "not found" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_downtime_analysis():
    """Test machine downtime analysis endpoint."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow() - timedelta(days=7)
        end_date = datetime.utcnow()
        
        mock_analysis = {
            'machine_id': 'TEST_001',
            'analysis_period': {
                'start_date': start_date,
                'end_date': end_date
            },
            'downtime_summary': {
                'total_downtime': 3600,  # 1 hour
                'downtime_breakdown': {
                    'setup_time': 1800,      # 30 minutes
                    'maintenance_time': 1200, # 20 minutes
                    'idle_time': 600         # 10 minutes
                }
            },
            'downtime_insights': {
                'recommendations': [
                    'Optimize setup procedures to reduce setup time',
                    'Review maintenance schedule for efficiency'
                ]
            }
        }
        
        with patch('app.services.machine_service.MachineService.analyze_machine_downtime') as mock_analyze:
            mock_analyze.return_value = mock_analysis
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(
                    f"/api/v1/machines/TEST_001/downtime"
                    f"?start_date={start_date.isoformat()}"
                    f"&end_date={end_date.isoformat()}"
                )
                
                assert response.status_code == 200
                data = response.json()
                
                # Verify response structure
                assert data["machine_id"] == "TEST_001"
                assert "analysis_period" in data
                assert "total_downtime" in data
                assert "downtime_by_category" in data
                assert "recommendations" in data
                
                # Verify analysis data
                assert data["total_downtime"] == 3600
                assert data["downtime_by_category"]["setup_time"] == 1800
                assert data["downtime_by_category"]["maintenance_time"] == 1200
                assert data["downtime_by_category"]["idle_time"] == 600
                
                # Verify recommendations
                assert len(data["recommendations"]) == 2
                assert "setup procedures" in data["recommendations"][0]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_downtime_analysis_invalid_date_range():
    """Test downtime analysis with invalid date range."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow()
        end_date = datetime.utcnow() - timedelta(days=1)  # End before start
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                f"/api/v1/machines/TEST_001/downtime"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            
            assert response.status_code == 400
            assert "Start date must be before end date" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_oee_with_dates():
    """Test machine OEE calculation with date parameters."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow() - timedelta(days=7)
        end_date = datetime.utcnow()
        
        mock_oee_data = {
            'oee_components': {
                'availability': 0.85,
                'performance': 0.90,
                'quality': 0.95
            },
            'oee_score': 0.726  # 0.85 * 0.90 * 0.95
        }
        
        with patch('app.services.machine_service.MachineService.calculate_machine_oee') as mock_oee:
            mock_oee.return_value = mock_oee_data
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get(
                    f"/api/v1/machines/TEST_001/oee"
                    f"?start_date={start_date.isoformat()}"
                    f"&end_date={end_date.isoformat()}"
                )
                
                assert response.status_code == 200
                data = response.json()
                
                # Verify OEE components
                assert "availability" in data
                assert "performance" in data
                assert "quality" in data
                assert "oee" in data
                
                # Verify values
                assert data["availability"] == 0.85
                assert data["performance"] == 0.90
                assert data["quality"] == 0.95
                assert data["oee"] == 0.726
                
                # Verify all values are between 0 and 1
                assert 0 <= data["availability"] <= 1
                assert 0 <= data["performance"] <= 1
                assert 0 <= data["quality"] <= 1
                assert 0 <= data["oee"] <= 1
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_oee_without_dates():
    """Test machine OEE calculation without date parameters."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        mock_oee_data = {
            'oee_components': {
                'availability': 0.80,
                'performance': 0.85,
                'quality': 0.92
            },
            'oee_score': 0.624  # 0.80 * 0.85 * 0.92
        }
        
        with patch('app.services.machine_service.MachineService.calculate_machine_oee') as mock_oee:
            mock_oee.return_value = mock_oee_data
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/machines/TEST_001/oee")
                
                assert response.status_code == 200
                data = response.json()
                
                # Verify OEE components
                assert data["availability"] == 0.80
                assert data["performance"] == 0.85
                assert data["quality"] == 0.92
                assert data["oee"] == 0.624
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_oee_invalid_date_range():
    """Test OEE calculation with invalid date range."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        start_date = datetime.utcnow()
        end_date = datetime.utcnow() - timedelta(days=1)  # End before start
        
        async with AsyncClient(app=app, base_url="http://test") as client:
            response = await client.get(
                f"/api/v1/machines/TEST_001/oee"
                f"?start_date={start_date.isoformat()}"
                f"&end_date={end_date.isoformat()}"
            )
            
            assert response.status_code == 400
            assert "Start date must be before end date" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_machine_oee_machine_not_found():
    """Test OEE calculation for non-existent machine."""
    mock_db_session = AsyncMock()
    
    app.dependency_overrides[get_database_session_dependency] = lambda: mock_db_session
    
    try:
        with patch('app.services.machine_service.MachineService.calculate_machine_oee') as mock_oee:
            mock_oee.side_effect = ValueError("Machine NONEXISTENT not found")
            
            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/api/v1/machines/NONEXISTENT/oee")
                
                assert response.status_code == 400
                assert "not found" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()