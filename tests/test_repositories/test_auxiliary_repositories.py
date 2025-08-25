"""
Unit tests for auxiliary repository classes (Operator, Job, Part).

This module tests the specialized functionality of OperatorRepository,
JobRepository, and PartRepository classes.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.operator_repository import OperatorRepository
from app.repositories.job_repository import JobRepository
from app.repositories.part_repository import PartRepository
from app.repositories.base_repository import PaginationParams, PaginatedResult
from app.models.database_models import Operator, Job, Part, JobLogOB


class MockOperator:
    """Mock Operator model for testing."""
    
    def __init__(self, **kwargs):
        self.emp_id = kwargs.get('emp_id', 'E001')
        self.operator_name = kwargs.get('operator_name', 'Test Operator')
        self.skill_level = kwargs.get('skill_level', 'INTERMEDIATE')
        self.department = kwargs.get('department', 'Production')
        self.status = kwargs.get('status', 'ACTIVE')
        self.job_logs = kwargs.get('job_logs', [])


class MockJob:
    """Mock Job model for testing."""
    
    def __init__(self, **kwargs):
        self.job_number = kwargs.get('job_number', 'J001')
        self.job_name = kwargs.get('job_name', 'Test Job')
        self.customer_name = kwargs.get('customer_name', 'Test Customer')
        self.priority = kwargs.get('priority', 'NORMAL')
        self.job_status = kwargs.get('job_status', 'IN_PROGRESS')
        self.quantity_ordered = kwargs.get('quantity_ordered', 100)
        self.quantity_completed = kwargs.get('quantity_completed', 50)
        self.estimated_hours = kwargs.get('estimated_hours', 10.0)
        self.actual_hours = kwargs.get('actual_hours', 8.0)
        self.due_date = kwargs.get('due_date', datetime.now() + timedelta(days=7))
        self.start_date = kwargs.get('start_date', datetime.now() - timedelta(days=3))
        self.completion_date = kwargs.get('completion_date')
        self.job_logs = kwargs.get('job_logs', [])


class MockPart:
    """Mock Part model for testing."""
    
    def __init__(self, **kwargs):
        self.part_number = kwargs.get('part_number', 'P001')
        self.part_name = kwargs.get('part_name', 'Test Part')
        self.part_description = kwargs.get('part_description', 'Test part description')
        self.material_type = kwargs.get('material_type', 'Steel')
        self.material_hardness = kwargs.get('material_hardness', 'HRC 45-50')
        self.weight = kwargs.get('weight', 2.5)
        self.dimensions_length = kwargs.get('dimensions_length', 100.0)
        self.dimensions_width = kwargs.get('dimensions_width', 50.0)
        self.dimensions_height = kwargs.get('dimensions_height', 25.0)
        self.tolerance_class = kwargs.get('tolerance_class', 'IT9')
        self.surface_finish = kwargs.get('surface_finish', 'Ra 1.6')
        self.standard_cycle_time = kwargs.get('standard_cycle_time', 300)
        self.cost_per_unit = kwargs.get('cost_per_unit', 15.50)
        self.job_logs = kwargs.get('job_logs', [])


@pytest.mark.asyncio
class TestOperatorRepository:
    """Test cases for OperatorRepository class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock AsyncSession for testing."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def repository(self, mock_session):
        """Create an OperatorRepository instance for testing."""
        return OperatorRepository(mock_session)
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization."""
        repo = OperatorRepository(mock_session)
        
        assert repo.session == mock_session
        assert repo.model_class == Operator
        assert repo.get_primary_key_field() == "emp_id"
    
    async def test_get_operators_by_skill_level(self, repository, mock_session):
        """Test retrieval of operators by skill level."""
        mock_operators = [
            MockOperator(emp_id='E001', skill_level='ADVANCED'),
            MockOperator(emp_id='E002', skill_level='ADVANCED')
        ]
        
        with patch.object(repository, 'get_all', return_value=mock_operators) as mock_get_all:
            result = await repository.get_operators_by_skill_level('ADVANCED')
            
            assert result == mock_operators
            mock_get_all.assert_called_once()
            
            # Verify filter conditions
            call_args = mock_get_all.call_args
            filters = call_args[1]['filters']
            assert len(filters) == 2
            assert any(f.field == 'skill_level' and f.value == 'ADVANCED' for f in filters)
            assert any(f.field == 'status' and f.value == 'ACTIVE' for f in filters)
    
    async def test_get_operator_performance_metrics_success(self, repository, mock_session):
        """Test successful operator performance metrics calculation."""
        # Mock performance query result
        mock_row = MagicMock()
        mock_row.total_jobs = 10
        mock_row.total_running_time = 36000  # 10 hours
        mock_row.total_job_duration = 40000  # 11.1 hours
        mock_row.total_parts_produced = 150
        mock_row.avg_running_time = 3600.0
        mock_row.avg_job_duration = 4000.0
        mock_row.avg_parts_per_job = 15.0
        mock_row.max_parts_per_job = 25
        mock_row.min_parts_per_job = 5
        mock_row.machines_operated = 3
        mock_row.unique_jobs = 8
        mock_row.unique_parts = 5
        mock_row.total_setup_time = 3000
        mock_row.total_maintenance_time = 1000
        mock_row.total_adjustment_time = 500
        mock_row.total_tooling_time = 800
        mock_row.total_idle_time = 200
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_row
        
        # Mock machine performance method
        mock_machine_performance = [
            {
                'machine': 'M001',
                'job_count': 5,
                'efficiency': 0.85,
                'productivity_per_hour': 12.5
            }
        ]
        
        with patch.object(repository, '_get_operator_machine_performance', return_value=mock_machine_performance):
            mock_session.execute = AsyncMock(return_value=mock_result)
            
            start_date = datetime(2023, 1, 1)
            end_date = datetime(2023, 12, 31)
            
            result = await repository.get_operator_performance_metrics('E001', start_date, end_date)
            
            assert result['emp_id'] == 'E001'
            assert result['performance_metrics']['total_jobs'] == 10
            assert result['performance_metrics']['total_parts_produced'] == 150
            assert result['performance_metrics']['efficiency'] == 36000 / 40000  # 0.9
            assert 'downtime_breakdown' in result
            assert 'machine_performance' in result
            
            mock_session.execute.assert_called_once()
    
    async def test_get_operator_performance_metrics_no_data(self, repository, mock_session):
        """Test operator performance metrics when no data is available."""
        mock_row = MagicMock()
        mock_row.total_jobs = 0
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_row
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_operator_performance_metrics('E999')
        
        assert result['emp_id'] == 'E999'
        assert result['performance_metrics'] == {}
        assert 'No data available' in result['message']
        
        mock_session.execute.assert_called_once()
    
    async def test_get_top_performers_by_productivity(self, repository, mock_session):
        """Test retrieval of top performers by productivity metric."""
        mock_rows = [
            MagicMock(
                emp_id='E001',
                operator_name='John Doe',
                skill_level='EXPERT',
                department='Production',
                total_jobs=10,
                total_running_time=36000,
                total_job_duration=40000,
                total_parts_produced=200,
                avg_parts_per_job=20.0
            ),
            MagicMock(
                emp_id='E002',
                operator_name='Jane Smith',
                skill_level='ADVANCED',
                department='Production',
                total_jobs=8,
                total_running_time=28800,
                total_job_duration=32000,
                total_parts_produced=150,
                avg_parts_per_job=18.75
            )
        ]
        
        mock_result = MagicMock()
        mock_result.all.return_value = mock_rows
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_top_performers('productivity', limit=5)
        
        assert len(result) == 2
        assert result[0]['emp_id'] == 'E001'
        assert result[0]['operator_name'] == 'John Doe'
        assert result[0]['total_parts_produced'] == 200
        assert result[0]['productivity_per_hour'] == 200 / (36000 / 3600)  # 20 parts/hour
        
        mock_session.execute.assert_called_once()
    
    async def test_get_top_performers_invalid_metric(self, repository, mock_session):
        """Test top performers with invalid metric."""
        with pytest.raises(ValueError, match="Unsupported metric: invalid"):
            await repository.get_top_performers('invalid')


@pytest.mark.asyncio
class TestJobRepository:
    """Test cases for JobRepository class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock AsyncSession for testing."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def repository(self, mock_session):
        """Create a JobRepository instance for testing."""
        return JobRepository(mock_session)
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization."""
        repo = JobRepository(mock_session)
        
        assert repo.session == mock_session
        assert repo.model_class == Job
        assert repo.get_primary_key_field() == "job_number"
    
    async def test_get_jobs_by_status(self, repository, mock_session):
        """Test retrieval of jobs by status."""
        mock_jobs = [
            MockJob(job_number='J001', job_status='IN_PROGRESS'),
            MockJob(job_number='J002', job_status='IN_PROGRESS')
        ]
        
        with patch.object(repository, 'get_all', return_value=mock_jobs) as mock_get_all:
            result = await repository.get_jobs_by_status('IN_PROGRESS')
            
            assert result == mock_jobs
            mock_get_all.assert_called_once()
            
            # Verify filter condition
            call_args = mock_get_all.call_args
            filters = call_args[1]['filters']
            assert len(filters) == 1
            assert filters[0].field == 'job_status'
            assert filters[0].value == 'IN_PROGRESS'
    
    async def test_get_overdue_jobs(self, repository, mock_session):
        """Test retrieval of overdue jobs."""
        # Create overdue job (due date in the past)
        overdue_date = datetime.utcnow() - timedelta(days=5)
        mock_jobs = [MockJob(job_number='J001', due_date=overdue_date, job_status='IN_PROGRESS')]
        
        with patch.object(repository, 'get_all', return_value=mock_jobs) as mock_get_all:
            result = await repository.get_overdue_jobs()
            
            assert result == mock_jobs
            mock_get_all.assert_called_once()
            
            # Verify filter conditions
            call_args = mock_get_all.call_args
            filters = call_args[1]['filters']
            assert len(filters) == 3  # due_date < now, status != COMPLETED, status != CANCELLED
    
    async def test_get_job_performance_metrics_success(self, repository, mock_session):
        """Test successful job performance metrics calculation."""
        # Mock job retrieval
        mock_job = MockJob(
            job_number='J001',
            job_name='Test Job',
            quantity_ordered=100,
            quantity_completed=75,
            estimated_hours=10.0,
            actual_hours=8.5
        )
        
        # Mock performance query result
        mock_row = MagicMock()
        mock_row.total_operations = 5
        mock_row.total_running_time = 30000
        mock_row.total_job_duration = 35000
        mock_row.total_parts_produced = 75
        mock_row.avg_running_time = 6000.0
        mock_row.avg_job_duration = 7000.0
        mock_row.avg_parts_per_operation = 15.0
        mock_row.machines_used = 2
        mock_row.operators_involved = 3
        mock_row.unique_parts = 2
        mock_row.total_setup_time = 2000
        mock_row.total_maintenance_time = 1000
        mock_row.total_adjustment_time = 500
        mock_row.total_tooling_time = 800
        mock_row.total_idle_time = 200
        mock_row.first_operation = datetime(2023, 1, 1)
        mock_row.last_operation = datetime(2023, 1, 5)
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_row
        
        with patch.object(repository, 'get_by_id', return_value=mock_job):
            with patch.object(repository, '_get_job_operation_details', return_value=[]):
                mock_session.execute = AsyncMock(return_value=mock_result)
                
                result = await repository.get_job_performance_metrics('J001', include_details=False)
                
                assert result['job_number'] == 'J001'
                assert result['job_info']['job_name'] == 'Test Job'
                assert result['job_info']['completion_percentage'] == 75.0  # 75/100 * 100
                assert result['performance_metrics']['total_operations'] == 5
                assert result['performance_metrics']['efficiency'] == 30000 / 35000
                assert 'schedule_performance' in result
                
                mock_session.execute.assert_called_once()
    
    async def test_get_job_performance_metrics_job_not_found(self, repository, mock_session):
        """Test job performance metrics when job is not found."""
        with patch.object(repository, 'get_by_id', return_value=None):
            with pytest.raises(ValueError, match="Job J999 not found"):
                await repository.get_job_performance_metrics('J999')
    
    async def test_update_job_progress_completion(self, repository, mock_session):
        """Test updating job progress to completion."""
        mock_job = MockJob(job_number='J001', quantity_ordered=100, quantity_completed=50)
        
        # Mock the update method to return updated job
        updated_job = MockJob(
            job_number='J001',
            quantity_ordered=100,
            quantity_completed=100,
            job_status='COMPLETED'
        )
        
        with patch.object(repository, 'get_by_id', return_value=mock_job):
            with patch.object(repository, 'update', return_value=updated_job) as mock_update:
                result = await repository.update_job_progress('J001', 100)
                
                assert result == updated_job
                mock_update.assert_called_once()
                
                # Verify update data includes completion status
                call_args = mock_update.call_args
                update_data = call_args[1]
                assert update_data['quantity_completed'] == 100
                assert update_data['job_status'] == 'COMPLETED'
                assert 'completion_date' in update_data


@pytest.mark.asyncio
class TestPartRepository:
    """Test cases for PartRepository class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock AsyncSession for testing."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def repository(self, mock_session):
        """Create a PartRepository instance for testing."""
        return PartRepository(mock_session)
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization."""
        repo = PartRepository(mock_session)
        
        assert repo.session == mock_session
        assert repo.model_class == Part
        assert repo.get_primary_key_field() == "part_number"
    
    async def test_get_parts_by_material_type(self, repository, mock_session):
        """Test retrieval of parts by material type."""
        mock_parts = [
            MockPart(part_number='P001', material_type='Steel'),
            MockPart(part_number='P002', material_type='Steel')
        ]
        
        with patch.object(repository, 'get_all', return_value=mock_parts) as mock_get_all:
            result = await repository.get_parts_by_material_type('Steel')
            
            assert result == mock_parts
            mock_get_all.assert_called_once()
            
            # Verify filter condition
            call_args = mock_get_all.call_args
            filters = call_args[1]['filters']
            assert len(filters) == 1
            assert filters[0].field == 'material_type'
            assert filters[0].value == 'Steel'
    
    async def test_search_parts_by_dimensions(self, repository, mock_session):
        """Test searching parts by dimensional constraints."""
        mock_parts = [MockPart(part_number='P001', dimensions_length=100.0)]
        
        with patch.object(repository, 'get_all', return_value=mock_parts) as mock_get_all:
            result = await repository.search_parts_by_dimensions(
                min_length=50.0, max_length=150.0,
                min_width=25.0, max_width=75.0
            )
            
            assert result == mock_parts
            mock_get_all.assert_called_once()
            
            # Verify filter conditions
            call_args = mock_get_all.call_args
            filters = call_args[1]['filters']
            assert len(filters) == 4  # min/max for length and width
    
    async def test_get_part_production_history_success(self, repository, mock_session):
        """Test successful part production history retrieval."""
        # Mock part retrieval
        mock_part = MockPart(
            part_number='P001',
            part_name='Test Part',
            material_type='Steel',
            standard_cycle_time=300
        )
        
        # Mock production summary query result
        mock_summary_row = MagicMock()
        mock_summary_row.total_operations = 8
        mock_summary_row.total_parts_produced = 120
        mock_summary_row.total_running_time = 28800  # 8 hours
        mock_summary_row.total_job_duration = 32000  # 8.89 hours
        mock_summary_row.avg_parts_per_operation = 15.0
        mock_summary_row.avg_running_time = 3600.0
        mock_summary_row.machines_used = 3
        mock_summary_row.operators_involved = 4
        mock_summary_row.jobs_involved = 5
        mock_summary_row.first_production = datetime(2023, 1, 1)
        mock_summary_row.last_production = datetime(2023, 12, 31)
        
        mock_summary_result = MagicMock()
        mock_summary_result.first.return_value = mock_summary_row
        
        # Mock machine performance method
        mock_machine_performance = [
            {
                'machine': 'M001',
                'parts_produced': 60,
                'efficiency': 0.88,
                'avg_cycle_time': 240.0
            }
        ]
        
        with patch.object(repository, 'get_by_id', return_value=mock_part):
            with patch.object(repository, '_get_part_machine_performance', return_value=mock_machine_performance):
                mock_session.execute = AsyncMock(return_value=mock_summary_result)
                
                start_date = datetime(2023, 1, 1)
                end_date = datetime(2023, 12, 31)
                
                result = await repository.get_part_production_history('P001', start_date, end_date)
                
                assert result['part_number'] == 'P001'
                assert result['part_info']['part_name'] == 'Test Part'
                assert result['production_summary']['total_operations'] == 8
                assert result['production_summary']['total_parts_produced'] == 120
                assert result['production_summary']['efficiency'] == 28800 / 32000  # 0.9
                
                # Check cycle time calculations
                actual_cycle_time = 28800 / 120  # 240 seconds
                expected_variance = ((actual_cycle_time - 300) / 300) * 100  # -20%
                assert result['production_summary']['actual_cycle_time'] == actual_cycle_time
                assert result['production_summary']['cycle_time_variance_percentage'] == expected_variance
                
                assert 'machine_performance' in result
                
                mock_session.execute.assert_called_once()
    
    async def test_get_part_production_history_part_not_found(self, repository, mock_session):
        """Test part production history when part is not found."""
        with patch.object(repository, 'get_by_id', return_value=None):
            with pytest.raises(ValueError, match="Part P999 not found"):
                await repository.get_part_production_history('P999')
    
    async def test_get_material_analysis(self, repository, mock_session):
        """Test material analysis functionality."""
        mock_rows = [
            MagicMock(
                material_type='Steel',
                unique_parts=15,
                total_operations=50,
                total_parts_produced=750,
                total_running_time=180000,  # 50 hours
                total_job_duration=200000,  # 55.56 hours
                avg_parts_per_operation=15.0,
                avg_running_time=3600.0
            ),
            MagicMock(
                material_type='Aluminum',
                unique_parts=8,
                total_operations=25,
                total_parts_produced=400,
                total_running_time=90000,  # 25 hours
                total_job_duration=100000,  # 27.78 hours
                avg_parts_per_operation=16.0,
                avg_running_time=3600.0
            )
        ]
        
        mock_result = MagicMock()
        mock_result.all.return_value = mock_rows
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        result = await repository.get_material_analysis(start_date, end_date)
        
        assert len(result['material_types']) == 2
        
        steel_data = result['material_types'][0]
        assert steel_data['material_type'] == 'Steel'
        assert steel_data['unique_parts'] == 15
        assert steel_data['total_parts_produced'] == 750
        assert steel_data['efficiency'] == 180000 / 200000  # 0.9
        assert steel_data['productivity_per_hour'] == 750 / (180000 / 3600)  # 15 parts/hour
        
        mock_session.execute.assert_called_once()
    
    async def test_get_part_complexity_analysis(self, repository, mock_session):
        """Test part complexity analysis functionality."""
        # Mock precision distribution query
        mock_precision_rows = [
            MagicMock(
                precision_category='High Precision',
                part_count=10,
                avg_cycle_time=450.0,
                avg_cost_per_unit=25.50
            ),
            MagicMock(
                precision_category='Medium Precision',
                part_count=25,
                avg_cycle_time=300.0,
                avg_cost_per_unit=15.75
            )
        ]
        
        # Mock hardness distribution query
        mock_hardness_rows = [
            MagicMock(
                material_hardness='HRC 45-50',
                part_count=15,
                avg_cycle_time=350.0
            )
        ]
        
        # Mock size distribution query
        mock_size_rows = [
            MagicMock(
                size_category='Medium',
                part_count=20,
                avg_cycle_time=320.0,
                avg_weight=2.5
            )
        ]
        
        # Configure mock session to return different results for different queries
        mock_results = [
            MagicMock(all=lambda: mock_precision_rows),
            MagicMock(all=lambda: mock_hardness_rows),
            MagicMock(all=lambda: mock_size_rows)
        ]
        mock_session.execute = AsyncMock(side_effect=mock_results)
        
        result = await repository.get_part_complexity_analysis()
        
        assert len(result['precision_distribution']) == 2
        assert len(result['hardness_distribution']) == 1
        assert len(result['size_distribution']) == 1
        
        # Check precision distribution
        high_precision = result['precision_distribution'][0]
        assert high_precision['precision_category'] == 'High Precision'
        assert high_precision['part_count'] == 10
        assert high_precision['avg_cycle_time'] == 450.0
        
        assert mock_session.execute.call_count == 3


class TestRepositoryEdgeCases:
    """Test edge cases and error conditions for auxiliary repositories."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock AsyncSession for testing."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.mark.asyncio
    async def test_operator_repository_empty_skill_level(self, mock_session):
        """Test operator repository with empty skill level."""
        repo = OperatorRepository(mock_session)
        
        with patch.object(repo, 'get_all', return_value=[]) as mock_get_all:
            result = await repo.get_operators_by_skill_level('')
            
            assert result == []
            mock_get_all.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_job_repository_update_progress_job_not_found(self, mock_session):
        """Test job progress update when job is not found."""
        repo = JobRepository(mock_session)
        
        with patch.object(repo, 'get_by_id', return_value=None):
            result = await repo.update_job_progress('J999', 50)
            
            assert result is None
    
    @pytest.mark.asyncio
    async def test_part_repository_search_no_constraints(self, mock_session):
        """Test part search with no dimensional constraints."""
        repo = PartRepository(mock_session)
        
        with patch.object(repo, 'get_all', return_value=[]) as mock_get_all:
            result = await repo.search_parts_by_dimensions()
            
            assert result == []
            # Should call get_all with empty filters
            call_args = mock_get_all.call_args
            filters = call_args[1]['filters']
            assert len(filters) == 0