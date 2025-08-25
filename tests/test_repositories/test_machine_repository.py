"""
Unit tests for MachineRepository class.

This module tests machine-specific repository operations including
downtime analysis, OEE calculations, and performance statistics.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.machine_repository import MachineRepository
from app.repositories.base_repository import PaginationParams, PaginatedResult
from app.models.database_models import Machine, JobLogOB


class MockMachine:
    """Mock Machine model for testing."""
    
    def __init__(self, **kwargs):
        self.machine_id = kwargs.get('machine_id', 'M001')
        self.machine_name = kwargs.get('machine_name', 'Test Machine')
        self.status = kwargs.get('status', 'ACTIVE')
        self.job_logs = kwargs.get('job_logs', [])


class MockJobLogOB:
    """Mock JobLogOB model for testing."""
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id', 1)
        self.machine = kwargs.get('machine', 'M001')
        self.start_time = kwargs.get('start_time', datetime.now())
        self.end_time = kwargs.get('end_time')
        self.job_number = kwargs.get('job_number', 'J001')
        self.state = kwargs.get('state', 'RUNNING')
        self.part_number = kwargs.get('part_number', 'P001')
        self.emp_id = kwargs.get('emp_id', 'E001')
        self.operator_name = kwargs.get('operator_name', 'Test Operator')
        self.parts_produced = kwargs.get('parts_produced', 10)
        self.job_duration = kwargs.get('job_duration', 3600)
        self.running_time = kwargs.get('running_time', 3000)
        
        # Downtime fields
        self.setup_time = kwargs.get('setup_time', 300)
        self.waiting_setup_time = kwargs.get('waiting_setup_time', 100)
        self.not_feeding_time = kwargs.get('not_feeding_time', 50)
        self.adjustment_time = kwargs.get('adjustment_time', 75)
        self.dressing_time = kwargs.get('dressing_time', 25)
        self.tooling_time = kwargs.get('tooling_time', 150)
        self.engineering_time = kwargs.get('engineering_time', 0)
        self.maintenance_time = kwargs.get('maintenance_time', 200)
        self.buy_in_time = kwargs.get('buy_in_time', 0)
        self.break_shift_change_time = kwargs.get('break_shift_change_time', 300)
        self.idle_time = kwargs.get('idle_time', 100)
        
        # Relationships (mocked)
        self.machine_ref = kwargs.get('machine_ref')
        self.operator_ref = kwargs.get('operator_ref')
        self.job_ref = kwargs.get('job_ref')
        self.part_ref = kwargs.get('part_ref')


@pytest.mark.asyncio
class TestMachineRepository:
    """Test cases for MachineRepository class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock AsyncSession for testing."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def repository(self, mock_session):
        """Create a MachineRepository instance for testing."""
        return MachineRepository(mock_session)
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization."""
        repo = MachineRepository(mock_session)
        
        assert repo.session == mock_session
        assert repo.model_class == Machine
        assert repo.get_primary_key_field() == "machine_id"
    
    async def test_get_machine_by_id_with_relationships_success(self, repository, mock_session):
        """Test successful retrieval of machine with relationships."""
        mock_machine = MockMachine(machine_id='M001', machine_name='Test Machine')
        mock_machine.job_logs = [MockJobLogOB(), MockJobLogOB()]
        
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_machine
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_machine_by_id_with_relationships('M001')
        
        assert result == mock_machine
        assert len(result.job_logs) == 2
        mock_session.execute.assert_called_once()
    
    async def test_get_machine_by_id_with_relationships_not_found(self, repository, mock_session):
        """Test machine retrieval when machine not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_machine_by_id_with_relationships('M999')
        
        assert result is None
        mock_session.execute.assert_called_once()
    
    async def test_get_active_machines(self, repository, mock_session):
        """Test retrieval of active machines."""
        mock_machines = [
            MockMachine(machine_id='M001', status='ACTIVE'),
            MockMachine(machine_id='M002', status='ACTIVE')
        ]
        
        # Mock the get_all method from base repository
        with patch.object(repository, 'get_all', return_value=mock_machines) as mock_get_all:
            result = await repository.get_active_machines()
            
            assert result == mock_machines
            mock_get_all.assert_called_once()
            
            # Verify the filter condition
            call_args = mock_get_all.call_args
            filters = call_args[1]['filters']
            assert len(filters) == 1
            assert filters[0].field == 'status'
            assert filters[0].value == 'ACTIVE'
    
    async def test_get_machine_job_logs_without_pagination(self, repository, mock_session):
        """Test retrieval of machine job logs without pagination."""
        mock_job_logs = [MockJobLogOB(id=1), MockJobLogOB(id=2)]
        
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = mock_job_logs
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        result = await repository.get_machine_job_logs('M001', start_date, end_date)
        
        assert result == mock_job_logs
        mock_session.execute.assert_called_once()
    
    async def test_get_machine_job_logs_with_pagination(self, repository, mock_session):
        """Test retrieval of machine job logs with pagination."""
        mock_job_logs = [MockJobLogOB(id=1), MockJobLogOB(id=2)]
        
        # Mock count query
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 10
        
        # Mock main query
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = mock_job_logs
        mock_result.scalars.return_value = mock_scalars
        
        mock_session.execute = AsyncMock(side_effect=[mock_count_result, mock_result])
        
        pagination = PaginationParams(skip=0, limit=5)
        result = await repository.get_machine_job_logs('M001', pagination=pagination)
        
        assert isinstance(result, PaginatedResult)
        assert result.items == mock_job_logs
        assert result.total_count == 10
        assert mock_session.execute.call_count == 2
    
    async def test_get_machine_downtime_summary_success(self, repository, mock_session):
        """Test successful downtime summary calculation."""
        # Mock aggregation query result
        mock_row = MagicMock()
        mock_row.total_records = 5
        mock_row.total_running_time = 15000
        mock_row.total_job_duration = 18000
        mock_row.total_setup_time = 1500
        mock_row.total_waiting_setup_time = 500
        mock_row.total_not_feeding_time = 250
        mock_row.total_adjustment_time = 375
        mock_row.total_dressing_time = 125
        mock_row.total_tooling_time = 750
        mock_row.total_engineering_time = 0
        mock_row.total_maintenance_time = 1000
        mock_row.total_buy_in_time = 0
        mock_row.total_break_shift_change_time = 1500
        mock_row.total_idle_time = 500
        mock_row.total_parts_produced = 50
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_row
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        result = await repository.get_machine_downtime_summary('M001', start_date, end_date)
        
        assert result['machine_id'] == 'M001'
        assert result['summary']['total_records'] == 5
        assert result['summary']['total_running_time'] == 15000
        assert result['summary']['total_job_duration'] == 18000
        assert result['summary']['total_parts_produced'] == 50
        
        # Check downtime breakdown
        downtime_breakdown = result['downtime_breakdown']
        assert downtime_breakdown['setup_time'] == 1500
        assert downtime_breakdown['maintenance_time'] == 1000
        
        # Check efficiency metrics
        efficiency_metrics = result['efficiency_metrics']
        assert 'overall_efficiency' in efficiency_metrics
        assert 'downtime_percentage' in efficiency_metrics
        assert 'parts_per_hour' in efficiency_metrics
        
        mock_session.execute.assert_called_once()
    
    async def test_get_machine_downtime_summary_no_data(self, repository, mock_session):
        """Test downtime summary when no data is available."""
        mock_row = MagicMock()
        mock_row.total_records = 0
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_row
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_machine_downtime_summary('M001')
        
        assert result['machine_id'] == 'M001'
        assert result['summary']['total_records'] == 0
        assert result['summary']['total_running_time'] == 0
        assert result['downtime_breakdown'] == {}
        
        mock_session.execute.assert_called_once()
    
    async def test_get_downtime_trends_daily(self, repository, mock_session):
        """Test downtime trends calculation with daily interval."""
        # Mock trend data
        mock_rows = [
            MagicMock(
                period='2023-01-01',
                record_count=2,
                running_time=7200,
                total_downtime=1800,
                parts_produced=20
            ),
            MagicMock(
                period='2023-01-02',
                record_count=3,
                running_time=10800,
                total_downtime=2700,
                parts_produced=30
            )
        ]
        
        mock_result = MagicMock()
        mock_result.all.return_value = mock_rows
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        
        result = await repository.get_downtime_trends('M001', start_date, end_date, 'daily')
        
        assert len(result) == 2
        assert result[0]['period'] == '2023-01-01'
        assert result[0]['record_count'] == 2
        assert result[0]['running_time'] == 7200
        assert result[0]['total_downtime'] == 1800
        assert result[0]['parts_produced'] == 20
        assert 'efficiency' in result[0]
        
        mock_session.execute.assert_called_once()
    
    async def test_calculate_machine_oee_success(self, repository, mock_session):
        """Test successful OEE calculation."""
        # Mock machine retrieval
        mock_machine = MockMachine(machine_id='M001')
        
        # Mock downtime summary
        mock_downtime_summary = {
            'machine_id': 'M001',
            'summary': {
                'total_running_time': 28800,  # 8 hours
                'total_job_duration': 36000,  # 10 hours
                'total_downtime': 7200,       # 2 hours
                'total_parts_produced': 100
            }
        }
        
        with patch.object(repository, 'get_by_id', return_value=mock_machine):
            with patch.object(repository, 'get_machine_downtime_summary', return_value=mock_downtime_summary):
                start_date = datetime(2023, 1, 1)
                end_date = datetime(2023, 1, 2)
                
                result = await repository.calculate_machine_oee('M001', start_date, end_date)
                
                assert result['machine_id'] == 'M001'
                assert 'oee_components' in result
                assert 'availability' in result['oee_components']
                assert 'performance' in result['oee_components']
                assert 'quality' in result['oee_components']
                assert 'oee_score' in result
                assert 'oee_percentage' in result
                assert 'classification' in result
                
                # Check that OEE score is between 0 and 1
                assert 0 <= result['oee_score'] <= 1
    
    async def test_calculate_machine_oee_machine_not_found(self, repository, mock_session):
        """Test OEE calculation when machine is not found."""
        with patch.object(repository, 'get_by_id', return_value=None):
            with pytest.raises(ValueError, match="Machine M999 not found"):
                await repository.calculate_machine_oee('M999')
    
    def test_classify_oee_score(self, repository):
        """Test OEE score classification."""
        # World Class
        classification = repository._classify_oee_score(0.90)
        assert classification['level'] == 'World Class'
        
        # Acceptable
        classification = repository._classify_oee_score(0.70)
        assert classification['level'] == 'Acceptable'
        
        # Low
        classification = repository._classify_oee_score(0.50)
        assert classification['level'] == 'Low'
        
        # Unacceptable
        classification = repository._classify_oee_score(0.30)
        assert classification['level'] == 'Unacceptable'
    
    async def test_get_machine_performance_statistics_success(self, repository, mock_session):
        """Test successful performance statistics calculation."""
        # Mock main statistics query
        mock_stats = MagicMock()
        mock_stats.total_jobs = 10
        mock_stats.avg_running_time = 3600.0
        mock_stats.avg_job_duration = 4000.0
        mock_stats.avg_parts_per_job = 15.0
        mock_stats.max_parts_per_job = 25
        mock_stats.min_parts_per_job = 5
        mock_stats.unique_operators = 3
        mock_stats.unique_jobs = 8
        mock_stats.unique_parts = 5
        
        # Mock top operators query
        mock_operators = [
            MagicMock(
                emp_id='E001',
                operator_name='John Doe',
                job_count=5,
                avg_running_time=3500.0,
                total_parts=75
            ),
            MagicMock(
                emp_id='E002',
                operator_name='Jane Smith',
                job_count=3,
                avg_running_time=3700.0,
                total_parts=45
            )
        ]
        
        # Mock top parts query
        mock_parts = [
            MagicMock(
                part_number='P001',
                job_count=4,
                total_produced=60,
                avg_running_time=3600.0
            ),
            MagicMock(
                part_number='P002',
                job_count=3,
                total_produced=45,
                avg_running_time=3500.0
            )
        ]
        
        # Configure mock session to return different results for different queries
        mock_results = [
            MagicMock(first=lambda: mock_stats),  # Main stats query
            MagicMock(all=lambda: mock_operators),  # Top operators query
            MagicMock(all=lambda: mock_parts)  # Top parts query
        ]
        mock_session.execute = AsyncMock(side_effect=mock_results)
        
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        result = await repository.get_machine_performance_statistics('M001', start_date, end_date)
        
        assert result['machine_id'] == 'M001'
        assert result['statistics']['total_jobs'] == 10
        assert result['statistics']['avg_running_time'] == 3600.0
        assert result['statistics']['unique_operators'] == 3
        
        assert len(result['top_operators']) == 2
        assert result['top_operators'][0]['emp_id'] == 'E001'
        assert result['top_operators'][0]['job_count'] == 5
        
        assert len(result['top_parts']) == 2
        assert result['top_parts'][0]['part_number'] == 'P001'
        assert result['top_parts'][0]['job_count'] == 4
        
        assert mock_session.execute.call_count == 3
    
    async def test_get_machine_performance_statistics_no_data(self, repository, mock_session):
        """Test performance statistics when no data is available."""
        mock_stats = MagicMock()
        mock_stats.total_jobs = 0
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_stats
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_machine_performance_statistics('M001')
        
        assert result['machine_id'] == 'M001'
        assert result['statistics'] == {}
        assert 'No data available' in result['message']
        
        mock_session.execute.assert_called_once()
    
    async def test_get_machine_utilization_success(self, repository, mock_session):
        """Test successful machine utilization calculation."""
        mock_row = MagicMock()
        mock_row.total_usage_time = 28800  # 8 hours
        mock_row.total_running_time = 25200  # 7 hours
        mock_row.total_jobs = 5
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_row
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        start_date = datetime(2023, 1, 1, 0, 0, 0)
        end_date = datetime(2023, 1, 2, 0, 0, 0)  # 24 hours period
        
        result = await repository.get_machine_utilization('M001', start_date, end_date)
        
        assert result['machine_id'] == 'M001'
        assert result['period']['total_period_hours'] == 24.0
        assert result['utilization']['total_usage_time'] == 28800
        assert result['utilization']['total_running_time'] == 25200
        assert result['utilization']['total_jobs'] == 5
        
        # Check calculated percentages
        utilization = result['utilization']
        expected_usage_percentage = (28800 / 86400) * 100  # 86400 seconds in 24 hours
        expected_efficiency_percentage = (25200 / 28800) * 100
        
        assert abs(utilization['usage_percentage'] - expected_usage_percentage) < 0.01
        assert abs(utilization['efficiency_percentage'] - expected_efficiency_percentage) < 0.01
        
        mock_session.execute.assert_called_once()
    
    async def test_get_machine_utilization_no_data(self, repository, mock_session):
        """Test machine utilization when no data is available."""
        mock_row = MagicMock()
        mock_row.total_usage_time = None
        mock_row.total_running_time = None
        mock_row.total_jobs = None
        
        mock_result = MagicMock()
        mock_result.first.return_value = mock_row
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        start_date = datetime(2023, 1, 1, 0, 0, 0)
        end_date = datetime(2023, 1, 2, 0, 0, 0)
        
        result = await repository.get_machine_utilization('M001', start_date, end_date)
        
        assert result['utilization']['total_usage_time'] == 0
        assert result['utilization']['total_running_time'] == 0
        assert result['utilization']['total_jobs'] == 0
        assert result['utilization']['usage_percentage'] == 0
        assert result['utilization']['efficiency_percentage'] == 0
        
        mock_session.execute.assert_called_once()


class TestMachineRepositoryEdgeCases:
    """Test edge cases and error conditions for MachineRepository."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock AsyncSession for testing."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def repository(self, mock_session):
        """Create a MachineRepository instance for testing."""
        return MachineRepository(mock_session)
    
    @pytest.mark.asyncio
    async def test_get_downtime_trends_invalid_interval(self, repository, mock_session):
        """Test downtime trends with invalid interval."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)
        
        with pytest.raises(ValueError, match="Unsupported interval: invalid"):
            await repository.get_downtime_trends('M001', start_date, end_date, 'invalid')
    
    @pytest.mark.asyncio
    async def test_calculate_machine_oee_zero_division_handling(self, repository, mock_session):
        """Test OEE calculation with zero values to check division by zero handling."""
        mock_machine = MockMachine(machine_id='M001')
        
        # Mock downtime summary with zero values
        mock_downtime_summary = {
            'machine_id': 'M001',
            'summary': {
                'total_running_time': 0,
                'total_job_duration': 0,
                'total_downtime': 0,
                'total_parts_produced': 0
            }
        }
        
        with patch.object(repository, 'get_by_id', return_value=mock_machine):
            with patch.object(repository, 'get_machine_downtime_summary', return_value=mock_downtime_summary):
                result = await repository.calculate_machine_oee('M001')
                
                # Should handle zero division gracefully
                assert result['oee_components']['availability'] == 0.0
                assert result['oee_components']['performance'] == 0.0
                assert result['oee_components']['quality'] == 0.0
                assert result['oee_score'] == 0.0