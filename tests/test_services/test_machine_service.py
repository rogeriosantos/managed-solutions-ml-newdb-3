"""
Tests for MachineService

This module contains unit tests for the MachineService class,
testing business logic, validation, and error handling.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.machine_service import MachineService
from app.models.database_models import Machine
from app.repositories.base_repository import PaginationParams


class TestMachineService:
    """Test cases for MachineService."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def machine_service(self, mock_session):
        """Create a MachineService instance with mocked dependencies."""
        return MachineService(mock_session)
    
    @pytest.fixture
    def sample_machine_data(self):
        """Sample machine data for testing."""
        return {
            'machine_id': 'CNC001',
            'machine_name': 'Test CNC Machine',
            'machine_type': 'CNC_MILL',
            'manufacturer': 'Test Manufacturer',
            'model': 'TM-100',
            'year_installed': 2020,
            'max_spindle_speed': 10000,
            'max_feed_rate': 1000.0,
            'work_envelope_x': 500.0,
            'work_envelope_y': 300.0,
            'work_envelope_z': 200.0
        }
    
    @pytest.fixture
    def sample_machine(self, sample_machine_data):
        """Create a sample Machine instance."""
        machine = Machine(**sample_machine_data)
        machine.status = 'ACTIVE'
        machine.created_at = datetime.utcnow()
        machine.updated_at = datetime.utcnow()
        return machine
    
    # Test create_machine method
    
    @pytest.mark.asyncio
    async def test_create_machine_success(self, machine_service, sample_machine_data, sample_machine):
        """Test successful machine creation."""
        # Mock repository methods
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=None)
        machine_service.machine_repository.create = AsyncMock(return_value=sample_machine)
        
        result = await machine_service.create_machine(sample_machine_data)
        
        assert result == sample_machine
        machine_service.machine_repository.get_by_id.assert_called_once_with('CNC001')
        machine_service.machine_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_machine_missing_required_field(self, machine_service):
        """Test machine creation with missing required field."""
        incomplete_data = {
            'machine_name': 'Test Machine',
            'machine_type': 'CNC_MILL'
            # Missing machine_id
        }
        
        with pytest.raises(ValueError, match="Required field 'machine_id' is missing or empty"):
            await machine_service.create_machine(incomplete_data)
    
    @pytest.mark.asyncio
    async def test_create_machine_already_exists(self, machine_service, sample_machine_data, sample_machine):
        """Test machine creation when machine already exists."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        
        with pytest.raises(ValueError, match="Machine with ID 'CNC001' already exists"):
            await machine_service.create_machine(sample_machine_data)
    
    @pytest.mark.asyncio
    async def test_create_machine_invalid_numeric_field(self, machine_service, sample_machine_data):
        """Test machine creation with invalid numeric field."""
        sample_machine_data['max_spindle_speed'] = -1000  # Invalid negative value
        
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Field 'max_spindle_speed' must be a positive number"):
            await machine_service.create_machine(sample_machine_data)
    
    # Test get_machine_by_id method
    
    @pytest.mark.asyncio
    async def test_get_machine_by_id_success(self, machine_service, sample_machine):
        """Test successful machine retrieval by ID."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        
        result = await machine_service.get_machine_by_id('CNC001')
        
        assert result == sample_machine
        machine_service.machine_repository.get_by_id.assert_called_once_with('CNC001')
    
    @pytest.mark.asyncio
    async def test_get_machine_by_id_with_relationships(self, machine_service, sample_machine):
        """Test machine retrieval with relationships."""
        machine_service.machine_repository.get_machine_by_id_with_relationships = AsyncMock(return_value=sample_machine)
        
        result = await machine_service.get_machine_by_id('CNC001', include_relationships=True)
        
        assert result == sample_machine
        machine_service.machine_repository.get_machine_by_id_with_relationships.assert_called_once_with('CNC001')
    
    @pytest.mark.asyncio
    async def test_get_machine_by_id_not_found(self, machine_service):
        """Test machine retrieval when machine not found."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await machine_service.get_machine_by_id('NONEXISTENT')
        
        assert result is None
    
    # Test update_machine method
    
    @pytest.mark.asyncio
    async def test_update_machine_success(self, machine_service, sample_machine):
        """Test successful machine update."""
        update_data = {'machine_name': 'Updated Machine Name'}
        updated_machine = MagicMock()
        updated_machine.machine_id = 'CNC001'
        updated_machine.machine_name = 'Updated Machine Name'
        
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        machine_service.machine_repository.update = AsyncMock(return_value=updated_machine)
        
        result = await machine_service.update_machine('CNC001', update_data)
        
        assert result == updated_machine
        machine_service.machine_repository.update.assert_called_once_with('CNC001', **update_data)
    
    @pytest.mark.asyncio
    async def test_update_machine_not_found(self, machine_service):
        """Test machine update when machine not found."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await machine_service.update_machine('NONEXISTENT', {'machine_name': 'New Name'})
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_machine_invalid_status(self, machine_service, sample_machine):
        """Test machine update with invalid status."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        
        with pytest.raises(ValueError, match="Status must be one of"):
            await machine_service.update_machine('CNC001', {'status': 'INVALID_STATUS'})
    
    # Test delete_machine method
    
    @pytest.mark.asyncio
    async def test_delete_machine_success(self, machine_service, sample_machine):
        """Test successful machine deletion (soft delete)."""
        retired_machine = MagicMock()
        retired_machine.status = 'RETIRED'
        
        machine_service.machine_repository.update = AsyncMock(return_value=retired_machine)
        
        result = await machine_service.delete_machine('CNC001')
        
        assert result is True
        machine_service.machine_repository.update.assert_called_once()
        
        # Check that update was called with RETIRED status
        call_args = machine_service.machine_repository.update.call_args
        assert call_args[0][0] == 'CNC001'
        assert call_args[1]['status'] == 'RETIRED'
    
    @pytest.mark.asyncio
    async def test_delete_machine_not_found(self, machine_service):
        """Test machine deletion when machine not found."""
        machine_service.machine_repository.update = AsyncMock(return_value=None)
        
        result = await machine_service.delete_machine('NONEXISTENT')
        
        assert result is False
    
    # Test get_machine_data method
    
    @pytest.mark.asyncio
    async def test_get_machine_data_success(self, machine_service, sample_machine):
        """Test successful machine data retrieval."""
        mock_job_logs = [MagicMock(), MagicMock()]
        start_date = datetime.utcnow() - timedelta(days=30)
        end_date = datetime.utcnow()
        
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        machine_service.machine_repository.get_machine_job_logs = AsyncMock(return_value=mock_job_logs)
        
        result = await machine_service.get_machine_data('CNC001', start_date, end_date)
        
        assert result == mock_job_logs
        machine_service.machine_repository.get_machine_job_logs.assert_called_once_with(
            machine_id='CNC001',
            start_date=start_date,
            end_date=end_date,
            pagination=None
        )
    
    @pytest.mark.asyncio
    async def test_get_machine_data_machine_not_found(self, machine_service):
        """Test machine data retrieval when machine not found."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Machine CNC001 not found"):
            await machine_service.get_machine_data('CNC001')
    
    @pytest.mark.asyncio
    async def test_get_machine_data_invalid_date_range(self, machine_service, sample_machine):
        """Test machine data retrieval with invalid date range."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        
        start_date = datetime.utcnow()
        end_date = datetime.utcnow() - timedelta(days=1)  # End before start
        
        with pytest.raises(ValueError, match="Start date must be before end date"):
            await machine_service.get_machine_data('CNC001', start_date, end_date)
    
    @pytest.mark.asyncio
    async def test_get_machine_data_default_date_range(self, machine_service, sample_machine):
        """Test machine data retrieval with default date range."""
        mock_job_logs = [MagicMock()]
        
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        machine_service.machine_repository.get_machine_job_logs = AsyncMock(return_value=mock_job_logs)
        
        result = await machine_service.get_machine_data('CNC001')
        
        assert result == mock_job_logs
        
        # Verify that dates were set (last 30 days)
        call_args = machine_service.machine_repository.get_machine_job_logs.call_args
        assert call_args[1]['start_date'] is not None
        assert call_args[1]['end_date'] is not None
    
    # Test analyze_machine_downtime method
    
    @pytest.mark.asyncio
    async def test_analyze_machine_downtime_success(self, machine_service, sample_machine):
        """Test successful machine downtime analysis."""
        mock_downtime_summary = {
            'machine_id': 'CNC001',
            'summary': {'total_downtime': 3600},
            'downtime_breakdown': {'setup_time': 1800, 'maintenance_time': 1800},
            'efficiency_metrics': {'overall_efficiency': 0.75}
        }
        mock_trends = [{'period': '2023-01-01', 'total_downtime': 1800}]
        
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        machine_service.machine_repository.get_machine_downtime_summary = AsyncMock(return_value=mock_downtime_summary)
        machine_service.machine_repository.get_downtime_trends = AsyncMock(return_value=mock_trends)
        
        result = await machine_service.analyze_machine_downtime('CNC001', include_trends=True)
        
        assert result['machine_id'] == 'CNC001'
        assert 'downtime_summary' in result
        assert 'downtime_insights' in result
        assert 'downtime_trends' in result
        assert 'trend_insights' in result
    
    # Test calculate_machine_oee method
    
    @pytest.mark.asyncio
    async def test_calculate_machine_oee_success(self, machine_service, sample_machine):
        """Test successful OEE calculation."""
        mock_oee_metrics = {
            'machine_id': 'CNC001',
            'oee_components': {'availability': 0.85, 'performance': 0.90, 'quality': 0.95},
            'oee_score': 0.726,
            'classification': {'level': 'Good'}
        }
        
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        machine_service.machine_repository.calculate_machine_oee = AsyncMock(return_value=mock_oee_metrics)
        
        result = await machine_service.calculate_machine_oee('CNC001', include_benchmarks=True)
        
        assert result['machine_id'] == 'CNC001'
        assert 'business_insights' in result
        assert 'industry_benchmarks' in result
        assert result['oee_score'] == 0.726
    
    # Test private helper methods
    
    def test_analyze_downtime_patterns_no_downtime(self, machine_service):
        """Test downtime pattern analysis with no downtime."""
        downtime_summary = {
            'downtime_breakdown': {},
            'efficiency_metrics': {'overall_efficiency': 1.0}
        }
        
        insights = machine_service._analyze_downtime_patterns(downtime_summary)
        
        assert insights['severity_assessment'] == 'Excellent'
        assert 'Maintain current operational practices' in insights['recommendations']
    
    def test_analyze_downtime_patterns_with_issues(self, machine_service):
        """Test downtime pattern analysis with significant downtime."""
        downtime_summary = {
            'downtime_breakdown': {
                'setup_time': 5000,
                'maintenance_time': 3000,
                'idle_time': 2000
            },
            'efficiency_metrics': {'overall_efficiency': 0.45}
        }
        
        insights = machine_service._analyze_downtime_patterns(downtime_summary)
        
        assert insights['severity_assessment'] == 'Critical'
        assert len(insights['primary_downtime_causes']) > 0
        assert insights['primary_downtime_causes'][0]['cause'] == 'Setup Time'
        assert len(insights['recommendations']) > 0
    
    def test_generate_oee_insights_world_class(self, machine_service, sample_machine):
        """Test OEE insights generation for world-class performance."""
        oee_metrics = {
            'oee_components': {'availability': 0.95, 'performance': 0.95, 'quality': 0.99},
            'oee_score': 0.89,
            'classification': {'level': 'World Class'}
        }
        
        insights = machine_service._generate_oee_insights(oee_metrics, sample_machine)
        
        assert insights['performance_assessment'] == 'World Class'
        assert len(insights['improvement_opportunities']) == 0  # No major opportunities for world-class
    
    def test_generate_oee_insights_needs_improvement(self, machine_service, sample_machine):
        """Test OEE insights generation for performance needing improvement."""
        oee_metrics = {
            'oee_components': {'availability': 0.70, 'performance': 0.80, 'quality': 0.90},
            'oee_score': 0.504,
            'classification': {'level': 'Low'}
        }
        
        insights = machine_service._generate_oee_insights(oee_metrics, sample_machine)
        
        assert insights['performance_assessment'] == 'Low'
        assert len(insights['improvement_opportunities']) > 0
        assert len(insights['priority_actions']) > 0
        
        # Check that availability is identified as the top opportunity
        top_opportunity = insights['improvement_opportunities'][0]
        assert top_opportunity['area'] == 'Availability'
    
    def test_get_industry_benchmarks_cnc_machine(self, machine_service):
        """Test industry benchmarks for CNC machine type."""
        benchmarks = machine_service._get_industry_benchmarks('CNC_MILL')
        
        assert benchmarks['machine_type'] == 'CNC_MILL'
        assert benchmarks['world_class_oee'] == 0.80  # Adjusted for CNC
        assert benchmarks['source'] == 'Industry Standards'
    
    def test_get_industry_benchmarks_assembly_machine(self, machine_service):
        """Test industry benchmarks for assembly machine type."""
        benchmarks = machine_service._get_industry_benchmarks('ASSEMBLY_LINE')
        
        assert benchmarks['machine_type'] == 'ASSEMBLY_LINE'
        assert benchmarks['world_class_oee'] == 0.90  # Higher for assembly
    
    # Test error handling
    
    @pytest.mark.asyncio
    async def test_create_machine_repository_error(self, machine_service, sample_machine_data):
        """Test machine creation when repository raises an error."""
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=None)
        machine_service.machine_repository.create = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception, match="Database error"):
            await machine_service.create_machine(sample_machine_data)
    
    @pytest.mark.asyncio
    async def test_get_machine_summary_statistics_success(self, machine_service, sample_machine):
        """Test successful machine summary statistics retrieval."""
        mock_performance_stats = {
            'machine_id': 'CNC001',
            'statistics': {'total_jobs': 100, 'avg_running_time': 3600}
        }
        mock_downtime_summary = {
            'machine_id': 'CNC001',
            'summary': {'total_downtime': 1800}
        }
        
        machine_service.machine_repository.get_by_id = AsyncMock(return_value=sample_machine)
        machine_service.machine_repository.get_machine_performance_statistics = AsyncMock(return_value=mock_performance_stats)
        machine_service.machine_repository.get_machine_downtime_summary = AsyncMock(return_value=mock_downtime_summary)
        
        result = await machine_service.get_machine_summary_statistics('CNC001')
        
        assert 'machine_info' in result
        assert 'performance_statistics' in result
        assert 'downtime_summary' in result
        assert 'business_insights' in result
        assert result['machine_info']['machine_id'] == 'CNC001'