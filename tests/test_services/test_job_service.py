"""
Tests for JobService

This module contains unit tests for the JobService class,
testing business logic, validation, and error handling.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.job_service import JobService
from app.models.database_models import Job


class TestJobService:
    """Test cases for JobService."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def job_service(self, mock_session):
        """Create a JobService instance with mocked dependencies."""
        return JobService(mock_session)
    
    @pytest.fixture
    def sample_job_data(self):
        """Sample job data for testing."""
        return {
            'job_number': 'JOB001',
            'job_name': 'Test Manufacturing Job',
            'customer_id': 'CUST001',
            'customer_name': 'Test Customer Inc.',
            'priority': 'NORMAL',
            'estimated_hours': 40.0,
            'quantity_ordered': 100,
            'due_date': datetime.utcnow() + timedelta(days=30),
            'complexity_rating': 5,
            'setup_complexity': 3
        }
    
    @pytest.fixture
    def sample_job(self, sample_job_data):
        """Create a sample Job instance."""
        job = Job(**sample_job_data)
        job.job_status = 'PENDING'
        job.quantity_completed = 0
        job.created_at = datetime.utcnow()
        job.updated_at = datetime.utcnow()
        return job
    
    # Test create_job method
    
    @pytest.mark.asyncio
    async def test_create_job_success(self, job_service, sample_job_data, sample_job):
        """Test successful job creation."""
        job_service.job_repository.get_by_id = AsyncMock(return_value=None)
        job_service.job_repository.create = AsyncMock(return_value=sample_job)
        
        result = await job_service.create_job(sample_job_data)
        
        assert result == sample_job
        job_service.job_repository.get_by_id.assert_called_once_with('JOB001')
        job_service.job_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_job_missing_required_field(self, job_service):
        """Test job creation with missing required field."""
        incomplete_data = {
            'job_name': 'Test Job',
            'quantity_ordered': 50
            # Missing job_number
        }
        
        with pytest.raises(ValueError, match="Required field 'job_number' is missing or empty"):
            await job_service.create_job(incomplete_data)
    
    @pytest.mark.asyncio
    async def test_create_job_already_exists(self, job_service, sample_job_data, sample_job):
        """Test job creation when job already exists."""
        job_service.job_repository.get_by_id = AsyncMock(return_value=sample_job)
        
        with pytest.raises(ValueError, match="Job with number 'JOB001' already exists"):
            await job_service.create_job(sample_job_data)
    
    @pytest.mark.asyncio
    async def test_create_job_invalid_quantity(self, job_service, sample_job_data):
        """Test job creation with invalid quantity."""
        sample_job_data['quantity_ordered'] = -10  # Invalid negative quantity
        
        job_service.job_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Quantity ordered must be a positive integer"):
            await job_service.create_job(sample_job_data)
    
    @pytest.mark.asyncio
    async def test_create_job_invalid_priority(self, job_service, sample_job_data):
        """Test job creation with invalid priority."""
        sample_job_data['priority'] = 'INVALID_PRIORITY'
        
        job_service.job_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Priority must be one of"):
            await job_service.create_job(sample_job_data)
    
    @pytest.mark.asyncio
    async def test_create_job_invalid_complexity_rating(self, job_service, sample_job_data):
        """Test job creation with invalid complexity rating."""
        sample_job_data['complexity_rating'] = 15  # Out of 1-10 range
        
        job_service.job_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Field 'complexity_rating' must be between 1 and 10"):
            await job_service.create_job(sample_job_data)
    
    @pytest.mark.asyncio
    async def test_create_job_past_due_date_warning(self, job_service, sample_job_data, sample_job):
        """Test job creation with past due date (should log warning but not fail)."""
        sample_job_data['due_date'] = datetime.utcnow() - timedelta(days=5)  # Past date
        
        job_service.job_repository.get_by_id = AsyncMock(return_value=None)
        job_service.job_repository.create = AsyncMock(return_value=sample_job)
        
        # Should not raise exception, just log warning
        result = await job_service.create_job(sample_job_data)
        assert result == sample_job
    
    # Test get_job_by_number method
    
    @pytest.mark.asyncio
    async def test_get_job_by_number_success(self, job_service, sample_job):
        """Test successful job retrieval by number."""
        job_service.job_repository.get_by_id = AsyncMock(return_value=sample_job)
        
        result = await job_service.get_job_by_number('JOB001')
        
        assert result == sample_job
        job_service.job_repository.get_by_id.assert_called_once_with('JOB001')
    
    @pytest.mark.asyncio
    async def test_get_job_by_number_with_relationships(self, job_service, sample_job):
        """Test job retrieval with relationships."""
        job_service.job_repository.get_job_by_number_with_relationships = AsyncMock(return_value=sample_job)
        
        result = await job_service.get_job_by_number('JOB001', include_relationships=True)
        
        assert result == sample_job
        job_service.job_repository.get_job_by_number_with_relationships.assert_called_once_with('JOB001')
    
    @pytest.mark.asyncio
    async def test_get_job_by_number_not_found(self, job_service):
        """Test job retrieval when job not found."""
        job_service.job_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await job_service.get_job_by_number('NONEXISTENT')
        
        assert result is None
    
    # Test get_jobs_by_status method
    
    @pytest.mark.asyncio
    async def test_get_jobs_by_status_success(self, job_service):
        """Test successful jobs retrieval by status."""
        mock_jobs = [MagicMock(), MagicMock()]
        
        job_service.job_repository.get_jobs_by_status = AsyncMock(return_value=mock_jobs)
        
        result = await job_service.get_jobs_by_status('PENDING')
        
        assert result == mock_jobs
        job_service.job_repository.get_jobs_by_status.assert_called_once_with('PENDING')
    
    @pytest.mark.asyncio
    async def test_get_jobs_by_status_invalid_status(self, job_service):
        """Test jobs retrieval with invalid status."""
        with pytest.raises(ValueError, match="Status must be one of"):
            await job_service.get_jobs_by_status('INVALID_STATUS')
    
    # Test update_job method
    
    @pytest.mark.asyncio
    async def test_update_job_success(self, job_service, sample_job):
        """Test successful job update."""
        update_data = {'job_name': 'Updated Job Name', 'priority': 'HIGH'}
        updated_job = MagicMock()
        updated_job.job_number = 'JOB001'
        updated_job.job_name = 'Updated Job Name'
        updated_job.priority = 'HIGH'
        
        job_service.job_repository.get_by_id = AsyncMock(return_value=sample_job)
        job_service.job_repository.update = AsyncMock(return_value=updated_job)
        
        result = await job_service.update_job('JOB001', update_data)
        
        assert result == updated_job
        job_service.job_repository.update.assert_called_once_with('JOB001', **update_data)
    
    @pytest.mark.asyncio
    async def test_update_job_not_found(self, job_service):
        """Test job update when job not found."""
        job_service.job_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await job_service.update_job('NONEXISTENT', {'job_name': 'New Name'})
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_job_status_to_completed(self, job_service, sample_job):
        """Test job update with status change to COMPLETED."""
        update_data = {'job_status': 'COMPLETED'}
        updated_job = MagicMock()
        updated_job.job_status = 'COMPLETED'
        updated_job.completion_date = datetime.utcnow()
        
        job_service.job_repository.get_by_id = AsyncMock(return_value=sample_job)
        job_service.job_repository.update = AsyncMock(return_value=updated_job)
        
        result = await job_service.update_job('JOB001', update_data)
        
        # Verify completion_date was added to update_data
        call_args = job_service.job_repository.update.call_args
        assert 'completion_date' in call_args[1]
    
    @pytest.mark.asyncio
    async def test_update_job_quantity_completed_exceeds_ordered(self, job_service, sample_job):
        """Test job update with quantity completed exceeding ordered."""
        job_service.job_repository.get_by_id = AsyncMock(return_value=sample_job)
        
        with pytest.raises(ValueError, match="Quantity completed cannot exceed quantity ordered"):
            await job_service.update_job('JOB001', {'quantity_completed': 150})  # sample_job has 100 ordered
    
    @pytest.mark.asyncio
    async def test_update_job_auto_complete_on_quantity(self, job_service, sample_job):
        """Test job auto-completion when quantity completed reaches ordered."""
        update_data = {'quantity_completed': 100}  # Matches quantity_ordered
        updated_job = MagicMock()
        updated_job.quantity_completed = 100
        updated_job.job_status = 'COMPLETED'
        
        job_service.job_repository.get_by_id = AsyncMock(return_value=sample_job)
        job_service.job_repository.update = AsyncMock(return_value=updated_job)
        
        result = await job_service.update_job('JOB001', update_data)
        
        # Verify auto-completion logic was triggered
        call_args = job_service.job_repository.update.call_args
        assert call_args[1]['job_status'] == 'COMPLETED'
        assert 'completion_date' in call_args[1]
    
    # Test update_job_progress method
    
    @pytest.mark.asyncio
    async def test_update_job_progress_success(self, job_service, sample_job):
        """Test successful job progress update."""
        updated_job = MagicMock()
        updated_job.quantity_completed = 50
        
        job_service.job_repository.update_job_progress = AsyncMock(return_value=updated_job)
        
        result = await job_service.update_job_progress('JOB001', 50)
        
        assert result == updated_job
        job_service.job_repository.update_job_progress.assert_called_once_with('JOB001', 50)
    
    @pytest.mark.asyncio
    async def test_update_job_progress_invalid_quantity(self, job_service):
        """Test job progress update with invalid quantity."""
        with pytest.raises(ValueError, match="Quantity completed must be a non-negative integer"):
            await job_service.update_job_progress('JOB001', -10)
    
    @pytest.mark.asyncio
    async def test_update_job_progress_not_found(self, job_service):
        """Test job progress update when job not found."""
        job_service.job_repository.update_job_progress = AsyncMock(return_value=None)
        
        result = await job_service.update_job_progress('NONEXISTENT', 50)
        
        assert result is None
    
    # Test get_overdue_jobs method
    
    @pytest.mark.asyncio
    async def test_get_overdue_jobs_success(self, job_service):
        """Test successful overdue jobs retrieval."""
        overdue_job1 = Job(job_number='JOB001', job_name='Job 1', quantity_ordered=100)
        overdue_job1.due_date = datetime.utcnow() - timedelta(days=5)
        overdue_job1.priority = 'HIGH'
        
        overdue_job2 = Job(job_number='JOB002', job_name='Job 2', quantity_ordered=50)
        overdue_job2.due_date = datetime.utcnow() - timedelta(days=2)
        overdue_job2.priority = 'NORMAL'
        
        mock_overdue_jobs = [overdue_job1, overdue_job2]
        
        job_service.job_repository.get_overdue_jobs = AsyncMock(return_value=mock_overdue_jobs)
        
        result = await job_service.get_overdue_jobs()
        
        assert len(result) == 2
        # Verify urgency levels were calculated and jobs sorted
        assert hasattr(result[0], 'urgency_level')
        assert hasattr(result[1], 'urgency_level')
    
    # Test get_job_schedule_analysis method
    
    @pytest.mark.asyncio
    async def test_get_job_schedule_analysis_success(self, job_service):
        """Test successful job schedule analysis."""
        mock_status_summary = {
            'summary': {'total_jobs': 50},
            'status_breakdown': [
                {'status': 'PENDING', 'job_count': 10},
                {'status': 'IN_PROGRESS', 'job_count': 20},
                {'status': 'COMPLETED', 'job_count': 20}
            ],
            'priority_breakdown': [
                {'priority': 'HIGH', 'job_count': 5},
                {'priority': 'NORMAL', 'job_count': 40},
                {'priority': 'LOW', 'job_count': 5}
            ]
        }
        
        mock_overdue_jobs = []
        
        job_service.job_repository.get_job_status_summary = AsyncMock(return_value=mock_status_summary)
        job_service.job_repository.get_overdue_jobs = AsyncMock(return_value=mock_overdue_jobs)
        
        result = await job_service.get_job_schedule_analysis()
        
        assert 'analysis_period' in result
        assert 'status_summary' in result
        assert 'overdue_analysis' in result
        assert 'scheduling_insights' in result
        assert result['overdue_analysis']['overdue_count'] == 0
    
    # Test get_job_performance_analysis method
    
    @pytest.mark.asyncio
    async def test_get_job_performance_analysis_success(self, job_service):
        """Test successful job performance analysis."""
        mock_performance_metrics = {
            'job_number': 'JOB001',
            'job_info': {'job_name': 'Test Job', 'completion_percentage': 75},
            'performance_metrics': {
                'total_operations': 10,
                'efficiency': 0.85,
                'machines_used': 2,
                'operators_involved': 3
            },
            'schedule_performance': {
                'on_schedule': True,
                'estimated_vs_actual_hours': {'variance_percentage': 5}
            }
        }
        
        job_service.job_repository.get_job_performance_metrics = AsyncMock(return_value=mock_performance_metrics)
        
        result = await job_service.get_job_performance_analysis('JOB001', include_details=True)
        
        assert 'performance_insights' in result
        assert result['job_number'] == 'JOB001'
    
    # Test get_customer_job_analysis method
    
    @pytest.mark.asyncio
    async def test_get_customer_job_analysis_success(self, job_service):
        """Test successful customer job analysis."""
        customer_jobs = [
            Job(job_number='JOB001', job_name='Job 1', quantity_ordered=100, quantity_completed=100, job_status='COMPLETED'),
            Job(job_number='JOB002', job_name='Job 2', quantity_ordered=50, quantity_completed=25, job_status='IN_PROGRESS'),
            Job(job_number='JOB003', job_name='Job 3', quantity_ordered=75, quantity_completed=0, job_status='PENDING')
        ]
        
        for job in customer_jobs:
            job.customer_id = 'CUST001'
            job.customer_name = 'Test Customer'
            job.priority = 'NORMAL'
            job.due_date = datetime.utcnow() + timedelta(days=10)
            job.created_at = datetime.utcnow()
        
        job_service.job_repository.get_jobs_by_customer = AsyncMock(return_value=customer_jobs)
        
        result = await job_service.get_customer_job_analysis('CUST001')
        
        assert result['customer_id'] == 'CUST001'
        assert result['summary']['total_jobs'] == 3
        assert result['summary']['completed_jobs'] == 1
        assert len(result['job_breakdown']) == 3
        assert 'insights' in result
    
    @pytest.mark.asyncio
    async def test_get_customer_job_analysis_no_jobs(self, job_service):
        """Test customer job analysis with no jobs."""
        job_service.job_repository.get_jobs_by_customer = AsyncMock(return_value=[])
        
        result = await job_service.get_customer_job_analysis('CUST999')
        
        assert result['customer_id'] == 'CUST999'
        assert 'No jobs found for this customer' in result['message']
    
    # Test private helper methods
    
    def test_calculate_urgency_level_high_priority_overdue(self, job_service):
        """Test urgency calculation for high priority overdue job."""
        job = Job(job_number='JOB001', job_name='Test Job', quantity_ordered=100)
        job.due_date = datetime.utcnow() - timedelta(days=3)
        job.priority = 'URGENT'
        
        urgency = job_service._calculate_urgency_level(job)
        
        assert urgency >= 8  # Should be high urgency
        assert urgency <= 10  # Capped at 10
    
    def test_calculate_urgency_level_low_priority_recent(self, job_service):
        """Test urgency calculation for low priority recently overdue job."""
        job = Job(job_number='JOB001', job_name='Test Job', quantity_ordered=100)
        job.due_date = datetime.utcnow() - timedelta(days=1)
        job.priority = 'LOW'
        
        urgency = job_service._calculate_urgency_level(job)
        
        assert urgency >= 1
        assert urgency <= 5  # Should be moderate urgency
    
    def test_generate_schedule_insights_excellent_performance(self, job_service):
        """Test schedule insights generation for excellent performance."""
        status_summary = {
            'summary': {'total_jobs': 100},
            'status_breakdown': [
                {'status': 'COMPLETED', 'job_count': 90},
                {'status': 'IN_PROGRESS', 'job_count': 10}
            ],
            'priority_breakdown': [
                {'priority': 'NORMAL', 'job_count': 95},
                {'priority': 'HIGH', 'job_count': 5}
            ]
        }
        overdue_jobs = []  # No overdue jobs
        
        insights = job_service._generate_schedule_insights(status_summary, overdue_jobs)
        
        assert insights['schedule_performance'] == 'Excellent'
        assert len(insights['bottlenecks']) == 0
    
    def test_generate_schedule_insights_poor_performance(self, job_service):
        """Test schedule insights generation for poor performance."""
        status_summary = {
            'summary': {'total_jobs': 100},
            'status_breakdown': [
                {'status': 'PENDING', 'job_count': 40},  # High pending
                {'status': 'IN_PROGRESS', 'job_count': 45},  # High in progress
                {'status': 'COMPLETED', 'job_count': 15}
            ],
            'priority_breakdown': [
                {'priority': 'URGENT', 'job_count': 25},  # High urgent
                {'priority': 'HIGH', 'job_count': 30},
                {'priority': 'NORMAL', 'job_count': 45}
            ]
        }
        overdue_jobs = [MagicMock() for _ in range(35)]  # 35% overdue
        
        insights = job_service._generate_schedule_insights(status_summary, overdue_jobs)
        
        assert insights['schedule_performance'] == 'Poor'
        assert len(insights['bottlenecks']) >= 2  # Should identify multiple bottlenecks
        assert len(insights['recommendations']) > 0
    
    def test_generate_job_performance_insights_excellent(self, job_service):
        """Test job performance insights for excellent performance."""
        performance_metrics = {
            'performance_metrics': {
                'efficiency': 0.90,
                'machines_used': 1,
                'operators_involved': 1
            },
            'job_info': {'completion_percentage': 100},
            'schedule_performance': {
                'on_schedule': True,
                'estimated_vs_actual_hours': {'variance_percentage': 5}
            }
        }
        
        insights = job_service._generate_job_performance_insights(performance_metrics)
        
        assert insights['efficiency_assessment'] == 'Excellent'
        assert insights['resource_utilization'] == 'Focused - single machine and operator'
        assert 'Delivered on schedule' in insights['quality_indicators']
        assert 'Accurate time estimation' in insights['quality_indicators']
    
    def test_generate_job_performance_insights_needs_improvement(self, job_service):
        """Test job performance insights for performance needing improvement."""
        performance_metrics = {
            'performance_metrics': {
                'efficiency': 0.45,  # Poor efficiency
                'machines_used': 5,   # Many machines
                'operators_involved': 8  # Many operators
            },
            'job_info': {'completion_percentage': 40},  # Low completion
            'schedule_performance': {
                'on_schedule': False,
                'estimated_vs_actual_hours': {'variance_percentage': 35}  # High variance
            }
        }
        
        insights = job_service._generate_job_performance_insights(performance_metrics)
        
        assert insights['efficiency_assessment'] == 'Poor'
        assert 'Complex - multiple resources involved' in insights['resource_utilization']
        assert 'Delivered late' in insights['quality_indicators']
        assert 'Significant time overrun' in insights['quality_indicators']
        assert len(insights['recommendations']) >= 3
    
    def test_generate_customer_insights_major_customer(self, job_service):
        """Test customer insights for major customer."""
        customer_jobs = []
        for i in range(25):  # Major customer (>=20 jobs)
            job = Job(job_number=f'JOB{i:03d}', job_name=f'Job {i}', quantity_ordered=100)
            job.job_status = 'COMPLETED'
            job.priority = 'NORMAL'
            job.due_date = datetime.utcnow() + timedelta(days=10)
            job.created_at = datetime.utcnow() - timedelta(days=30)
            customer_jobs.append(job)
        
        insights = job_service._generate_customer_insights(customer_jobs)
        
        assert insights['customer_relationship'] == 'Major customer - high volume'
        assert insights['delivery_performance'] == 'Excellent - no overdue jobs'
        assert insights['job_complexity'] == 'Standard complexity jobs'
    
    def test_generate_customer_insights_problematic_customer(self, job_service):
        """Test customer insights for problematic customer."""
        customer_jobs = []
        for i in range(10):
            job = Job(job_number=f'JOB{i:03d}', job_name=f'Job {i}', quantity_ordered=100)
            job.job_status = 'IN_PROGRESS'
            job.priority = 'URGENT'  # High proportion of urgent jobs
            job.due_date = datetime.utcnow() - timedelta(days=5)  # Overdue
            job.created_at = datetime.utcnow() - timedelta(days=60)
            customer_jobs.append(job)
        
        insights = job_service._generate_customer_insights(customer_jobs)
        
        assert insights['customer_relationship'] == 'Regular customer - moderate volume'
        assert insights['delivery_performance'] == 'Poor - frequent delays'
        assert insights['job_complexity'] == 'High urgency customer - frequent rush orders'
        assert len(insights['recommendations']) >= 2