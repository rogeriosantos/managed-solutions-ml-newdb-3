"""
Tests for OperatorService

This module contains unit tests for the OperatorService class,
testing business logic, validation, and error handling.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.operator_service import OperatorService
from app.models.database_models import Operator


class TestOperatorService:
    """Test cases for OperatorService."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def operator_service(self, mock_session):
        """Create an OperatorService instance with mocked dependencies."""
        return OperatorService(mock_session)
    
    @pytest.fixture
    def sample_operator_data(self):
        """Sample operator data for testing."""
        return {
            'emp_id': 'EMP001',
            'operator_name': 'John Doe',
            'skill_level': 'INTERMEDIATE',
            'hire_date': datetime(2020, 1, 15).date(),
            'shift_preference': 'DAY',
            'department': 'MACHINING',
            'hourly_rate': 25.50
        }
    
    @pytest.fixture
    def sample_operator(self, sample_operator_data):
        """Create a sample Operator instance."""
        operator = Operator(**sample_operator_data)
        operator.status = 'ACTIVE'
        operator.created_at = datetime.utcnow()
        operator.updated_at = datetime.utcnow()
        return operator
    
    # Test create_operator method
    
    @pytest.mark.asyncio
    async def test_create_operator_success(self, operator_service, sample_operator_data, sample_operator):
        """Test successful operator creation."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        operator_service.operator_repository.create = AsyncMock(return_value=sample_operator)
        
        result = await operator_service.create_operator(sample_operator_data)
        
        assert result == sample_operator
        operator_service.operator_repository.get_by_id.assert_called_once_with('EMP001')
        operator_service.operator_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_operator_missing_required_field(self, operator_service):
        """Test operator creation with missing required field."""
        incomplete_data = {
            'operator_name': 'John Doe',
            'skill_level': 'INTERMEDIATE'
            # Missing emp_id
        }
        
        with pytest.raises(ValueError, match="Required field 'emp_id' is missing or empty"):
            await operator_service.create_operator(incomplete_data)
    
    @pytest.mark.asyncio
    async def test_create_operator_already_exists(self, operator_service, sample_operator_data, sample_operator):
        """Test operator creation when operator already exists."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=sample_operator)
        
        with pytest.raises(ValueError, match="Operator with ID 'EMP001' already exists"):
            await operator_service.create_operator(sample_operator_data)
    
    @pytest.mark.asyncio
    async def test_create_operator_invalid_skill_level(self, operator_service, sample_operator_data):
        """Test operator creation with invalid skill level."""
        sample_operator_data['skill_level'] = 'INVALID_SKILL'
        
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Skill level must be one of"):
            await operator_service.create_operator(sample_operator_data)
    
    @pytest.mark.asyncio
    async def test_create_operator_invalid_shift_preference(self, operator_service, sample_operator_data):
        """Test operator creation with invalid shift preference."""
        sample_operator_data['shift_preference'] = 'INVALID_SHIFT'
        
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Shift preference must be one of"):
            await operator_service.create_operator(sample_operator_data)
    
    @pytest.mark.asyncio
    async def test_create_operator_invalid_hourly_rate(self, operator_service, sample_operator_data):
        """Test operator creation with invalid hourly rate."""
        sample_operator_data['hourly_rate'] = -10.0  # Negative rate
        
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Hourly rate must be a positive number"):
            await operator_service.create_operator(sample_operator_data)
    
    # Test get_operator_by_id method
    
    @pytest.mark.asyncio
    async def test_get_operator_by_id_success(self, operator_service, sample_operator):
        """Test successful operator retrieval by ID."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=sample_operator)
        
        result = await operator_service.get_operator_by_id('EMP001')
        
        assert result == sample_operator
        operator_service.operator_repository.get_by_id.assert_called_once_with('EMP001')
    
    @pytest.mark.asyncio
    async def test_get_operator_by_id_with_relationships(self, operator_service, sample_operator):
        """Test operator retrieval with relationships."""
        operator_service.operator_repository.get_operator_by_id_with_relationships = AsyncMock(return_value=sample_operator)
        
        result = await operator_service.get_operator_by_id('EMP001', include_relationships=True)
        
        assert result == sample_operator
        operator_service.operator_repository.get_operator_by_id_with_relationships.assert_called_once_with('EMP001')
    
    @pytest.mark.asyncio
    async def test_get_operator_by_id_not_found(self, operator_service):
        """Test operator retrieval when operator not found."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await operator_service.get_operator_by_id('NONEXISTENT')
        
        assert result is None
    
    # Test update_operator method
    
    @pytest.mark.asyncio
    async def test_update_operator_success(self, operator_service, sample_operator):
        """Test successful operator update."""
        update_data = {'operator_name': 'Jane Doe', 'skill_level': 'ADVANCED'}
        updated_operator = MagicMock()
        updated_operator.emp_id = 'EMP001'
        updated_operator.operator_name = 'Jane Doe'
        updated_operator.skill_level = 'ADVANCED'
        
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=sample_operator)
        operator_service.operator_repository.update = AsyncMock(return_value=updated_operator)
        
        result = await operator_service.update_operator('EMP001', update_data)
        
        assert result == updated_operator
        operator_service.operator_repository.update.assert_called_once_with('EMP001', **update_data)
    
    @pytest.mark.asyncio
    async def test_update_operator_not_found(self, operator_service):
        """Test operator update when operator not found."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await operator_service.update_operator('NONEXISTENT', {'operator_name': 'New Name'})
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_operator_invalid_skill_level(self, operator_service, sample_operator):
        """Test operator update with invalid skill level."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=sample_operator)
        
        with pytest.raises(ValueError, match="Skill level must be one of"):
            await operator_service.update_operator('EMP001', {'skill_level': 'INVALID'})
    
    # Test get_operator_performance_analysis method
    
    @pytest.mark.asyncio
    async def test_get_operator_performance_analysis_success(self, operator_service, sample_operator):
        """Test successful operator performance analysis."""
        mock_performance_metrics = {
            'emp_id': 'EMP001',
            'performance_metrics': {
                'total_jobs': 50,
                'efficiency': 0.85,
                'productivity_per_hour': 12.5,
                'machines_operated': 2
            }
        }
        
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=sample_operator)
        operator_service.operator_repository.get_operator_performance_metrics = AsyncMock(return_value=mock_performance_metrics)
        
        result = await operator_service.get_operator_performance_analysis('EMP001', include_benchmarks=True)
        
        assert 'operator_info' in result
        assert 'performance_metrics' in result
        assert 'performance_insights' in result
        assert 'performance_benchmarks' in result
        assert result['operator_info']['emp_id'] == 'EMP001'
    
    @pytest.mark.asyncio
    async def test_get_operator_performance_analysis_operator_not_found(self, operator_service):
        """Test performance analysis when operator not found."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Operator EMP001 not found"):
            await operator_service.get_operator_performance_analysis('EMP001')
    
    # Test get_skill_level_analysis method
    
    @pytest.mark.asyncio
    async def test_get_skill_level_analysis_success(self, operator_service):
        """Test successful skill level analysis."""
        mock_skill_analysis = {
            'skill_levels': [
                {'skill_level': 'BEGINNER', 'operator_count': 5, 'efficiency': 0.65},
                {'skill_level': 'INTERMEDIATE', 'operator_count': 8, 'efficiency': 0.75},
                {'skill_level': 'ADVANCED', 'operator_count': 4, 'efficiency': 0.85},
                {'skill_level': 'EXPERT', 'operator_count': 2, 'efficiency': 0.90}
            ]
        }
        
        operator_service.operator_repository.get_operator_skill_analysis = AsyncMock(return_value=mock_skill_analysis)
        
        result = await operator_service.get_skill_level_analysis()
        
        assert 'skill_levels' in result
        assert 'insights' in result
        assert len(result['skill_levels']) == 4
    
    # Test get_top_performers method
    
    @pytest.mark.asyncio
    async def test_get_top_performers_success(self, operator_service):
        """Test successful top performers retrieval."""
        mock_top_performers = [
            {
                'emp_id': 'EMP001',
                'operator_name': 'John Doe',
                'skill_level': 'EXPERT',
                'efficiency': 0.92,
                'productivity_per_hour': 15.0
            },
            {
                'emp_id': 'EMP002',
                'operator_name': 'Jane Smith',
                'skill_level': 'ADVANCED',
                'efficiency': 0.88,
                'productivity_per_hour': 13.5
            }
        ]
        
        operator_service.operator_repository.get_top_performers = AsyncMock(return_value=mock_top_performers)
        
        result = await operator_service.get_top_performers('productivity', limit=5)
        
        assert 'top_performers' in result
        assert 'insights' in result
        assert result['metric'] == 'productivity'
        assert len(result['top_performers']) == 2
    
    @pytest.mark.asyncio
    async def test_get_top_performers_invalid_metric(self, operator_service):
        """Test top performers with invalid metric."""
        with pytest.raises(ValueError, match="Metric must be one of"):
            await operator_service.get_top_performers('invalid_metric')
    
    # Test get_operators_by_skill_level method
    
    @pytest.mark.asyncio
    async def test_get_operators_by_skill_level_success(self, operator_service):
        """Test successful operators retrieval by skill level."""
        mock_operators = [MagicMock(), MagicMock()]
        
        operator_service.operator_repository.get_operators_by_skill_level = AsyncMock(return_value=mock_operators)
        
        result = await operator_service.get_operators_by_skill_level('INTERMEDIATE')
        
        assert result == mock_operators
        operator_service.operator_repository.get_operators_by_skill_level.assert_called_once_with('INTERMEDIATE')
    
    @pytest.mark.asyncio
    async def test_get_operators_by_skill_level_invalid(self, operator_service):
        """Test operators retrieval with invalid skill level."""
        with pytest.raises(ValueError, match="Skill level must be one of"):
            await operator_service.get_operators_by_skill_level('INVALID')
    
    # Test recommend_skill_development method
    
    @pytest.mark.asyncio
    async def test_recommend_skill_development_success(self, operator_service, sample_operator):
        """Test successful skill development recommendations."""
        mock_performance_metrics = {
            'performance_metrics': {
                'efficiency': 0.70,
                'machines_operated': 1,
                'total_jobs': 75
            }
        }
        
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=sample_operator)
        operator_service.operator_repository.get_operator_performance_metrics = AsyncMock(return_value=mock_performance_metrics)
        
        result = await operator_service.recommend_skill_development('EMP001')
        
        assert 'current_skill_level' in result
        assert 'recommended_next_level' in result
        assert 'development_areas' in result
        assert 'training_recommendations' in result
        assert result['current_skill_level'] == 'INTERMEDIATE'
    
    @pytest.mark.asyncio
    async def test_recommend_skill_development_operator_not_found(self, operator_service):
        """Test skill development recommendations when operator not found."""
        operator_service.operator_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Operator EMP001 not found"):
            await operator_service.recommend_skill_development('EMP001')
    
    # Test private helper methods
    
    def test_generate_performance_insights_excellent(self, operator_service, sample_operator):
        """Test performance insights generation for excellent performance."""
        performance_metrics = {
            'performance_metrics': {
                'efficiency': 0.90,
                'productivity_per_hour': 15.0,
                'machines_operated': 3
            }
        }
        
        insights = operator_service._generate_performance_insights(performance_metrics, sample_operator)
        
        assert insights['overall_assessment'] == 'Excellent'
        assert 'High efficiency and productivity' in insights['strengths']
        assert 'High machine versatility' in insights['strengths']
    
    def test_generate_performance_insights_needs_improvement(self, operator_service, sample_operator):
        """Test performance insights generation for performance needing improvement."""
        performance_metrics = {
            'performance_metrics': {
                'efficiency': 0.55,
                'productivity_per_hour': 5.0,
                'machines_operated': 1
            }
        }
        
        insights = operator_service._generate_performance_insights(performance_metrics, sample_operator)
        
        assert insights['overall_assessment'] == 'Needs Improvement'
        assert 'Efficiency below target' in insights['improvement_areas']
        assert 'Limited to single machine operation' in insights['improvement_areas']
        assert len(insights['recommendations']) > 0
    
    def test_generate_skill_level_insights_high_expertise(self, operator_service):
        """Test skill level insights with high expertise distribution."""
        skill_analysis = {
            'skill_levels': [
                {'skill_level': 'BEGINNER', 'operator_count': 2, 'efficiency': 0.60},
                {'skill_level': 'INTERMEDIATE', 'operator_count': 3, 'efficiency': 0.70},
                {'skill_level': 'ADVANCED', 'operator_count': 3, 'efficiency': 0.80},
                {'skill_level': 'EXPERT', 'operator_count': 4, 'efficiency': 0.90}
            ]
        }
        
        insights = operator_service._generate_skill_level_insights(skill_analysis)
        
        assert insights['skill_distribution'] == 'High expertise level'
        assert insights['performance_correlation'] == 'Strong correlation between skill and performance'
    
    def test_generate_skill_level_insights_limited_expertise(self, operator_service):
        """Test skill level insights with limited expertise."""
        skill_analysis = {
            'skill_levels': [
                {'skill_level': 'BEGINNER', 'operator_count': 8, 'efficiency': 0.60},
                {'skill_level': 'INTERMEDIATE', 'operator_count': 4, 'efficiency': 0.65},
                {'skill_level': 'ADVANCED', 'operator_count': 2, 'efficiency': 0.70},
                {'skill_level': 'EXPERT', 'operator_count': 1, 'efficiency': 0.72}
            ]
        }
        
        insights = operator_service._generate_skill_level_insights(skill_analysis)
        
        assert insights['skill_distribution'] == 'Limited expertise available'
        assert 'Invest in advanced skill development programs' in insights['recommendations']
        assert 'High proportion of beginners' in insights['recommendations'][1]
    
    def test_generate_top_performer_insights_large_gap(self, operator_service):
        """Test top performer insights with large performance gap."""
        top_performers = [
            {'productivity_per_hour': 20.0, 'skill_level': 'EXPERT', 'department': 'MACHINING'},
            {'productivity_per_hour': 15.0, 'skill_level': 'ADVANCED', 'department': 'MACHINING'},
            {'productivity_per_hour': 10.0, 'skill_level': 'INTERMEDIATE', 'department': 'ASSEMBLY'}
        ]
        
        insights = operator_service._generate_top_performer_insights(top_performers, 'productivity')
        
        assert insights['performance_gap'] == 'Large performance gap - significant improvement opportunity'
        assert 'Analyze top performer practices' in insights['recommendations'][0]
        # Check that either skill level or department commonality is detected
        characteristics = ' '.join(insights['common_characteristics'])
        assert ('EXPERT skill level' in characteristics or 'MACHINING department' in characteristics)
    
    def test_generate_skill_recommendations_ready_for_promotion(self, operator_service):
        """Test skill recommendations for operator ready for promotion."""
        operator = Operator(emp_id='EMP001', operator_name='John Doe', skill_level='INTERMEDIATE')
        performance_metrics = {
            'performance_metrics': {
                'efficiency': 0.80,  # Above threshold
                'machines_operated': 3,  # Above threshold
                'total_jobs': 150  # Above threshold
            }
        }
        
        recommendations = operator_service._generate_skill_recommendations(operator, performance_metrics)
        
        assert recommendations['current_skill_level'] == 'INTERMEDIATE'
        assert recommendations['recommended_next_level'] == 'ADVANCED'
        assert 'Ready for promotion to ADVANCED level' in recommendations['training_recommendations']
    
    def test_generate_skill_recommendations_needs_development(self, operator_service):
        """Test skill recommendations for operator needing development."""
        operator = Operator(emp_id='EMP001', operator_name='John Doe', skill_level='BEGINNER')
        performance_metrics = {
            'performance_metrics': {
                'efficiency': 0.55,  # Below threshold
                'machines_operated': 1,  # At threshold
                'total_jobs': 30  # Below threshold
            }
        }
        
        recommendations = operator_service._generate_skill_recommendations(operator, performance_metrics)
        
        assert recommendations['current_skill_level'] == 'BEGINNER'
        assert recommendations['recommended_next_level'] == 'INTERMEDIATE'
        assert 'Improve operational efficiency' in recommendations['development_areas']
        assert 'Gain more operational experience' in recommendations['development_areas']
    
    def test_get_performance_benchmarks_skill_based(self, operator_service):
        """Test performance benchmarks based on skill level."""
        benchmarks = operator_service._get_performance_benchmarks('EXPERT')
        
        assert benchmarks['skill_level'] == 'EXPERT'
        assert benchmarks['efficiency_target'] == 0.85
        assert benchmarks['productivity_target'] == 15.0
        assert benchmarks['machine_versatility_target'] == 4
    
    def test_get_performance_benchmarks_no_skill(self, operator_service):
        """Test performance benchmarks with no skill level."""
        benchmarks = operator_service._get_performance_benchmarks(None)
        
        assert 'skill_level' not in benchmarks
        assert benchmarks['efficiency_target'] == 0.75  # Default
        assert benchmarks['source'] == 'Internal Standards'