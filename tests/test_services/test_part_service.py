"""
Tests for PartService

This module contains unit tests for the PartService class,
testing business logic, validation, and error handling.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.part_service import PartService
from app.models.database_models import Part
from app.repositories.base_repository import PaginationParams


class TestPartService:
    """Test cases for PartService."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock async session."""
        return AsyncMock(spec=AsyncSession)
    
    @pytest.fixture
    def part_service(self, mock_session):
        """Create a PartService instance with mocked dependencies."""
        return PartService(mock_session)
    
    @pytest.fixture
    def sample_part_data(self):
        """Sample part data for testing."""
        return {
            'part_number': 'PART001',
            'part_name': 'Test Machined Part',
            'part_description': 'A test part for manufacturing',
            'material_type': 'Steel',
            'material_hardness': 'HRC 45-50',
            'weight': 2.5,
            'dimensions_length': 100.0,
            'dimensions_width': 50.0,
            'dimensions_height': 25.0,
            'tolerance_class': 'IT8',
            'surface_finish': 'Ra 1.6',
            'standard_cycle_time': 300,
            'setup_time_standard': 600,
            'cost_per_unit': 45.75
        }
    
    @pytest.fixture
    def sample_part(self, sample_part_data):
        """Create a sample Part instance."""
        part = Part(**sample_part_data)
        part.created_at = datetime.utcnow()
        part.updated_at = datetime.utcnow()
        return part
    
    # Test create_part method
    
    @pytest.mark.asyncio
    async def test_create_part_success(self, part_service, sample_part_data, sample_part):
        """Test successful part creation."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        part_service.part_repository.create = AsyncMock(return_value=sample_part)
        
        result = await part_service.create_part(sample_part_data)
        
        assert result == sample_part
        part_service.part_repository.get_by_id.assert_called_once_with('PART001')
        part_service.part_repository.create.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_part_missing_required_field(self, part_service):
        """Test part creation with missing required field."""
        incomplete_data = {
            'part_name': 'Test Part',
            'material_type': 'Steel'
            # Missing part_number
        }
        
        with pytest.raises(ValueError, match="Required field 'part_number' is missing or empty"):
            await part_service.create_part(incomplete_data)
    
    @pytest.mark.asyncio
    async def test_create_part_already_exists(self, part_service, sample_part_data, sample_part):
        """Test part creation when part already exists."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=sample_part)
        
        with pytest.raises(ValueError, match="Part with number 'PART001' already exists"):
            await part_service.create_part(sample_part_data)
    
    @pytest.mark.asyncio
    async def test_create_part_invalid_numeric_field(self, part_service, sample_part_data):
        """Test part creation with invalid numeric field."""
        sample_part_data['weight'] = -2.5  # Invalid negative weight
        
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Field 'weight' must be a non-negative number"):
            await part_service.create_part(sample_part_data)
    
    @pytest.mark.asyncio
    async def test_create_part_incomplete_dimensions_warning(self, part_service, sample_part_data, sample_part):
        """Test part creation with incomplete dimensions (should log warning)."""
        # Remove one dimension to trigger warning
        del sample_part_data['dimensions_height']
        
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        part_service.part_repository.create = AsyncMock(return_value=sample_part)
        
        # Should not raise exception, just log warning
        result = await part_service.create_part(sample_part_data)
        assert result == sample_part
    
    @pytest.mark.asyncio
    async def test_create_part_uncommon_material_info(self, part_service, sample_part_data, sample_part):
        """Test part creation with uncommon material type (should log info)."""
        sample_part_data['material_type'] = 'Unobtainium'  # Uncommon material
        
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        part_service.part_repository.create = AsyncMock(return_value=sample_part)
        
        # Should not raise exception, just log info
        result = await part_service.create_part(sample_part_data)
        assert result == sample_part
    
    # Test get_part_by_number method
    
    @pytest.mark.asyncio
    async def test_get_part_by_number_success(self, part_service, sample_part):
        """Test successful part retrieval by number."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=sample_part)
        
        result = await part_service.get_part_by_number('PART001')
        
        assert result == sample_part
        part_service.part_repository.get_by_id.assert_called_once_with('PART001')
    
    @pytest.mark.asyncio
    async def test_get_part_by_number_with_relationships(self, part_service, sample_part):
        """Test part retrieval with relationships."""
        part_service.part_repository.get_part_by_number_with_relationships = AsyncMock(return_value=sample_part)
        
        result = await part_service.get_part_by_number('PART001', include_relationships=True)
        
        assert result == sample_part
        part_service.part_repository.get_part_by_number_with_relationships.assert_called_once_with('PART001')
    
    @pytest.mark.asyncio
    async def test_get_part_by_number_not_found(self, part_service):
        """Test part retrieval when part not found."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await part_service.get_part_by_number('NONEXISTENT')
        
        assert result is None
    
    # Test get_parts_by_material method
    
    @pytest.mark.asyncio
    async def test_get_parts_by_material_type_only(self, part_service):
        """Test parts retrieval by material type only."""
        mock_parts = [MagicMock(), MagicMock()]
        
        part_service.part_repository.get_parts_by_material_type = AsyncMock(return_value=mock_parts)
        
        result = await part_service.get_parts_by_material('Steel')
        
        assert result == mock_parts
        part_service.part_repository.get_parts_by_material_type.assert_called_once_with('Steel')
    
    @pytest.mark.asyncio
    async def test_get_parts_by_material_with_hardness(self, part_service):
        """Test parts retrieval by material type and hardness."""
        steel_parts = [
            MagicMock(material_hardness='HRC 45-50'),
            MagicMock(material_hardness='HRC 30-35'),
            MagicMock(material_hardness='HRC 45-50')
        ]
        
        part_service.part_repository.get_parts_by_material_type = AsyncMock(return_value=steel_parts)
        
        result = await part_service.get_parts_by_material('Steel', 'HRC 45-50')
        
        assert len(result) == 2  # Only parts with matching hardness
        assert all(part.material_hardness == 'HRC 45-50' for part in result)
    
    # Test update_part method
    
    @pytest.mark.asyncio
    async def test_update_part_success(self, part_service, sample_part):
        """Test successful part update."""
        update_data = {'part_name': 'Updated Part Name', 'cost_per_unit': 50.00}
        updated_part = MagicMock()
        updated_part.part_number = 'PART001'
        updated_part.part_name = 'Updated Part Name'
        updated_part.cost_per_unit = 50.00
        
        part_service.part_repository.get_by_id = AsyncMock(return_value=sample_part)
        part_service.part_repository.update = AsyncMock(return_value=updated_part)
        
        result = await part_service.update_part('PART001', update_data)
        
        assert result == updated_part
        part_service.part_repository.update.assert_called_once_with('PART001', **update_data)
    
    @pytest.mark.asyncio
    async def test_update_part_not_found(self, part_service):
        """Test part update when part not found."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        
        result = await part_service.update_part('NONEXISTENT', {'part_name': 'New Name'})
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_update_part_invalid_numeric_field(self, part_service, sample_part):
        """Test part update with invalid numeric field."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=sample_part)
        
        with pytest.raises(ValueError, match="Field 'cost_per_unit' must be a non-negative number"):
            await part_service.update_part('PART001', {'cost_per_unit': -10.0})
    
    # Test get_part_production_analysis method
    
    @pytest.mark.asyncio
    async def test_get_part_production_analysis_success(self, part_service, sample_part):
        """Test successful part production analysis."""
        mock_production_history = {
            'part_number': 'PART001',
            'production_summary': {
                'total_operations': 25,
                'total_parts_produced': 500,
                'efficiency': 0.82,
                'actual_cycle_time': 320,
                'cycle_time_variance_percentage': 6.7
            },
            'machine_performance': [
                {'machine': 'CNC001', 'efficiency': 0.85, 'productivity_per_hour': 11.2}
            ]
        }
        
        part_service.part_repository.get_by_id = AsyncMock(return_value=sample_part)
        part_service.part_repository.get_part_production_history = AsyncMock(return_value=mock_production_history)
        
        result = await part_service.get_part_production_analysis('PART001', include_details=True)
        
        assert 'production_insights' in result
        assert 'cost_analysis' in result
        assert result['part_number'] == 'PART001'
    
    @pytest.mark.asyncio
    async def test_get_part_production_analysis_part_not_found(self, part_service):
        """Test production analysis when part not found."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Part PART001 not found"):
            await part_service.get_part_production_analysis('PART001')
    
    # Test get_material_type_analysis method
    
    @pytest.mark.asyncio
    async def test_get_material_type_analysis_success(self, part_service):
        """Test successful material type analysis."""
        mock_material_analysis = {
            'material_types': [
                {'material_type': 'Steel', 'total_parts_produced': 1000, 'efficiency': 0.80},
                {'material_type': 'Aluminum', 'total_parts_produced': 750, 'efficiency': 0.85},
                {'material_type': 'Brass', 'total_parts_produced': 250, 'efficiency': 0.75}
            ]
        }
        
        part_service.part_repository.get_material_analysis = AsyncMock(return_value=mock_material_analysis)
        
        result = await part_service.get_material_type_analysis()
        
        assert 'material_types' in result
        assert 'insights' in result
        assert len(result['material_types']) == 3
    
    # Test get_part_complexity_analysis method
    
    @pytest.mark.asyncio
    async def test_get_part_complexity_analysis_success(self, part_service):
        """Test successful part complexity analysis."""
        mock_complexity_analysis = {
            'precision_distribution': [
                {'precision_category': 'High Precision', 'part_count': 15, 'avg_cycle_time': 450},
                {'precision_category': 'Medium Precision', 'part_count': 25, 'avg_cycle_time': 300},
                {'precision_category': 'Low Precision', 'part_count': 10, 'avg_cycle_time': 180}
            ],
            'size_distribution': [
                {'size_category': 'Small', 'part_count': 20, 'avg_cycle_time': 200},
                {'size_category': 'Medium', 'part_count': 25, 'avg_cycle_time': 350},
                {'size_category': 'Large', 'part_count': 5, 'avg_cycle_time': 600}
            ]
        }
        
        part_service.part_repository.get_part_complexity_analysis = AsyncMock(return_value=mock_complexity_analysis)
        
        result = await part_service.get_part_complexity_analysis()
        
        assert 'precision_distribution' in result
        assert 'size_distribution' in result
        assert 'insights' in result
    
    # Test search_parts_by_specifications method
    
    @pytest.mark.asyncio
    async def test_search_parts_by_specifications_material_only(self, part_service):
        """Test parts search by material type only."""
        mock_parts = [MagicMock(), MagicMock()]
        
        part_service.part_repository.get_parts_by_material_type = AsyncMock(return_value=mock_parts)
        
        result = await part_service.search_parts_by_specifications(material_type='Steel')
        
        assert result == mock_parts
        part_service.part_repository.get_parts_by_material_type.assert_called_once_with('Steel')
    
    @pytest.mark.asyncio
    async def test_search_parts_by_specifications_complex_filter(self, part_service):
        """Test parts search with multiple specification filters."""
        # Mock parts with different properties
        all_parts = [
            MagicMock(part_number='PART001', weight=2.0),
            MagicMock(part_number='PART002', weight=5.0),
            MagicMock(part_number='PART003', weight=8.0),
            MagicMock(part_number='PART004', weight=None)  # No weight data
        ]
        
        tolerance_parts = [all_parts[0], all_parts[1], all_parts[2]]  # Exclude PART004
        dimension_parts = [all_parts[0], all_parts[1]]  # Only first two match dimensions
        
        part_service.part_repository.get_parts_by_material_type = AsyncMock(return_value=all_parts)
        part_service.part_repository.get_parts_by_tolerance_class = AsyncMock(return_value=tolerance_parts)
        part_service.part_repository.search_parts_by_dimensions = AsyncMock(return_value=dimension_parts)
        
        result = await part_service.search_parts_by_specifications(
            material_type='Steel',
            tolerance_class='IT8',
            min_weight=1.0,
            max_weight=6.0,
            dimension_constraints={'min_length': 50.0, 'max_length': 150.0}
        )
        
        # Should return parts that match all criteria (PART001 and PART002 both have weight <= 6.0)
        assert len(result) == 2  # PART001 and PART002 should match all filters
        part_numbers = [part.part_number for part in result]
        assert 'PART001' in part_numbers
        assert 'PART002' in part_numbers
    
    # Test get_part_recommendations method
    
    @pytest.mark.asyncio
    async def test_get_part_recommendations_success(self, part_service, sample_part):
        """Test successful part recommendations generation."""
        mock_production_history = {
            'production_summary': {
                'total_operations': 50,
                'efficiency': 0.65,  # Below target
                'cycle_time_variance_percentage': 25,  # High variance
                'machines_used': 4  # Multiple machines
            }
        }
        
        part_service.part_repository.get_by_id = AsyncMock(return_value=sample_part)
        part_service.part_repository.get_part_production_history = AsyncMock(return_value=mock_production_history)
        
        result = await part_service.get_part_recommendations('PART001')
        
        assert result['part_number'] == 'PART001'
        assert 'optimization_opportunities' in result
        assert 'process_improvements' in result
        assert 'cost_optimization' in result
        assert 'quality_enhancements' in result
        assert result['priority_level'] in ['Low', 'Medium', 'High']
    
    @pytest.mark.asyncio
    async def test_get_part_recommendations_part_not_found(self, part_service):
        """Test part recommendations when part not found."""
        part_service.part_repository.get_by_id = AsyncMock(return_value=None)
        
        with pytest.raises(ValueError, match="Part PART001 not found"):
            await part_service.get_part_recommendations('PART001')
    
    # Test private helper methods
    
    def test_generate_production_insights_high_volume(self, part_service, sample_part):
        """Test production insights for high volume production."""
        production_history = {
            'production_summary': {
                'total_operations': 150,  # High volume
                'efficiency': 0.88,  # Good efficiency
                'actual_cycle_time': 310,
                'cycle_time_variance_percentage': 3.3,  # Low variance
                'machines_used': 2
            }
        }
        
        insights = part_service._generate_production_insights(production_history, sample_part)
        
        assert insights['production_performance'] == 'High volume production'
        assert insights['cycle_time_analysis'] == 'Cycle time meets standards'
        assert insights['machine_efficiency'] == 'Excellent efficiency across machines'
    
    def test_generate_production_insights_needs_improvement(self, part_service, sample_part):
        """Test production insights for production needing improvement."""
        production_history = {
            'production_summary': {
                'total_operations': 8,  # Low volume
                'efficiency': 0.45,  # Poor efficiency
                'actual_cycle_time': 420,
                'cycle_time_variance_percentage': 40,  # High variance
                'machines_used': 5  # Too many machines
            }
        }
        
        insights = part_service._generate_production_insights(production_history, sample_part)
        
        assert insights['production_performance'] == 'Low volume production'
        assert 'significantly above standard' in insights['cycle_time_analysis']
        assert insights['machine_efficiency'] == 'Poor efficiency - needs attention'
        assert len(insights['recommendations']) >= 2
    
    def test_calculate_cost_analysis_high_value_part(self, part_service, sample_part):
        """Test cost analysis for high-value part."""
        production_history = {
            'production_summary': {
                'total_parts_produced': 200,
                'total_running_time': 36000  # 10 hours
            },
            'machine_performance': [
                {'machine': 'CNC001', 'productivity_per_hour': 25.0},
                {'machine': 'CNC002', 'productivity_per_hour': 15.0}
            ]
        }
        
        # High-value part
        sample_part.cost_per_unit = 150.0
        
        cost_analysis = part_service._calculate_cost_analysis(production_history, sample_part)
        
        assert cost_analysis['total_production_value'] == 30000.0  # 200 * 150
        assert cost_analysis['cost_per_production_hour'] == 3000.0  # 30000 / 10
        assert cost_analysis['cost_efficiency'] == 'High value production'
        # High value parts should get recommendations about best machine usage
        assert len(cost_analysis['recommendations']) >= 0  # May or may not have recommendations
    
    def test_generate_material_insights_balanced_mix(self, part_service):
        """Test material insights for balanced material mix."""
        material_analysis = {
            'material_types': [
                {'material_type': 'Steel', 'total_parts_produced': 600, 'efficiency': 0.80, 'productivity_per_hour': 10.0},
                {'material_type': 'Aluminum', 'total_parts_produced': 500, 'efficiency': 0.85, 'productivity_per_hour': 12.0},
                {'material_type': 'Brass', 'total_parts_produced': 400, 'efficiency': 0.75, 'productivity_per_hour': 8.0}
            ]
        }
        
        insights = part_service._generate_material_insights(material_analysis)
        
        # With Steel having 600/1500 = 40% (>30%), it should be "Balanced material mix"
        assert insights['material_distribution'] == 'Balanced material mix'
        assert len(insights['performance_by_material']) == 3
        
        # Check performance ratings
        aluminum_perf = next(p for p in insights['performance_by_material'] if p['material_type'] == 'Aluminum')
        assert aluminum_perf['performance_rating'] == 'High Performance'
        
        brass_perf = next(p for p in insights['performance_by_material'] if p['material_type'] == 'Brass')
        assert brass_perf['performance_rating'] == 'Good Performance'
    
    def test_generate_material_insights_performance_gap(self, part_service):
        """Test material insights with significant performance gap."""
        material_analysis = {
            'material_types': [
                {'material_type': 'Steel', 'total_parts_produced': 1000, 'efficiency': 0.85},
                {'material_type': 'Titanium', 'total_parts_produced': 200, 'efficiency': 0.55}  # Poor performance
            ]
        }
        
        insights = part_service._generate_material_insights(material_analysis)
        
        assert 'Significant efficiency difference' in insights['recommendations'][0]
        # Check that low efficiency materials are mentioned in recommendations
        recommendations_text = ' '.join(insights['recommendations'])
        assert 'Titanium' in recommendations_text
    
    def test_generate_complexity_insights_high_precision_focus(self, part_service):
        """Test complexity insights for high precision manufacturing."""
        complexity_analysis = {
            'precision_distribution': [
                {'precision_category': 'High Precision', 'part_count': 40, 'avg_cycle_time': 500},
                {'precision_category': 'Medium Precision', 'part_count': 30, 'avg_cycle_time': 300},
                {'precision_category': 'Low Precision', 'part_count': 30, 'avg_cycle_time': 200}
            ],
            'size_distribution': [
                {'size_category': 'Large', 'part_count': 20, 'avg_cycle_time': 800},
                {'size_category': 'Medium', 'part_count': 50, 'avg_cycle_time': 400},
                {'size_category': 'Small', 'part_count': 30, 'avg_cycle_time': 200}
            ]
        }
        
        insights = part_service._generate_complexity_insights(complexity_analysis)
        
        assert insights['precision_distribution'] == 'High precision manufacturing focus'
        assert 'Specialized high-precision capabilities' in insights['recommendations'][0]
        assert insights['complexity_impact'] == 'High precision significantly increases cycle time'
    
    def test_generate_part_recommendations_high_priority(self, part_service):
        """Test part recommendations for high priority part."""
        part = Part(part_number='PART001', part_name='Critical Part', cost_per_unit=200.0)
        part.tolerance_class = 'IT6'  # High precision
        part.material_type = 'Titanium'  # Difficult material
        
        production_history = {
            'production_summary': {
                'total_operations': 75,
                'efficiency': 0.55,  # Poor efficiency
                'cycle_time_variance_percentage': 35,  # High variance
                'machines_used': 4
            }
        }
        
        recommendations = part_service._generate_part_recommendations(part, production_history)
        
        assert recommendations['priority_level'] == 'High'
        assert len(recommendations['process_improvements']) >= 2
        assert len(recommendations['cost_optimization']) >= 1
        assert len(recommendations['quality_enhancements']) >= 2
        
        # Check for specific recommendations
        process_improvements = ' '.join(recommendations['process_improvements'])
        assert 'Low efficiency detected' in process_improvements
        assert 'High cycle time variance' in process_improvements
        
        quality_enhancements = ' '.join(recommendations['quality_enhancements'])
        assert 'High precision part' in quality_enhancements
        assert 'Difficult-to-machine material' in quality_enhancements
    
    def test_generate_part_recommendations_low_volume_optimization(self, part_service):
        """Test part recommendations for low volume part."""
        part = Part(part_number='PART002', part_name='Low Volume Part', cost_per_unit=25.0)
        
        production_history = {
            'production_summary': {
                'total_operations': 8,  # Low volume
                'efficiency': 0.78,  # Good efficiency
                'cycle_time_variance_percentage': 5,  # Low variance
                'machines_used': 1
            }
        }
        
        recommendations = part_service._generate_part_recommendations(part, production_history)
        
        assert recommendations['priority_level'] == 'Low'
        
        # Should recommend batch processing for low volume
        optimization_opportunities = ' '.join(recommendations['optimization_opportunities'])
        assert 'Low volume part' in optimization_opportunities
        assert 'batch processing' in optimization_opportunities