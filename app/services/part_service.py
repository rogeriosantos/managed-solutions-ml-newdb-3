"""
Part Service Module

This module provides business logic for part operations, including
production history analysis and material-based insights.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from app.repositories.part_repository import PartRepository
from app.repositories.base_repository import PaginationParams, FilterCondition, FilterOperator
from app.models.database_models import Part

logger = logging.getLogger(__name__)


class PartService:
    """
    Service class for part-related business logic.
    
    Provides high-level operations for part management, production history analysis,
    and material-based insights with business rule validation.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the part service with a database session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
        self.part_repository = PartRepository(session)
    
    # Part CRUD operations with business logic
    
    async def create_part(self, part_data: Dict[str, Any]) -> Part:
        """
        Create a new part with validation.
        
        Args:
            part_data: Part data dictionary
            
        Returns:
            Part: Created part
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Validate required fields
            required_fields = ['part_number', 'part_name']
            for field in required_fields:
                if field not in part_data or not part_data[field]:
                    raise ValueError(f"Required field '{field}' is missing or empty")
            
            # Check if part already exists
            existing_part = await self.part_repository.get_by_id(part_data['part_number'])
            if existing_part:
                raise ValueError(f"Part with number '{part_data['part_number']}' already exists")
            
            # Validate numeric fields
            numeric_fields = ['weight', 'dimensions_length', 'dimensions_width', 'dimensions_height',
                            'standard_cycle_time', 'setup_time_standard', 'cost_per_unit']
            
            for field in numeric_fields:
                if field in part_data and part_data[field] is not None:
                    if not isinstance(part_data[field], (int, float)) or part_data[field] < 0:
                        raise ValueError(f"Field '{field}' must be a non-negative number")
            
            # Validate dimensions consistency
            dimensions = ['dimensions_length', 'dimensions_width', 'dimensions_height']
            dimension_values = [part_data.get(dim) for dim in dimensions if part_data.get(dim) is not None]
            
            if len(dimension_values) > 0 and len(dimension_values) < 3:
                logger.warning(f"Part {part_data['part_number']} has incomplete dimension data")
            
            # Validate material type
            if 'material_type' in part_data and part_data['material_type']:
                # Common material types validation (could be expanded)
                common_materials = [
                    'Steel', 'Aluminum', 'Brass', 'Copper', 'Plastic', 'Titanium',
                    'Stainless Steel', 'Carbon Steel', 'Cast Iron', 'Bronze'
                ]
                if part_data['material_type'] not in common_materials:
                    logger.info(f"Uncommon material type specified: {part_data['material_type']}")
            
            # Set default values
            part_data.setdefault('created_at', datetime.utcnow())
            part_data.setdefault('updated_at', datetime.utcnow())
            
            part = await self.part_repository.create(**part_data)
            
            logger.info(f"Created part: {part.part_number} - {part.part_name}")
            return part
            
        except Exception as e:
            logger.error(f"Failed to create part: {e}")
            raise
    
    async def get_part_by_number(self, part_number: str, include_relationships: bool = False) -> Optional[Part]:
        """
        Get part by number with optional relationships.
        
        Args:
            part_number: Part number identifier
            include_relationships: Whether to include job log relationships
            
        Returns:
            Optional[Part]: Part if found, None otherwise
        """
        try:
            if include_relationships:
                part = await self.part_repository.get_part_by_number_with_relationships(part_number)
            else:
                part = await self.part_repository.get_by_id(part_number)
            
            if part:
                logger.debug(f"Retrieved part: {part_number}")
            else:
                logger.warning(f"Part not found: {part_number}")
            
            return part
            
        except Exception as e:
            logger.error(f"Failed to get part {part_number}: {e}")
            raise
    
    async def get_parts_by_material(self, 
                                   material_type: str,
                                   material_hardness: Optional[str] = None) -> List[Part]:
        """
        Get parts filtered by material properties.
        
        Args:
            material_type: Material type filter
            material_hardness: Optional material hardness filter
            
        Returns:
            List[Part]: List of parts matching material criteria
        """
        try:
            if material_hardness:
                # Get parts by both material type and hardness
                parts_by_type = await self.part_repository.get_parts_by_material_type(material_type)
                parts = [part for part in parts_by_type if part.material_hardness == material_hardness]
            else:
                parts = await self.part_repository.get_parts_by_material_type(material_type)
            
            logger.debug(f"Retrieved {len(parts)} parts with material type {material_type}")
            return parts
            
        except Exception as e:
            logger.error(f"Failed to get parts by material {material_type}: {e}")
            raise
    
    async def update_part(self, part_number: str, update_data: Dict[str, Any]) -> Optional[Part]:
        """
        Update part with validation.
        
        Args:
            part_number: Part number identifier
            update_data: Fields to update
            
        Returns:
            Optional[Part]: Updated part if found, None otherwise
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Check if part exists
            existing_part = await self.part_repository.get_by_id(part_number)
            if not existing_part:
                return None
            
            # Validate numeric fields if present
            numeric_fields = ['weight', 'dimensions_length', 'dimensions_width', 'dimensions_height',
                            'standard_cycle_time', 'setup_time_standard', 'cost_per_unit']
            
            for field in numeric_fields:
                if field in update_data and update_data[field] is not None:
                    if not isinstance(update_data[field], (int, float)) or update_data[field] < 0:
                        raise ValueError(f"Field '{field}' must be a non-negative number")
            
            updated_part = await self.part_repository.update(part_number, **update_data)
            
            logger.info(f"Updated part: {part_number}")
            return updated_part
            
        except Exception as e:
            logger.error(f"Failed to update part {part_number}: {e}")
            raise
    
    # Production history and analysis methods
    
    async def get_part_production_analysis(self,
                                         part_number: str,
                                         start_date: Optional[datetime] = None,
                                         end_date: Optional[datetime] = None,
                                         include_details: bool = False) -> Dict[str, Any]:
        """
        Get comprehensive production analysis for a part.
        
        Args:
            part_number: Part number identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            include_details: Whether to include detailed production records
            
        Returns:
            Dict[str, Any]: Production analysis with insights
        """
        try:
            # Validate part exists
            part = await self.part_repository.get_by_id(part_number)
            if not part:
                raise ValueError(f"Part {part_number} not found")
            
            # Set default date range if not provided (last 90 days)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=90)
            
            # Get pagination for details if requested
            pagination = PaginationParams(skip=0, limit=50) if include_details else None
            
            # Get production history
            production_history = await self.part_repository.get_part_production_history(
                part_number, start_date, end_date, pagination
            )
            
            # Generate production insights
            insights = self._generate_production_insights(production_history, part)
            production_history['production_insights'] = insights
            
            # Add cost analysis
            cost_analysis = self._calculate_cost_analysis(production_history, part)
            production_history['cost_analysis'] = cost_analysis
            
            logger.debug(f"Generated production analysis for part {part_number}")
            return production_history
            
        except Exception as e:
            logger.error(f"Failed to get production analysis for part {part_number}: {e}")
            raise
    
    async def get_material_type_analysis(self,
                                       start_date: Optional[datetime] = None,
                                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Analyze production performance by material type.
        
        Args:
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Material type analysis with insights
        """
        try:
            # Set default date range if not provided (last 90 days)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=90)
            
            # Get material analysis
            material_analysis = await self.part_repository.get_material_analysis(start_date, end_date)
            
            # Generate insights
            insights = self._generate_material_insights(material_analysis)
            material_analysis['insights'] = insights
            
            logger.debug("Generated material type analysis")
            return material_analysis
            
        except Exception as e:
            logger.error(f"Failed to get material type analysis: {e}")
            raise
    
    async def get_part_complexity_analysis(self) -> Dict[str, Any]:
        """
        Analyze parts by complexity metrics with business insights.
        
        Returns:
            Dict[str, Any]: Part complexity analysis with insights
        """
        try:
            # Get complexity analysis
            complexity_analysis = await self.part_repository.get_part_complexity_analysis()
            
            # Generate insights
            insights = self._generate_complexity_insights(complexity_analysis)
            complexity_analysis['insights'] = insights
            
            logger.debug("Generated part complexity analysis")
            return complexity_analysis
            
        except Exception as e:
            logger.error(f"Failed to get part complexity analysis: {e}")
            raise
    
    async def search_parts_by_specifications(self,
                                           material_type: Optional[str] = None,
                                           tolerance_class: Optional[str] = None,
                                           min_weight: Optional[float] = None,
                                           max_weight: Optional[float] = None,
                                           dimension_constraints: Optional[Dict[str, float]] = None) -> List[Part]:
        """
        Search parts by various specifications with validation.
        
        Args:
            material_type: Material type filter
            tolerance_class: Tolerance class filter
            min_weight: Minimum weight constraint
            max_weight: Maximum weight constraint
            dimension_constraints: Dictionary with min/max dimension constraints
            
        Returns:
            List[Part]: List of parts matching specifications
        """
        try:
            parts = []
            
            # Start with material type filter if provided
            if material_type:
                parts = await self.part_repository.get_parts_by_material_type(material_type)
            else:
                parts = await self.part_repository.get_all()
            
            # Apply additional filters
            if tolerance_class:
                tolerance_parts = await self.part_repository.get_parts_by_tolerance_class(tolerance_class)
                tolerance_part_numbers = {part.part_number for part in tolerance_parts}
                parts = [part for part in parts if part.part_number in tolerance_part_numbers]
            
            # Apply weight constraints
            if min_weight is not None or max_weight is not None:
                filtered_parts = []
                for part in parts:
                    if part.weight is not None:
                        if min_weight is not None and part.weight < min_weight:
                            continue
                        if max_weight is not None and part.weight > max_weight:
                            continue
                    filtered_parts.append(part)
                parts = filtered_parts
            
            # Apply dimension constraints
            if dimension_constraints:
                dimension_parts = await self.part_repository.search_parts_by_dimensions(
                    min_length=dimension_constraints.get('min_length'),
                    max_length=dimension_constraints.get('max_length'),
                    min_width=dimension_constraints.get('min_width'),
                    max_width=dimension_constraints.get('max_width'),
                    min_height=dimension_constraints.get('min_height'),
                    max_height=dimension_constraints.get('max_height')
                )
                dimension_part_numbers = {part.part_number for part in dimension_parts}
                parts = [part for part in parts if part.part_number in dimension_part_numbers]
            
            logger.debug(f"Found {len(parts)} parts matching specifications")
            return parts
            
        except Exception as e:
            logger.error(f"Failed to search parts by specifications: {e}")
            raise
    
    async def get_part_recommendations(self, part_number: str) -> Dict[str, Any]:
        """
        Get optimization recommendations for a part based on production history.
        
        Args:
            part_number: Part number identifier
            
        Returns:
            Dict[str, Any]: Part optimization recommendations
        """
        try:
            # Get part and production analysis
            part = await self.part_repository.get_by_id(part_number)
            if not part:
                raise ValueError(f"Part {part_number} not found")
            
            # Get production history for the last 6 months
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=180)
            
            production_history = await self.part_repository.get_part_production_history(
                part_number, start_date, end_date
            )
            
            # Generate recommendations
            recommendations = self._generate_part_recommendations(part, production_history)
            
            logger.debug(f"Generated recommendations for part {part_number}")
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to get recommendations for part {part_number}: {e}")
            raise
    
    # Private helper methods
    
    def _generate_production_insights(self, 
                                    production_history: Dict[str, Any], 
                                    part: Part) -> Dict[str, Any]:
        """
        Generate production insights from history data.
        
        Args:
            production_history: Production history data
            part: Part entity
            
        Returns:
            Dict[str, Any]: Production insights
        """
        insights = {
            'production_performance': 'Unknown',
            'cycle_time_analysis': 'Unknown',
            'machine_efficiency': 'Unknown',
            'recommendations': []
        }
        
        try:
            summary = production_history.get('production_summary', {})
            
            if not summary:
                return insights
            
            total_operations = summary.get('total_operations', 0)
            efficiency = summary.get('efficiency', 0)
            actual_cycle_time = summary.get('actual_cycle_time', 0)
            cycle_time_variance = summary.get('cycle_time_variance_percentage', 0)
            
            # Production performance assessment
            if total_operations >= 100:
                insights['production_performance'] = 'High volume production'
            elif total_operations >= 20:
                insights['production_performance'] = 'Regular production'
            elif total_operations >= 5:
                insights['production_performance'] = 'Low volume production'
            else:
                insights['production_performance'] = 'Minimal production history'
                insights['recommendations'].append('Insufficient data for reliable analysis')
            
            # Cycle time analysis
            if part.standard_cycle_time and actual_cycle_time > 0:
                if abs(cycle_time_variance) <= 10:
                    insights['cycle_time_analysis'] = 'Cycle time meets standards'
                elif cycle_time_variance > 20:
                    insights['cycle_time_analysis'] = 'Cycle time significantly above standard'
                    insights['recommendations'].append('Investigate causes of extended cycle times')
                elif cycle_time_variance < -20:
                    insights['cycle_time_analysis'] = 'Cycle time better than standard'
                    insights['recommendations'].append('Consider updating standard cycle time')
                else:
                    insights['cycle_time_analysis'] = 'Cycle time slightly off standard'
            else:
                insights['cycle_time_analysis'] = 'No standard cycle time for comparison'
                if actual_cycle_time > 0:
                    insights['recommendations'].append('Consider establishing standard cycle time')
            
            # Machine efficiency
            machines_used = summary.get('machines_used', 0)
            if efficiency >= 0.85:
                insights['machine_efficiency'] = 'Excellent efficiency across machines'
            elif efficiency >= 0.70:
                insights['machine_efficiency'] = 'Good efficiency'
            elif efficiency >= 0.50:
                insights['machine_efficiency'] = 'Moderate efficiency - room for improvement'
                insights['recommendations'].append('Analyze machine-specific performance variations')
            else:
                insights['machine_efficiency'] = 'Poor efficiency - needs attention'
                insights['recommendations'].append('Urgent efficiency improvement needed')
            
            if machines_used > 3:
                insights['recommendations'].append('Part produced on multiple machines - consider standardization')
            
        except Exception as e:
            logger.warning(f"Error generating production insights: {e}")
        
        return insights
    
    def _calculate_cost_analysis(self, 
                               production_history: Dict[str, Any], 
                               part: Part) -> Dict[str, Any]:
        """
        Calculate cost analysis for part production.
        
        Args:
            production_history: Production history data
            part: Part entity
            
        Returns:
            Dict[str, Any]: Cost analysis
        """
        cost_analysis = {
            'cost_per_unit': part.cost_per_unit,
            'total_production_value': 0,
            'cost_efficiency': 'Unknown',
            'recommendations': []
        }
        
        try:
            summary = production_history.get('production_summary', {})
            total_parts_produced = summary.get('total_parts_produced', 0)
            total_running_time = summary.get('total_running_time', 0)
            
            if part.cost_per_unit and total_parts_produced > 0:
                cost_analysis['total_production_value'] = part.cost_per_unit * total_parts_produced
                
                # Calculate cost per hour
                if total_running_time > 0:
                    cost_per_hour = (part.cost_per_unit * total_parts_produced) / (total_running_time / 3600)
                    cost_analysis['cost_per_production_hour'] = cost_per_hour
                    
                    # Cost efficiency assessment (arbitrary thresholds - would be industry specific)
                    if cost_per_hour >= 100:
                        cost_analysis['cost_efficiency'] = 'High value production'
                    elif cost_per_hour >= 50:
                        cost_analysis['cost_efficiency'] = 'Moderate value production'
                    else:
                        cost_analysis['cost_efficiency'] = 'Low value production'
                        cost_analysis['recommendations'].append('Consider cost optimization opportunities')
            
            # Analyze cost trends if we have machine performance data
            machine_performance = production_history.get('machine_performance', [])
            if len(machine_performance) > 1:
                # Find most and least efficient machines by productivity
                sorted_machines = sorted(machine_performance, key=lambda x: x.get('productivity_per_hour', 0), reverse=True)
                
                if len(sorted_machines) >= 2:
                    best_machine = sorted_machines[0]
                    worst_machine = sorted_machines[-1]
                    
                    productivity_gap = best_machine.get('productivity_per_hour', 0) / max(worst_machine.get('productivity_per_hour', 1), 1)
                    
                    if productivity_gap >= 2.0:
                        cost_analysis['recommendations'].append(
                            f"Significant productivity difference between machines - "
                            f"focus production on {best_machine.get('machine')} for cost efficiency"
                        )
                        
        except Exception as e:
            logger.warning(f"Error calculating cost analysis: {e}")
        
        return cost_analysis
    
    def _generate_material_insights(self, material_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights from material type analysis.
        
        Args:
            material_analysis: Material analysis data
            
        Returns:
            Dict[str, Any]: Material insights
        """
        insights = {
            'material_distribution': 'Unknown',
            'performance_by_material': [],
            'recommendations': []
        }
        
        try:
            material_types = material_analysis.get('material_types', [])
            
            if not material_types:
                return insights
            
            # Analyze material distribution
            total_parts = sum(mat['total_parts_produced'] for mat in material_types)
            
            # Find dominant materials
            dominant_materials = [mat for mat in material_types if mat['total_parts_produced'] / total_parts >= 0.3]
            
            if len(dominant_materials) == 1:
                insights['material_distribution'] = f"Dominated by {dominant_materials[0]['material_type']}"
            elif len(dominant_materials) >= 2:
                insights['material_distribution'] = "Balanced material mix"
            else:
                insights['material_distribution'] = "Diverse material portfolio"
            
            # Performance analysis by material
            for material in material_types:
                material_type = material['material_type']
                efficiency = material.get('efficiency', 0)
                productivity = material.get('productivity_per_hour', 0)
                
                performance_rating = 'Unknown'
                if efficiency >= 0.80 and productivity > 0:
                    performance_rating = 'High Performance'
                elif efficiency >= 0.65:
                    performance_rating = 'Good Performance'
                else:
                    performance_rating = 'Needs Improvement'
                
                insights['performance_by_material'].append({
                    'material_type': material_type,
                    'performance_rating': performance_rating,
                    'efficiency': efficiency,
                    'productivity': productivity
                })
            
            # Generate recommendations
            sorted_materials = sorted(material_types, key=lambda x: x.get('efficiency', 0), reverse=True)
            
            if len(sorted_materials) >= 2:
                best_material = sorted_materials[0]
                worst_material = sorted_materials[-1]
                
                efficiency_gap = best_material.get('efficiency', 0) - worst_material.get('efficiency', 0)
                
                if efficiency_gap >= 0.2:
                    insights['recommendations'].append(
                        f"Significant efficiency difference between materials - "
                        f"{best_material['material_type']} performs much better than {worst_material['material_type']}"
                    )
                    insights['recommendations'].append("Consider material-specific process optimization")
            
            # Check for low-performing materials
            low_performers = [mat for mat in material_types if mat.get('efficiency', 0) < 0.6]
            if low_performers:
                material_names = [mat['material_type'] for mat in low_performers]
                insights['recommendations'].append(
                    f"Low efficiency materials detected: {', '.join(material_names)} - investigate tooling and parameters"
                )
                
        except Exception as e:
            logger.warning(f"Error generating material insights: {e}")
        
        return insights
    
    def _generate_complexity_insights(self, complexity_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights from part complexity analysis.
        
        Args:
            complexity_analysis: Complexity analysis data
            
        Returns:
            Dict[str, Any]: Complexity insights
        """
        insights = {
            'precision_distribution': 'Unknown',
            'complexity_impact': 'Unknown',
            'recommendations': []
        }
        
        try:
            precision_dist = complexity_analysis.get('precision_distribution', [])
            size_dist = complexity_analysis.get('size_distribution', [])
            
            # Analyze precision distribution
            if precision_dist:
                total_precision_parts = sum(p['part_count'] for p in precision_dist)
                high_precision = sum(p['part_count'] for p in precision_dist if p['precision_category'] == 'High Precision')
                
                high_precision_ratio = high_precision / total_precision_parts if total_precision_parts > 0 else 0
                
                if high_precision_ratio >= 0.4:
                    insights['precision_distribution'] = 'High precision manufacturing focus'
                    insights['recommendations'].append('Specialized high-precision capabilities are a competitive advantage')
                elif high_precision_ratio >= 0.2:
                    insights['precision_distribution'] = 'Mixed precision requirements'
                else:
                    insights['precision_distribution'] = 'Standard precision manufacturing'
                
                # Analyze cycle time impact of precision
                high_prec_cycle = next((p['avg_cycle_time'] for p in precision_dist if p['precision_category'] == 'High Precision'), 0)
                low_prec_cycle = next((p['avg_cycle_time'] for p in precision_dist if p['precision_category'] == 'Low Precision'), 0)
                
                if high_prec_cycle > 0 and low_prec_cycle > 0:
                    cycle_ratio = high_prec_cycle / low_prec_cycle
                    if cycle_ratio >= 2.0:
                        insights['complexity_impact'] = 'High precision significantly increases cycle time'
                        insights['recommendations'].append('Consider precision-based pricing and scheduling')
                    else:
                        insights['complexity_impact'] = 'Moderate precision impact on cycle time'
            
            # Analyze size distribution impact
            if size_dist:
                large_parts = [s for s in size_dist if s['size_category'] == 'Large']
                small_parts = [s for s in size_dist if s['size_category'] == 'Small']
                
                if large_parts and small_parts:
                    large_cycle = large_parts[0].get('avg_cycle_time', 0)
                    small_cycle = small_parts[0].get('avg_cycle_time', 0)
                    
                    if large_cycle > 0 and small_cycle > 0:
                        size_cycle_ratio = large_cycle / small_cycle
                        if size_cycle_ratio >= 3.0:
                            insights['recommendations'].append('Large parts require significantly more time - optimize scheduling')
                        
        except Exception as e:
            logger.warning(f"Error generating complexity insights: {e}")
        
        return insights
    
    def _generate_part_recommendations(self, 
                                     part: Part, 
                                     production_history: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate optimization recommendations for a part.
        
        Args:
            part: Part entity
            production_history: Production history data
            
        Returns:
            Dict[str, Any]: Part recommendations
        """
        recommendations = {
            'part_number': part.part_number,
            'part_name': part.part_name,
            'optimization_opportunities': [],
            'process_improvements': [],
            'cost_optimization': [],
            'quality_enhancements': [],
            'priority_level': 'Low'
        }
        
        try:
            summary = production_history.get('production_summary', {})
            
            if not summary:
                recommendations['optimization_opportunities'].append('Insufficient production data for analysis')
                return recommendations
            
            efficiency = summary.get('efficiency', 0)
            cycle_time_variance = summary.get('cycle_time_variance_percentage', 0)
            machines_used = summary.get('machines_used', 0)
            total_operations = summary.get('total_operations', 0)
            
            # Determine priority level
            if efficiency < 0.6 or abs(cycle_time_variance) > 30:
                recommendations['priority_level'] = 'High'
            elif efficiency < 0.75 or abs(cycle_time_variance) > 15:
                recommendations['priority_level'] = 'Medium'
            
            # Process improvement recommendations
            if efficiency < 0.70:
                recommendations['process_improvements'].append('Low efficiency detected - analyze setup and operation procedures')
                recommendations['process_improvements'].append('Consider operator training and process standardization')
            
            if cycle_time_variance > 20:
                recommendations['process_improvements'].append('High cycle time variance - investigate process consistency')
                recommendations['process_improvements'].append('Review tooling and fixture standardization')
            elif cycle_time_variance < -20:
                recommendations['process_improvements'].append('Cycle time better than standard - update standard time')
            
            if machines_used > 3:
                recommendations['process_improvements'].append('Part produced on multiple machines - consider process standardization')
                recommendations['optimization_opportunities'].append('Evaluate machine-specific performance and optimize allocation')
            
            # Cost optimization
            if part.cost_per_unit:
                if part.cost_per_unit > 100:  # High-value part
                    recommendations['cost_optimization'].append('High-value part - focus on yield optimization and waste reduction')
                
                machine_performance = production_history.get('machine_performance', [])
                if len(machine_performance) > 1:
                    # Find most efficient machine
                    best_machine = max(machine_performance, key=lambda x: x.get('productivity_per_hour', 0))
                    recommendations['cost_optimization'].append(
                        f"Consider prioritizing production on {best_machine.get('machine')} for cost efficiency"
                    )
            
            # Quality enhancements
            if part.tolerance_class and 'IT' in part.tolerance_class:
                tolerance_num = int(part.tolerance_class.replace('IT', ''))
                if tolerance_num <= 8:  # High precision
                    recommendations['quality_enhancements'].append('High precision part - implement statistical process control')
                    recommendations['quality_enhancements'].append('Consider dedicated tooling and environmental controls')
            
            if part.material_type in ['Titanium', 'Stainless Steel']:
                recommendations['quality_enhancements'].append('Difficult-to-machine material - optimize cutting parameters and tool selection')
            
            # Volume-based recommendations
            if total_operations >= 50:
                recommendations['optimization_opportunities'].append('High volume part - consider automation opportunities')
                recommendations['optimization_opportunities'].append('Evaluate dedicated tooling and fixtures')
            elif total_operations < 10:
                recommendations['optimization_opportunities'].append('Low volume part - consider batch processing optimization')
            
            # Material-specific recommendations
            if part.material_hardness:
                if 'hard' in part.material_hardness.lower():
                    recommendations['process_improvements'].append('Hard material - optimize tool selection and cutting parameters')
                    
        except Exception as e:
            logger.warning(f"Error generating part recommendations: {e}")
        
        return recommendations