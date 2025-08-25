"""
Machine Service Module

This module provides business logic for machine operations, including
CRUD operations, data aggregation, downtime analysis, and OEE calculations.
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from app.repositories.machine_repository import MachineRepository
from app.repositories.base_repository import PaginationParams, PaginatedResult, FilterCondition, FilterOperator
from app.models.database_models import Machine, JobLogOB

logger = logging.getLogger(__name__)


class MachineService:
    """
    Service class for machine-related business logic.
    
    Provides high-level operations for machine management, data aggregation,
    downtime analysis, and OEE calculations with business rule validation.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the machine service with a database session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
        self.machine_repository = MachineRepository(session)
    
    # Machine CRUD operations with business logic
    
    async def create_machine(self, machine_data: Dict[str, Any]) -> Machine:
        """
        Create a new machine with validation.
        
        Args:
            machine_data: Machine data dictionary
            
        Returns:
            Machine: Created machine
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Validate required fields
            required_fields = ['machine_id', 'machine_name', 'machine_type']
            for field in required_fields:
                if field not in machine_data or not machine_data[field]:
                    raise ValueError(f"Required field '{field}' is missing or empty")
            
            # Check if machine already exists
            existing_machine = await self.machine_repository.get_by_id(machine_data['machine_id'])
            if existing_machine:
                raise ValueError(f"Machine with ID '{machine_data['machine_id']}' already exists")
            
            # Set default values
            machine_data.setdefault('status', 'ACTIVE')
            machine_data.setdefault('created_at', datetime.utcnow())
            machine_data.setdefault('updated_at', datetime.utcnow())
            
            # Validate numeric fields
            numeric_fields = ['year_installed', 'max_spindle_speed', 'max_feed_rate', 
                            'work_envelope_x', 'work_envelope_y', 'work_envelope_z', 
                            'maintenance_schedule_hours']
            
            for field in numeric_fields:
                if field in machine_data and machine_data[field] is not None:
                    if not isinstance(machine_data[field], (int, float)) or machine_data[field] < 0:
                        raise ValueError(f"Field '{field}' must be a positive number")
            
            machine = await self.machine_repository.create(**machine_data)
            
            logger.info(f"Created machine: {machine.machine_id} - {machine.machine_name}")
            return machine
            
        except Exception as e:
            logger.error(f"Failed to create machine: {e}")
            raise
    
    async def get_machine_by_id(self, machine_id: str, include_relationships: bool = False) -> Optional[Machine]:
        """
        Get machine by ID with optional relationships.
        
        Args:
            machine_id: Machine identifier
            include_relationships: Whether to include job log relationships
            
        Returns:
            Optional[Machine]: Machine if found, None otherwise
        """
        try:
            if include_relationships:
                machine = await self.machine_repository.get_machine_by_id_with_relationships(machine_id)
            else:
                machine = await self.machine_repository.get_by_id(machine_id)
            
            if machine:
                logger.debug(f"Retrieved machine: {machine_id}")
            else:
                logger.warning(f"Machine not found: {machine_id}")
            
            return machine
            
        except Exception as e:
            logger.error(f"Failed to get machine {machine_id}: {e}")
            raise
    
    async def get_all_machines(self, 
                              active_only: bool = True,
                              machine_type: Optional[str] = None) -> List[Machine]:
        """
        Get all machines with optional filtering.
        
        Args:
            active_only: Whether to return only active machines
            machine_type: Optional machine type filter
            
        Returns:
            List[Machine]: List of machines
        """
        try:
            filters = []
            
            if active_only:
                filters.append(FilterCondition("status", FilterOperator.EQ, "ACTIVE"))
            
            if machine_type:
                filters.append(FilterCondition("machine_type", FilterOperator.EQ, machine_type))
            
            machines = await self.machine_repository.get_all(filters=filters, order_by="machine_name")
            
            logger.debug(f"Retrieved {len(machines)} machines (active_only={active_only})")
            return machines
            
        except Exception as e:
            logger.error(f"Failed to get all machines: {e}")
            raise
    
    async def update_machine(self, machine_id: str, update_data: Dict[str, Any]) -> Optional[Machine]:
        """
        Update machine with validation.
        
        Args:
            machine_id: Machine identifier
            update_data: Fields to update
            
        Returns:
            Optional[Machine]: Updated machine if found, None otherwise
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Check if machine exists
            existing_machine = await self.machine_repository.get_by_id(machine_id)
            if not existing_machine:
                return None
            
            # Validate numeric fields if present
            numeric_fields = ['year_installed', 'max_spindle_speed', 'max_feed_rate', 
                            'work_envelope_x', 'work_envelope_y', 'work_envelope_z', 
                            'maintenance_schedule_hours']
            
            for field in numeric_fields:
                if field in update_data and update_data[field] is not None:
                    if not isinstance(update_data[field], (int, float)) or update_data[field] < 0:
                        raise ValueError(f"Field '{field}' must be a positive number")
            
            # Validate status if present
            if 'status' in update_data:
                valid_statuses = ['ACTIVE', 'INACTIVE', 'MAINTENANCE', 'RETIRED']
                if update_data['status'] not in valid_statuses:
                    raise ValueError(f"Status must be one of: {valid_statuses}")
            
            updated_machine = await self.machine_repository.update(machine_id, **update_data)
            
            logger.info(f"Updated machine: {machine_id}")
            return updated_machine
            
        except Exception as e:
            logger.error(f"Failed to update machine {machine_id}: {e}")
            raise
    
    async def delete_machine(self, machine_id: str) -> bool:
        """
        Delete machine (soft delete by setting status to RETIRED).
        
        Args:
            machine_id: Machine identifier
            
        Returns:
            bool: True if machine was deleted, False if not found
        """
        try:
            # Soft delete by updating status
            updated_machine = await self.machine_repository.update(
                machine_id, 
                status='RETIRED',
                updated_at=datetime.utcnow()
            )
            
            if updated_machine:
                logger.info(f"Soft deleted machine: {machine_id}")
                return True
            else:
                logger.warning(f"Machine not found for deletion: {machine_id}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete machine {machine_id}: {e}")
            raise
    
    # Machine data aggregation and filtering
    
    async def get_machine_data(self,
                              machine_id: str,
                              start_date: Optional[datetime] = None,
                              end_date: Optional[datetime] = None,
                              pagination: Optional[PaginationParams] = None,
                              include_relationships: bool = True) -> Union[List[JobLogOB], PaginatedResult]:
        """
        Get machine operational data with filtering and pagination.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            pagination: Pagination parameters (optional)
            include_relationships: Whether to include related entities
            
        Returns:
            Union[List[JobLogOB], PaginatedResult]: Job logs or paginated results
            
        Raises:
            ValueError: If machine not found
        """
        try:
            # Validate machine exists
            machine = await self.machine_repository.get_by_id(machine_id)
            if not machine:
                raise ValueError(f"Machine {machine_id} not found")
            
            # Validate date range
            if start_date and end_date and start_date > end_date:
                raise ValueError("Start date must be before end date")
            
            # Set default date range if not provided (last 30 days)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=30)
            
            job_logs = await self.machine_repository.get_machine_job_logs(
                machine_id=machine_id,
                start_date=start_date,
                end_date=end_date,
                pagination=pagination
            )
            
            logger.debug(f"Retrieved machine data for {machine_id}: "
                        f"{len(job_logs) if isinstance(job_logs, list) else job_logs.total_count} records")
            
            return job_logs
            
        except Exception as e:
            logger.error(f"Failed to get machine data for {machine_id}: {e}")
            raise
    
    async def get_machine_summary_statistics(self,
                                           machine_id: str,
                                           start_date: Optional[datetime] = None,
                                           end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get comprehensive summary statistics for a machine.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Summary statistics with business insights
        """
        try:
            # Validate machine exists
            machine = await self.machine_repository.get_by_id(machine_id)
            if not machine:
                raise ValueError(f"Machine {machine_id} not found")
            
            # Get performance statistics
            performance_stats = await self.machine_repository.get_machine_performance_statistics(
                machine_id, start_date, end_date
            )
            
            # Get downtime summary
            downtime_summary = await self.machine_repository.get_machine_downtime_summary(
                machine_id, start_date, end_date
            )
            
            # Calculate business insights
            insights = self._generate_machine_insights(performance_stats, downtime_summary)
            
            summary = {
                'machine_info': {
                    'machine_id': machine.machine_id,
                    'machine_name': machine.machine_name,
                    'machine_type': machine.machine_type,
                    'status': machine.status
                },
                'performance_statistics': performance_stats,
                'downtime_summary': downtime_summary,
                'business_insights': insights
            }
            
            logger.debug(f"Generated summary statistics for machine {machine_id}")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get summary statistics for machine {machine_id}: {e}")
            raise
    
    # Downtime analysis methods
    
    async def analyze_machine_downtime(self,
                                     machine_id: str,
                                     start_date: Optional[datetime] = None,
                                     end_date: Optional[datetime] = None,
                                     include_trends: bool = True) -> Dict[str, Any]:
        """
        Perform comprehensive downtime analysis for a machine.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            include_trends: Whether to include trend analysis
            
        Returns:
            Dict[str, Any]: Comprehensive downtime analysis
        """
        try:
            # Validate machine exists
            machine = await self.machine_repository.get_by_id(machine_id)
            if not machine:
                raise ValueError(f"Machine {machine_id} not found")
            
            # Set default date range if not provided (last 90 days for trends)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=90)
            
            # Get downtime summary
            downtime_summary = await self.machine_repository.get_machine_downtime_summary(
                machine_id, start_date, end_date
            )
            
            analysis = {
                'machine_id': machine_id,
                'analysis_period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'downtime_summary': downtime_summary,
                'downtime_insights': self._analyze_downtime_patterns(downtime_summary)
            }
            
            # Add trend analysis if requested
            if include_trends:
                trends = await self.machine_repository.get_downtime_trends(
                    machine_id, start_date, end_date, interval='daily'
                )
                analysis['downtime_trends'] = trends
                analysis['trend_insights'] = self._analyze_downtime_trends(trends)
            
            logger.debug(f"Completed downtime analysis for machine {machine_id}")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze downtime for machine {machine_id}: {e}")
            raise
    
    def _analyze_downtime_patterns(self, downtime_summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze downtime patterns and provide insights.
        
        Args:
            downtime_summary: Downtime summary data
            
        Returns:
            Dict[str, Any]: Downtime pattern insights
        """
        insights = {
            'primary_downtime_causes': [],
            'recommendations': [],
            'severity_assessment': 'Unknown'
        }
        
        try:
            downtime_breakdown = downtime_summary.get('downtime_breakdown', {})
            total_downtime = sum(downtime_breakdown.values())
            
            if total_downtime == 0:
                insights['severity_assessment'] = 'Excellent'
                insights['recommendations'].append('Maintain current operational practices')
                return insights
            
            # Identify primary causes (>10% of total downtime)
            primary_causes = []
            for cause, time_value in downtime_breakdown.items():
                if time_value > 0:
                    percentage = (time_value / total_downtime) * 100
                    if percentage >= 10:
                        primary_causes.append({
                            'cause': cause.replace('_', ' ').title(),
                            'time': time_value,
                            'percentage': percentage
                        })
            
            # Sort by percentage descending
            primary_causes.sort(key=lambda x: x['percentage'], reverse=True)
            insights['primary_downtime_causes'] = primary_causes
            
            # Generate recommendations based on primary causes
            recommendations = []
            for cause in primary_causes[:3]:  # Top 3 causes
                cause_name = cause['cause'].lower()
                if 'setup' in cause_name:
                    recommendations.append('Consider setup time reduction initiatives and operator training')
                elif 'maintenance' in cause_name:
                    recommendations.append('Review preventive maintenance schedule and procedures')
                elif 'tooling' in cause_name:
                    recommendations.append('Optimize tool management and pre-staging processes')
                elif 'adjustment' in cause_name:
                    recommendations.append('Investigate process stability and quality control measures')
                elif 'idle' in cause_name:
                    recommendations.append('Analyze scheduling efficiency and material flow')
            
            insights['recommendations'] = recommendations
            
            # Assess severity based on efficiency metrics
            efficiency_metrics = downtime_summary.get('efficiency_metrics', {})
            overall_efficiency = efficiency_metrics.get('overall_efficiency', 0)
            
            if overall_efficiency >= 0.85:
                insights['severity_assessment'] = 'Good'
            elif overall_efficiency >= 0.70:
                insights['severity_assessment'] = 'Moderate'
            elif overall_efficiency >= 0.50:
                insights['severity_assessment'] = 'Poor'
            else:
                insights['severity_assessment'] = 'Critical'
                
        except Exception as e:
            logger.warning(f"Error analyzing downtime patterns: {e}")
        
        return insights
    
    def _analyze_downtime_trends(self, trends: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze downtime trends over time.
        
        Args:
            trends: List of trend data points
            
        Returns:
            Dict[str, Any]: Trend analysis insights
        """
        insights = {
            'trend_direction': 'Stable',
            'efficiency_trend': 'Stable',
            'recommendations': []
        }
        
        try:
            if len(trends) < 2:
                return insights
            
            # Calculate trend direction for downtime
            recent_downtime = sum(point['total_downtime'] for point in trends[-7:])  # Last 7 days
            earlier_downtime = sum(point['total_downtime'] for point in trends[-14:-7])  # Previous 7 days
            
            if recent_downtime > earlier_downtime * 1.1:
                insights['trend_direction'] = 'Increasing'
                insights['recommendations'].append('Downtime is trending upward - investigate recent changes')
            elif recent_downtime < earlier_downtime * 0.9:
                insights['trend_direction'] = 'Decreasing'
                insights['recommendations'].append('Downtime improvements detected - document successful practices')
            
            # Calculate efficiency trend
            recent_efficiency = sum(point['efficiency'] for point in trends[-7:]) / min(7, len(trends[-7:]))
            earlier_efficiency = sum(point['efficiency'] for point in trends[-14:-7]) / min(7, len(trends[-14:-7]))
            
            if recent_efficiency > earlier_efficiency * 1.05:
                insights['efficiency_trend'] = 'Improving'
            elif recent_efficiency < earlier_efficiency * 0.95:
                insights['efficiency_trend'] = 'Declining'
                insights['recommendations'].append('Efficiency is declining - review recent operational changes')
                
        except Exception as e:
            logger.warning(f"Error analyzing downtime trends: {e}")
        
        return insights
    
    # OEE calculation methods
    
    async def calculate_machine_oee(self,
                                  machine_id: str,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None,
                                  include_benchmarks: bool = True) -> Dict[str, Any]:
        """
        Calculate Overall Equipment Effectiveness (OEE) with business insights.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            include_benchmarks: Whether to include industry benchmarks
            
        Returns:
            Dict[str, Any]: OEE metrics with business insights
        """
        try:
            # Validate machine exists
            machine = await self.machine_repository.get_by_id(machine_id)
            if not machine:
                raise ValueError(f"Machine {machine_id} not found")
            
            # Calculate OEE
            oee_metrics = await self.machine_repository.calculate_machine_oee(
                machine_id, start_date, end_date
            )
            
            # Add business insights
            oee_insights = self._generate_oee_insights(oee_metrics, machine)
            oee_metrics['business_insights'] = oee_insights
            
            # Add industry benchmarks if requested
            if include_benchmarks:
                oee_metrics['industry_benchmarks'] = self._get_industry_benchmarks(machine.machine_type)
            
            logger.debug(f"Calculated OEE for machine {machine_id}: {oee_metrics['oee_score']:.3f}")
            return oee_metrics
            
        except Exception as e:
            logger.error(f"Failed to calculate OEE for machine {machine_id}: {e}")
            raise
    
    def _generate_oee_insights(self, oee_metrics: Dict[str, Any], machine: Machine) -> Dict[str, Any]:
        """
        Generate business insights from OEE metrics.
        
        Args:
            oee_metrics: OEE calculation results
            machine: Machine entity
            
        Returns:
            Dict[str, Any]: Business insights and recommendations
        """
        insights = {
            'performance_assessment': 'Unknown',
            'improvement_opportunities': [],
            'priority_actions': [],
            'estimated_improvement_potential': {}
        }
        
        try:
            oee_components = oee_metrics.get('oee_components', {})
            availability = oee_components.get('availability', 0)
            performance = oee_components.get('performance', 0)
            quality = oee_components.get('quality', 0)
            oee_score = oee_metrics.get('oee_score', 0)
            
            # Performance assessment
            classification = oee_metrics.get('classification', {})
            insights['performance_assessment'] = classification.get('level', 'Unknown')
            
            # Identify improvement opportunities
            opportunities = []
            if availability < 0.90:
                opportunities.append({
                    'area': 'Availability',
                    'current': availability,
                    'target': 0.90,
                    'potential_gain': (0.90 - availability) * performance * quality,
                    'focus': 'Reduce downtime and improve maintenance efficiency'
                })
            
            if performance < 0.95:
                opportunities.append({
                    'area': 'Performance',
                    'current': performance,
                    'target': 0.95,
                    'potential_gain': availability * (0.95 - performance) * quality,
                    'focus': 'Optimize cycle times and reduce minor stops'
                })
            
            if quality < 0.99:
                opportunities.append({
                    'area': 'Quality',
                    'current': quality,
                    'target': 0.99,
                    'potential_gain': availability * performance * (0.99 - quality),
                    'focus': 'Improve first-pass quality and reduce rework'
                })
            
            # Sort by potential gain
            opportunities.sort(key=lambda x: x['potential_gain'], reverse=True)
            insights['improvement_opportunities'] = opportunities
            
            # Generate priority actions
            priority_actions = []
            if opportunities:
                top_opportunity = opportunities[0]
                area = top_opportunity['area']
                
                if area == 'Availability':
                    priority_actions.extend([
                        'Implement predictive maintenance program',
                        'Reduce setup and changeover times',
                        'Improve spare parts inventory management'
                    ])
                elif area == 'Performance':
                    priority_actions.extend([
                        'Optimize machine parameters and speeds',
                        'Reduce minor stops and micro-downtime',
                        'Improve operator training and procedures'
                    ])
                elif area == 'Quality':
                    priority_actions.extend([
                        'Implement statistical process control',
                        'Improve tooling and fixture quality',
                        'Enhance quality inspection procedures'
                    ])
            
            insights['priority_actions'] = priority_actions
            
            # Calculate improvement potential
            if opportunities:
                total_potential = sum(opp['potential_gain'] for opp in opportunities)
                insights['estimated_improvement_potential'] = {
                    'current_oee': oee_score,
                    'potential_oee': min(oee_score + total_potential, 1.0),
                    'improvement_points': total_potential,
                    'improvement_percentage': (total_potential / max(oee_score, 0.01)) * 100
                }
                
        except Exception as e:
            logger.warning(f"Error generating OEE insights: {e}")
        
        return insights
    
    def _get_industry_benchmarks(self, machine_type: str) -> Dict[str, Any]:
        """
        Get industry benchmarks for machine type.
        
        Args:
            machine_type: Type of machine
            
        Returns:
            Dict[str, Any]: Industry benchmark data
        """
        # Default benchmarks (would typically come from industry data)
        benchmarks = {
            'world_class_oee': 0.85,
            'good_oee': 0.65,
            'average_oee': 0.60,
            'availability_target': 0.90,
            'performance_target': 0.95,
            'quality_target': 0.99,
            'source': 'Industry Standards',
            'machine_type': machine_type
        }
        
        # Adjust benchmarks based on machine type
        machine_type_lower = machine_type.lower()
        if 'cnc' in machine_type_lower or 'machining' in machine_type_lower:
            benchmarks.update({
                'world_class_oee': 0.80,
                'good_oee': 0.60,
                'average_oee': 0.55
            })
        elif 'assembly' in machine_type_lower:
            benchmarks.update({
                'world_class_oee': 0.90,
                'good_oee': 0.70,
                'average_oee': 0.65
            })
        
        return benchmarks
    
    def _generate_machine_insights(self, 
                                 performance_stats: Dict[str, Any], 
                                 downtime_summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate business insights from machine statistics.
        
        Args:
            performance_stats: Performance statistics
            downtime_summary: Downtime summary
            
        Returns:
            Dict[str, Any]: Business insights
        """
        insights = {
            'utilization_assessment': 'Unknown',
            'operator_efficiency': 'Unknown',
            'part_diversity': 'Unknown',
            'recommendations': []
        }
        
        try:
            stats = performance_stats.get('statistics', {})
            efficiency_metrics = downtime_summary.get('efficiency_metrics', {})
            
            # Utilization assessment
            total_jobs = stats.get('total_jobs', 0)
            if total_jobs > 100:
                insights['utilization_assessment'] = 'High'
            elif total_jobs > 50:
                insights['utilization_assessment'] = 'Moderate'
            else:
                insights['utilization_assessment'] = 'Low'
                insights['recommendations'].append('Consider increasing machine utilization')
            
            # Operator efficiency
            unique_operators = stats.get('unique_operators', 0)
            if unique_operators > 5:
                insights['operator_efficiency'] = 'Multiple operators - ensure consistent training'
                insights['recommendations'].append('Standardize operating procedures across operators')
            elif unique_operators > 0:
                insights['operator_efficiency'] = f'{unique_operators} operators - good consistency'
            
            # Part diversity
            unique_parts = stats.get('unique_parts', 0)
            if unique_parts > 20:
                insights['part_diversity'] = 'High diversity - complex scheduling'
                insights['recommendations'].append('Consider part family grouping for setup optimization')
            elif unique_parts > 5:
                insights['part_diversity'] = 'Moderate diversity - manageable complexity'
            else:
                insights['part_diversity'] = 'Low diversity - specialized production'
                
        except Exception as e:
            logger.warning(f"Error generating machine insights: {e}")
        
        return insights