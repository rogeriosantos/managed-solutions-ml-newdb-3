"""
Operator Service Module

This module provides business logic for operator operations, including
performance analysis, skill management, and productivity tracking.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from app.repositories.operator_repository import OperatorRepository
from app.repositories.base_repository import PaginationParams, FilterCondition, FilterOperator
from app.models.database_models import Operator

logger = logging.getLogger(__name__)


class OperatorService:
    """
    Service class for operator-related business logic.
    
    Provides high-level operations for operator management, performance analysis,
    skill management, and productivity tracking with business rule validation.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the operator service with a database session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
        self.operator_repository = OperatorRepository(session)
    
    # Operator CRUD operations with business logic
    
    async def create_operator(self, operator_data: Dict[str, Any]) -> Operator:
        """
        Create a new operator with validation.
        
        Args:
            operator_data: Operator data dictionary
            
        Returns:
            Operator: Created operator
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Validate required fields
            required_fields = ['emp_id', 'operator_name']
            for field in required_fields:
                if field not in operator_data or not operator_data[field]:
                    raise ValueError(f"Required field '{field}' is missing or empty")
            
            # Check if operator already exists
            existing_operator = await self.operator_repository.get_by_id(operator_data['emp_id'])
            if existing_operator:
                raise ValueError(f"Operator with ID '{operator_data['emp_id']}' already exists")
            
            # Validate skill level
            if 'skill_level' in operator_data and operator_data['skill_level']:
                valid_skills = ['BEGINNER', 'INTERMEDIATE', 'ADVANCED', 'EXPERT']
                if operator_data['skill_level'].upper() not in valid_skills:
                    raise ValueError(f"Skill level must be one of: {valid_skills}")
                operator_data['skill_level'] = operator_data['skill_level'].upper()
            
            # Validate shift preference
            if 'shift_preference' in operator_data and operator_data['shift_preference']:
                valid_shifts = ['DAY', 'NIGHT', 'ROTATING']
                if operator_data['shift_preference'].upper() not in valid_shifts:
                    raise ValueError(f"Shift preference must be one of: {valid_shifts}")
                operator_data['shift_preference'] = operator_data['shift_preference'].upper()
            
            # Validate hourly rate
            if 'hourly_rate' in operator_data and operator_data['hourly_rate'] is not None:
                if not isinstance(operator_data['hourly_rate'], (int, float)) or operator_data['hourly_rate'] <= 0:
                    raise ValueError("Hourly rate must be a positive number")
            
            # Set default values
            operator_data.setdefault('status', 'ACTIVE')
            operator_data.setdefault('created_at', datetime.utcnow())
            operator_data.setdefault('updated_at', datetime.utcnow())
            
            operator = await self.operator_repository.create(**operator_data)
            
            logger.info(f"Created operator: {operator.emp_id} - {operator.operator_name}")
            return operator
            
        except Exception as e:
            logger.error(f"Failed to create operator: {e}")
            raise
    
    async def get_operator_by_id(self, emp_id: str, include_relationships: bool = False) -> Optional[Operator]:
        """
        Get operator by ID with optional relationships.
        
        Args:
            emp_id: Employee identifier
            include_relationships: Whether to include job log relationships
            
        Returns:
            Optional[Operator]: Operator if found, None otherwise
        """
        try:
            if include_relationships:
                operator = await self.operator_repository.get_operator_by_id_with_relationships(emp_id)
            else:
                operator = await self.operator_repository.get_by_id(emp_id)
            
            if operator:
                logger.debug(f"Retrieved operator: {emp_id}")
            else:
                logger.warning(f"Operator not found: {emp_id}")
            
            return operator
            
        except Exception as e:
            logger.error(f"Failed to get operator {emp_id}: {e}")
            raise
    
    async def get_all_operators(self, 
                               active_only: bool = True,
                               skill_level: Optional[str] = None,
                               department: Optional[str] = None) -> List[Operator]:
        """
        Get all operators with optional filtering.
        
        Args:
            active_only: Whether to return only active operators
            skill_level: Optional skill level filter
            department: Optional department filter
            
        Returns:
            List[Operator]: List of operators
        """
        try:
            filters = []
            
            if active_only:
                filters.append(FilterCondition("status", FilterOperator.EQ, "ACTIVE"))
            
            if skill_level:
                filters.append(FilterCondition("skill_level", FilterOperator.EQ, skill_level.upper()))
            
            if department:
                filters.append(FilterCondition("department", FilterOperator.EQ, department))
            
            operators = await self.operator_repository.get_all(filters=filters, order_by="operator_name")
            
            logger.debug(f"Retrieved {len(operators)} operators")
            return operators
            
        except Exception as e:
            logger.error(f"Failed to get all operators: {e}")
            raise
    
    async def update_operator(self, emp_id: str, update_data: Dict[str, Any]) -> Optional[Operator]:
        """
        Update operator with validation.
        
        Args:
            emp_id: Employee identifier
            update_data: Fields to update
            
        Returns:
            Optional[Operator]: Updated operator if found, None otherwise
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Check if operator exists
            existing_operator = await self.operator_repository.get_by_id(emp_id)
            if not existing_operator:
                return None
            
            # Validate skill level if present
            if 'skill_level' in update_data and update_data['skill_level']:
                valid_skills = ['BEGINNER', 'INTERMEDIATE', 'ADVANCED', 'EXPERT']
                if update_data['skill_level'].upper() not in valid_skills:
                    raise ValueError(f"Skill level must be one of: {valid_skills}")
                update_data['skill_level'] = update_data['skill_level'].upper()
            
            # Validate shift preference if present
            if 'shift_preference' in update_data and update_data['shift_preference']:
                valid_shifts = ['DAY', 'NIGHT', 'ROTATING']
                if update_data['shift_preference'].upper() not in valid_shifts:
                    raise ValueError(f"Shift preference must be one of: {valid_shifts}")
                update_data['shift_preference'] = update_data['shift_preference'].upper()
            
            # Validate status if present
            if 'status' in update_data:
                valid_statuses = ['ACTIVE', 'INACTIVE', 'TERMINATED']
                if update_data['status'] not in valid_statuses:
                    raise ValueError(f"Status must be one of: {valid_statuses}")
            
            # Validate hourly rate if present
            if 'hourly_rate' in update_data and update_data['hourly_rate'] is not None:
                if not isinstance(update_data['hourly_rate'], (int, float)) or update_data['hourly_rate'] <= 0:
                    raise ValueError("Hourly rate must be a positive number")
            
            updated_operator = await self.operator_repository.update(emp_id, **update_data)
            
            logger.info(f"Updated operator: {emp_id}")
            return updated_operator
            
        except Exception as e:
            logger.error(f"Failed to update operator {emp_id}: {e}")
            raise
    
    # Performance analysis methods
    
    async def get_operator_performance_analysis(self,
                                              emp_id: str,
                                              start_date: Optional[datetime] = None,
                                              end_date: Optional[datetime] = None,
                                              include_benchmarks: bool = True) -> Dict[str, Any]:
        """
        Get comprehensive performance analysis for an operator.
        
        Args:
            emp_id: Employee identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            include_benchmarks: Whether to include performance benchmarks
            
        Returns:
            Dict[str, Any]: Performance analysis with insights
        """
        try:
            # Validate operator exists
            operator = await self.operator_repository.get_by_id(emp_id)
            if not operator:
                raise ValueError(f"Operator {emp_id} not found")
            
            # Set default date range if not provided (last 30 days)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=30)
            
            # Get performance metrics
            performance_metrics = await self.operator_repository.get_operator_performance_metrics(
                emp_id, start_date, end_date
            )
            
            # Generate performance insights
            insights = self._generate_performance_insights(performance_metrics, operator)
            
            analysis = {
                'operator_info': {
                    'emp_id': operator.emp_id,
                    'operator_name': operator.operator_name,
                    'skill_level': operator.skill_level,
                    'department': operator.department,
                    'hire_date': operator.hire_date.isoformat() if operator.hire_date else None
                },
                'performance_metrics': performance_metrics,
                'performance_insights': insights
            }
            
            # Add benchmarks if requested
            if include_benchmarks:
                analysis['performance_benchmarks'] = self._get_performance_benchmarks(operator.skill_level)
            
            logger.debug(f"Generated performance analysis for operator {emp_id}")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to get performance analysis for operator {emp_id}: {e}")
            raise
    
    async def get_skill_level_analysis(self,
                                     start_date: Optional[datetime] = None,
                                     end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Analyze performance metrics across different skill levels.
        
        Args:
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Skill level analysis with insights
        """
        try:
            # Set default date range if not provided (last 90 days)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=90)
            
            # Get skill analysis
            skill_analysis = await self.operator_repository.get_operator_skill_analysis(
                start_date, end_date
            )
            
            # Generate insights
            insights = self._generate_skill_level_insights(skill_analysis)
            skill_analysis['insights'] = insights
            
            logger.debug("Generated skill level analysis")
            return skill_analysis
            
        except Exception as e:
            logger.error(f"Failed to get skill level analysis: {e}")
            raise
    
    async def get_top_performers(self,
                               metric: str = 'productivity',
                               limit: int = 10,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get top performing operators with analysis.
        
        Args:
            metric: Performance metric ('productivity', 'efficiency', 'parts_produced')
            limit: Number of top performers to return
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Top performers with analysis
        """
        try:
            # Validate metric
            valid_metrics = ['productivity', 'efficiency', 'parts_produced']
            if metric not in valid_metrics:
                raise ValueError(f"Metric must be one of: {valid_metrics}")
            
            # Set default date range if not provided (last 30 days)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=30)
            
            # Get top performers
            top_performers = await self.operator_repository.get_top_performers(
                metric, limit, start_date, end_date
            )
            
            # Generate insights
            insights = self._generate_top_performer_insights(top_performers, metric)
            
            analysis = {
                'metric': metric,
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'top_performers': top_performers,
                'insights': insights
            }
            
            logger.debug(f"Generated top performers analysis by {metric}")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to get top performers by {metric}: {e}")
            raise
    
    # Skill management methods
    
    async def get_operators_by_skill_level(self, skill_level: str) -> List[Operator]:
        """
        Get operators filtered by skill level with validation.
        
        Args:
            skill_level: Skill level (BEGINNER, INTERMEDIATE, ADVANCED, EXPERT)
            
        Returns:
            List[Operator]: List of operators with specified skill level
        """
        try:
            # Validate skill level
            valid_skills = ['BEGINNER', 'INTERMEDIATE', 'ADVANCED', 'EXPERT']
            if skill_level.upper() not in valid_skills:
                raise ValueError(f"Skill level must be one of: {valid_skills}")
            
            operators = await self.operator_repository.get_operators_by_skill_level(skill_level)
            
            logger.debug(f"Retrieved {len(operators)} operators with skill level {skill_level}")
            return operators
            
        except Exception as e:
            logger.error(f"Failed to get operators by skill level {skill_level}: {e}")
            raise
    
    async def recommend_skill_development(self, emp_id: str) -> Dict[str, Any]:
        """
        Recommend skill development opportunities for an operator.
        
        Args:
            emp_id: Employee identifier
            
        Returns:
            Dict[str, Any]: Skill development recommendations
        """
        try:
            # Get operator information
            operator = await self.operator_repository.get_by_id(emp_id)
            if not operator:
                raise ValueError(f"Operator {emp_id} not found")
            
            # Get performance metrics for the last 90 days
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=90)
            
            performance_metrics = await self.operator_repository.get_operator_performance_metrics(
                emp_id, start_date, end_date
            )
            
            # Generate recommendations
            recommendations = self._generate_skill_recommendations(operator, performance_metrics)
            
            logger.debug(f"Generated skill development recommendations for operator {emp_id}")
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to generate skill recommendations for operator {emp_id}: {e}")
            raise
    
    # Private helper methods
    
    def _generate_performance_insights(self, 
                                     performance_metrics: Dict[str, Any], 
                                     operator: Operator) -> Dict[str, Any]:
        """
        Generate performance insights from metrics.
        
        Args:
            performance_metrics: Performance metrics data
            operator: Operator entity
            
        Returns:
            Dict[str, Any]: Performance insights
        """
        insights = {
            'overall_assessment': 'Unknown',
            'strengths': [],
            'improvement_areas': [],
            'recommendations': []
        }
        
        try:
            metrics = performance_metrics.get('performance_metrics', {})
            
            if not metrics:
                return insights
            
            efficiency = metrics.get('efficiency', 0)
            productivity = metrics.get('productivity_per_hour', 0)
            machines_operated = metrics.get('machines_operated', 0)
            
            # Overall assessment
            if efficiency >= 0.85 and productivity > 0:
                insights['overall_assessment'] = 'Excellent'
                insights['strengths'].append('High efficiency and productivity')
            elif efficiency >= 0.70:
                insights['overall_assessment'] = 'Good'
                insights['strengths'].append('Good operational efficiency')
            elif efficiency >= 0.50:
                insights['overall_assessment'] = 'Needs Improvement'
                insights['improvement_areas'].append('Efficiency below target')
            else:
                insights['overall_assessment'] = 'Poor'
                insights['improvement_areas'].append('Significant efficiency issues')
            
            # Machine versatility
            if machines_operated >= 3:
                insights['strengths'].append('High machine versatility')
            elif machines_operated == 1:
                insights['improvement_areas'].append('Limited to single machine operation')
                insights['recommendations'].append('Consider cross-training on additional machines')
            
            # Skill level assessment
            if operator.skill_level:
                skill_benchmarks = {
                    'BEGINNER': {'min_efficiency': 0.60, 'target_machines': 1},
                    'INTERMEDIATE': {'min_efficiency': 0.70, 'target_machines': 2},
                    'ADVANCED': {'min_efficiency': 0.80, 'target_machines': 3},
                    'EXPERT': {'min_efficiency': 0.85, 'target_machines': 4}
                }
                
                benchmark = skill_benchmarks.get(operator.skill_level, {})
                min_efficiency = benchmark.get('min_efficiency', 0.70)
                target_machines = benchmark.get('target_machines', 2)
                
                if efficiency < min_efficiency:
                    insights['improvement_areas'].append(f'Efficiency below {operator.skill_level} level expectations')
                    insights['recommendations'].append('Focus on process optimization and training')
                
                if machines_operated < target_machines:
                    insights['recommendations'].append(f'Consider training on additional machines for {operator.skill_level} level')
                
        except Exception as e:
            logger.warning(f"Error generating performance insights: {e}")
        
        return insights
    
    def _generate_skill_level_insights(self, skill_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights from skill level analysis.
        
        Args:
            skill_analysis: Skill level analysis data
            
        Returns:
            Dict[str, Any]: Skill level insights
        """
        insights = {
            'skill_distribution': 'Unknown',
            'performance_correlation': 'Unknown',
            'recommendations': []
        }
        
        try:
            skill_levels = skill_analysis.get('skill_levels', [])
            
            if not skill_levels:
                return insights
            
            # Analyze skill distribution
            total_operators = sum(level['operator_count'] for level in skill_levels)
            expert_count = sum(level['operator_count'] for level in skill_levels if level['skill_level'] == 'EXPERT')
            beginner_count = sum(level['operator_count'] for level in skill_levels if level['skill_level'] == 'BEGINNER')
            
            expert_ratio = expert_count / total_operators if total_operators > 0 else 0
            beginner_ratio = beginner_count / total_operators if total_operators > 0 else 0
            
            if expert_ratio >= 0.3:
                insights['skill_distribution'] = 'High expertise level'
            elif expert_ratio >= 0.15:
                insights['skill_distribution'] = 'Balanced skill distribution'
            else:
                insights['skill_distribution'] = 'Limited expertise available'
                insights['recommendations'].append('Invest in advanced skill development programs')
            
            if beginner_ratio >= 0.4:
                insights['recommendations'].append('High proportion of beginners - prioritize training programs')
            
            # Analyze performance correlation
            skill_performance = {}
            for level in skill_levels:
                skill_performance[level['skill_level']] = level.get('efficiency', 0)
            
            if len(skill_performance) >= 2:
                expert_eff = skill_performance.get('EXPERT', 0)
                beginner_eff = skill_performance.get('BEGINNER', 0)
                
                if expert_eff > beginner_eff * 1.2:
                    insights['performance_correlation'] = 'Strong correlation between skill and performance'
                elif expert_eff > beginner_eff * 1.1:
                    insights['performance_correlation'] = 'Moderate correlation between skill and performance'
                else:
                    insights['performance_correlation'] = 'Weak correlation - investigate training effectiveness'
                    insights['recommendations'].append('Review training programs and skill assessment criteria')
                    
        except Exception as e:
            logger.warning(f"Error generating skill level insights: {e}")
        
        return insights
    
    def _generate_top_performer_insights(self, 
                                       top_performers: List[Dict[str, Any]], 
                                       metric: str) -> Dict[str, Any]:
        """
        Generate insights from top performer analysis.
        
        Args:
            top_performers: List of top performer data
            metric: Performance metric used
            
        Returns:
            Dict[str, Any]: Top performer insights
        """
        insights = {
            'performance_gap': 'Unknown',
            'common_characteristics': [],
            'recommendations': []
        }
        
        try:
            if len(top_performers) < 2:
                return insights
            
            # Analyze performance gap
            if metric == 'productivity':
                top_value = top_performers[0].get('productivity_per_hour', 0)
                bottom_value = top_performers[-1].get('productivity_per_hour', 0)
            elif metric == 'efficiency':
                top_value = top_performers[0].get('efficiency', 0)
                bottom_value = top_performers[-1].get('efficiency', 0)
            else:  # parts_produced
                top_value = top_performers[0].get('total_parts_produced', 0)
                bottom_value = top_performers[-1].get('total_parts_produced', 0)
            
            if bottom_value > 0:
                gap_ratio = top_value / bottom_value
                if gap_ratio >= 2.0:
                    insights['performance_gap'] = 'Large performance gap - significant improvement opportunity'
                    insights['recommendations'].append('Analyze top performer practices for knowledge transfer')
                elif gap_ratio >= 1.5:
                    insights['performance_gap'] = 'Moderate performance gap'
                    insights['recommendations'].append('Implement peer mentoring programs')
                else:
                    insights['performance_gap'] = 'Small performance gap - consistent performance'
            
            # Analyze common characteristics
            skill_levels = [performer.get('skill_level') for performer in top_performers[:3]]
            departments = [performer.get('department') for performer in top_performers[:3]]
            
            # Most common skill level
            if skill_levels:
                most_common_skill = max(set(skill_levels), key=skill_levels.count)
                if skill_levels.count(most_common_skill) >= 2:
                    insights['common_characteristics'].append(f'Most top performers have {most_common_skill} skill level')
            
            # Most common department
            if departments:
                most_common_dept = max(set(departments), key=departments.count)
                if departments.count(most_common_dept) >= 2:
                    insights['common_characteristics'].append(f'Top performers concentrated in {most_common_dept} department')
                    
        except Exception as e:
            logger.warning(f"Error generating top performer insights: {e}")
        
        return insights
    
    def _generate_skill_recommendations(self, 
                                      operator: Operator, 
                                      performance_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate skill development recommendations.
        
        Args:
            operator: Operator entity
            performance_metrics: Performance metrics data
            
        Returns:
            Dict[str, Any]: Skill development recommendations
        """
        recommendations = {
            'current_skill_level': operator.skill_level or 'Unknown',
            'recommended_next_level': None,
            'development_areas': [],
            'training_recommendations': [],
            'timeline': 'Unknown'
        }
        
        try:
            metrics = performance_metrics.get('performance_metrics', {})
            
            if not metrics:
                return recommendations
            
            efficiency = metrics.get('efficiency', 0)
            machines_operated = metrics.get('machines_operated', 0)
            total_jobs = metrics.get('total_jobs', 0)
            
            current_skill = operator.skill_level or 'BEGINNER'
            
            # Determine readiness for next level
            skill_progression = {
                'BEGINNER': {
                    'next': 'INTERMEDIATE',
                    'requirements': {'min_efficiency': 0.65, 'min_jobs': 50, 'min_machines': 1},
                    'timeline': '3-6 months'
                },
                'INTERMEDIATE': {
                    'next': 'ADVANCED',
                    'requirements': {'min_efficiency': 0.75, 'min_jobs': 100, 'min_machines': 2},
                    'timeline': '6-12 months'
                },
                'ADVANCED': {
                    'next': 'EXPERT',
                    'requirements': {'min_efficiency': 0.85, 'min_jobs': 200, 'min_machines': 3},
                    'timeline': '12-18 months'
                },
                'EXPERT': {
                    'next': None,
                    'requirements': {},
                    'timeline': 'Continuous improvement'
                }
            }
            
            progression = skill_progression.get(current_skill, {})
            requirements = progression.get('requirements', {})
            
            if progression.get('next'):
                recommendations['recommended_next_level'] = progression['next']
                recommendations['timeline'] = progression['timeline']
                
                # Check requirements
                if efficiency < requirements.get('min_efficiency', 0):
                    recommendations['development_areas'].append('Improve operational efficiency')
                    recommendations['training_recommendations'].append('Process optimization training')
                
                if total_jobs < requirements.get('min_jobs', 0):
                    recommendations['development_areas'].append('Gain more operational experience')
                    recommendations['training_recommendations'].append('Increase job assignments and variety')
                
                if machines_operated < requirements.get('min_machines', 0):
                    recommendations['development_areas'].append('Learn additional machine operations')
                    recommendations['training_recommendations'].append('Cross-training on different machine types')
                
                # If all requirements met
                if (efficiency >= requirements.get('min_efficiency', 0) and
                    total_jobs >= requirements.get('min_jobs', 0) and
                    machines_operated >= requirements.get('min_machines', 0)):
                    recommendations['training_recommendations'].append(f'Ready for promotion to {progression["next"]} level')
            else:
                recommendations['training_recommendations'].append('Focus on mentoring and knowledge transfer to junior operators')
                
        except Exception as e:
            logger.warning(f"Error generating skill recommendations: {e}")
        
        return recommendations
    
    def _get_performance_benchmarks(self, skill_level: Optional[str]) -> Dict[str, Any]:
        """
        Get performance benchmarks based on skill level.
        
        Args:
            skill_level: Operator skill level
            
        Returns:
            Dict[str, Any]: Performance benchmarks
        """
        # Default benchmarks
        benchmarks = {
            'efficiency_target': 0.75,
            'productivity_target': 10.0,  # parts per hour
            'machine_versatility_target': 2,
            'source': 'Internal Standards'
        }
        
        # Adjust based on skill level
        if skill_level:
            skill_benchmarks = {
                'BEGINNER': {'efficiency': 0.60, 'productivity': 6.0, 'machines': 1},
                'INTERMEDIATE': {'efficiency': 0.70, 'productivity': 8.0, 'machines': 2},
                'ADVANCED': {'efficiency': 0.80, 'productivity': 12.0, 'machines': 3},
                'EXPERT': {'efficiency': 0.85, 'productivity': 15.0, 'machines': 4}
            }
            
            if skill_level in skill_benchmarks:
                skill_data = skill_benchmarks[skill_level]
                benchmarks.update({
                    'efficiency_target': skill_data['efficiency'],
                    'productivity_target': skill_data['productivity'],
                    'machine_versatility_target': skill_data['machines'],
                    'skill_level': skill_level
                })
        
        return benchmarks