"""
Job Service Module

This module provides business logic for job operations, including
scheduling, progress tracking, and performance analysis.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from app.repositories.job_repository import JobRepository
from app.repositories.base_repository import PaginationParams, FilterCondition, FilterOperator
from app.models.database_models import Job

logger = logging.getLogger(__name__)


class JobService:
    """
    Service class for job-related business logic.
    
    Provides high-level operations for job management, scheduling,
    progress tracking, and performance analysis with business rule validation.
    """
    
    def __init__(self, session: AsyncSession):
        """
        Initialize the job service with a database session.
        
        Args:
            session: SQLAlchemy async session
        """
        self.session = session
        self.job_repository = JobRepository(session)
    
    # Job CRUD operations with business logic
    
    async def create_job(self, job_data: Dict[str, Any]) -> Job:
        """
        Create a new job with validation and business rules.
        
        Args:
            job_data: Job data dictionary
            
        Returns:
            Job: Created job
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Validate required fields
            required_fields = ['job_number', 'job_name', 'quantity_ordered']
            for field in required_fields:
                if field not in job_data or not job_data[field]:
                    raise ValueError(f"Required field '{field}' is missing or empty")
            
            # Check if job already exists
            existing_job = await self.job_repository.get_by_id(job_data['job_number'])
            if existing_job:
                raise ValueError(f"Job with number '{job_data['job_number']}' already exists")
            
            # Validate quantity
            if not isinstance(job_data['quantity_ordered'], int) or job_data['quantity_ordered'] <= 0:
                raise ValueError("Quantity ordered must be a positive integer")
            
            # Validate priority
            if 'priority' in job_data and job_data['priority']:
                valid_priorities = ['LOW', 'NORMAL', 'HIGH', 'URGENT']
                if job_data['priority'].upper() not in valid_priorities:
                    raise ValueError(f"Priority must be one of: {valid_priorities}")
                job_data['priority'] = job_data['priority'].upper()
            
            # Validate status
            if 'job_status' in job_data and job_data['job_status']:
                valid_statuses = ['PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED']
                if job_data['job_status'].upper() not in valid_statuses:
                    raise ValueError(f"Job status must be one of: {valid_statuses}")
                job_data['job_status'] = job_data['job_status'].upper()
            
            # Validate numeric fields
            numeric_fields = ['estimated_hours', 'actual_hours', 'complexity_rating', 'setup_complexity']
            for field in numeric_fields:
                if field in job_data and job_data[field] is not None:
                    if not isinstance(job_data[field], (int, float)) or job_data[field] < 0:
                        raise ValueError(f"Field '{field}' must be a non-negative number")
            
            # Validate complexity ratings (1-10 scale)
            for field in ['complexity_rating', 'setup_complexity']:
                if field in job_data and job_data[field] is not None:
                    if not (1 <= job_data[field] <= 10):
                        raise ValueError(f"Field '{field}' must be between 1 and 10")
            
            # Validate dates
            if 'due_date' in job_data and job_data['due_date']:
                if isinstance(job_data['due_date'], str):
                    try:
                        job_data['due_date'] = datetime.fromisoformat(job_data['due_date'].replace('Z', '+00:00'))
                    except ValueError:
                        raise ValueError("Due date must be in ISO format")
                
                # Check if due date is in the future (with some tolerance for existing jobs)
                if job_data['due_date'] < datetime.utcnow() - timedelta(days=1):
                    logger.warning(f"Job {job_data['job_number']} has due date in the past")
            
            # Set default values
            job_data.setdefault('priority', 'NORMAL')
            job_data.setdefault('job_status', 'PENDING')
            job_data.setdefault('quantity_completed', 0)
            job_data.setdefault('created_at', datetime.utcnow())
            job_data.setdefault('updated_at', datetime.utcnow())
            
            job = await self.job_repository.create(**job_data)
            
            logger.info(f"Created job: {job.job_number} - {job.job_name}")
            return job
            
        except Exception as e:
            logger.error(f"Failed to create job: {e}")
            raise
    
    async def get_job_by_number(self, job_number: str, include_relationships: bool = False) -> Optional[Job]:
        """
        Get job by number with optional relationships.
        
        Args:
            job_number: Job number identifier
            include_relationships: Whether to include job log relationships
            
        Returns:
            Optional[Job]: Job if found, None otherwise
        """
        try:
            if include_relationships:
                job = await self.job_repository.get_job_by_number_with_relationships(job_number)
            else:
                job = await self.job_repository.get_by_id(job_number)
            
            if job:
                logger.debug(f"Retrieved job: {job_number}")
            else:
                logger.warning(f"Job not found: {job_number}")
            
            return job
            
        except Exception as e:
            logger.error(f"Failed to get job {job_number}: {e}")
            raise
    
    async def get_jobs_by_status(self, status: str) -> List[Job]:
        """
        Get jobs filtered by status with validation.
        
        Args:
            status: Job status (PENDING, IN_PROGRESS, COMPLETED, CANCELLED)
            
        Returns:
            List[Job]: List of jobs with specified status
        """
        try:
            # Validate status
            valid_statuses = ['PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED']
            if status.upper() not in valid_statuses:
                raise ValueError(f"Status must be one of: {valid_statuses}")
            
            jobs = await self.job_repository.get_jobs_by_status(status)
            
            logger.debug(f"Retrieved {len(jobs)} jobs with status {status}")
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get jobs by status {status}: {e}")
            raise
    
    async def update_job(self, job_number: str, update_data: Dict[str, Any]) -> Optional[Job]:
        """
        Update job with validation and business rules.
        
        Args:
            job_number: Job number identifier
            update_data: Fields to update
            
        Returns:
            Optional[Job]: Updated job if found, None otherwise
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Check if job exists
            existing_job = await self.job_repository.get_by_id(job_number)
            if not existing_job:
                return None
            
            # Validate priority if present
            if 'priority' in update_data and update_data['priority']:
                valid_priorities = ['LOW', 'NORMAL', 'HIGH', 'URGENT']
                if update_data['priority'].upper() not in valid_priorities:
                    raise ValueError(f"Priority must be one of: {valid_priorities}")
                update_data['priority'] = update_data['priority'].upper()
            
            # Validate status if present
            if 'job_status' in update_data and update_data['job_status']:
                valid_statuses = ['PENDING', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED']
                if update_data['job_status'].upper() not in valid_statuses:
                    raise ValueError(f"Job status must be one of: {valid_statuses}")
                update_data['job_status'] = update_data['job_status'].upper()
                
                # Business rule: Set completion date when status changes to COMPLETED
                if update_data['job_status'] == 'COMPLETED' and existing_job.job_status != 'COMPLETED':
                    update_data['completion_date'] = datetime.utcnow()
                
                # Business rule: Set start date when status changes to IN_PROGRESS
                if (update_data['job_status'] == 'IN_PROGRESS' and 
                    existing_job.job_status == 'PENDING' and 
                    not existing_job.start_date):
                    update_data['start_date'] = datetime.utcnow()
            
            # Validate quantity completed
            if 'quantity_completed' in update_data:
                if (not isinstance(update_data['quantity_completed'], int) or 
                    update_data['quantity_completed'] < 0):
                    raise ValueError("Quantity completed must be a non-negative integer")
                
                if update_data['quantity_completed'] > existing_job.quantity_ordered:
                    raise ValueError("Quantity completed cannot exceed quantity ordered")
                
                # Auto-update status if job is completed
                if (update_data['quantity_completed'] >= existing_job.quantity_ordered and
                    existing_job.job_status != 'COMPLETED'):
                    update_data['job_status'] = 'COMPLETED'
                    update_data['completion_date'] = datetime.utcnow()
            
            # Validate numeric fields
            numeric_fields = ['estimated_hours', 'actual_hours', 'complexity_rating', 'setup_complexity']
            for field in numeric_fields:
                if field in update_data and update_data[field] is not None:
                    if not isinstance(update_data[field], (int, float)) or update_data[field] < 0:
                        raise ValueError(f"Field '{field}' must be a non-negative number")
            
            updated_job = await self.job_repository.update(job_number, **update_data)
            
            logger.info(f"Updated job: {job_number}")
            return updated_job
            
        except Exception as e:
            logger.error(f"Failed to update job {job_number}: {e}")
            raise
    
    # Scheduling and progress tracking methods
    
    async def update_job_progress(self, job_number: str, quantity_completed: int) -> Optional[Job]:
        """
        Update job progress with business rule validation.
        
        Args:
            job_number: Job number identifier
            quantity_completed: New quantity completed
            
        Returns:
            Optional[Job]: Updated job or None if not found
        """
        try:
            # Validate input
            if not isinstance(quantity_completed, int) or quantity_completed < 0:
                raise ValueError("Quantity completed must be a non-negative integer")
            
            # Use repository method which includes business logic
            updated_job = await self.job_repository.update_job_progress(job_number, quantity_completed)
            
            if updated_job:
                logger.info(f"Updated progress for job {job_number}: {quantity_completed}/{updated_job.quantity_ordered}")
            else:
                logger.warning(f"Job not found for progress update: {job_number}")
            
            return updated_job
            
        except Exception as e:
            logger.error(f"Failed to update job progress for {job_number}: {e}")
            raise
    
    async def get_overdue_jobs(self) -> List[Job]:
        """
        Get overdue jobs with additional analysis.
        
        Returns:
            List[Job]: List of overdue jobs
        """
        try:
            overdue_jobs = await self.job_repository.get_overdue_jobs()
            
            # Add urgency analysis
            for job in overdue_jobs:
                job.urgency_level = self._calculate_urgency_level(job)
            
            # Sort by urgency (most urgent first)
            overdue_jobs.sort(key=lambda j: getattr(j, 'urgency_level', 0), reverse=True)
            
            logger.debug(f"Retrieved {len(overdue_jobs)} overdue jobs")
            return overdue_jobs
            
        except Exception as e:
            logger.error(f"Failed to get overdue jobs: {e}")
            raise
    
    async def get_job_schedule_analysis(self,
                                      start_date: Optional[datetime] = None,
                                      end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Analyze job scheduling performance and provide insights.
        
        Args:
            start_date: Start date filter for analysis
            end_date: End date filter for analysis
            
        Returns:
            Dict[str, Any]: Schedule analysis with insights
        """
        try:
            # Set default date range if not provided (last 30 days)
            if not start_date and not end_date:
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=30)
            
            # Get job status summary
            status_summary = await self.job_repository.get_job_status_summary(start_date, end_date)
            
            # Get overdue jobs
            overdue_jobs = await self.get_overdue_jobs()
            
            # Generate scheduling insights
            insights = self._generate_schedule_insights(status_summary, overdue_jobs)
            
            analysis = {
                'analysis_period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'status_summary': status_summary,
                'overdue_analysis': {
                    'overdue_count': len(overdue_jobs),
                    'overdue_jobs': [
                        {
                            'job_number': job.job_number,
                            'job_name': job.job_name,
                            'due_date': job.due_date.isoformat() if job.due_date else None,
                            'days_overdue': (datetime.utcnow() - job.due_date).days if job.due_date else 0,
                            'urgency_level': getattr(job, 'urgency_level', 0)
                        }
                        for job in overdue_jobs[:10]  # Top 10 most urgent
                    ]
                },
                'scheduling_insights': insights
            }
            
            logger.debug("Generated job schedule analysis")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to get job schedule analysis: {e}")
            raise
    
    # Performance analysis methods
    
    async def get_job_performance_analysis(self,
                                         job_number: str,
                                         include_details: bool = True) -> Dict[str, Any]:
        """
        Get comprehensive performance analysis for a job.
        
        Args:
            job_number: Job number identifier
            include_details: Whether to include detailed operation breakdown
            
        Returns:
            Dict[str, Any]: Performance analysis with insights
        """
        try:
            # Get performance metrics
            performance_metrics = await self.job_repository.get_job_performance_metrics(
                job_number, include_details
            )
            
            # Generate performance insights
            insights = self._generate_job_performance_insights(performance_metrics)
            performance_metrics['performance_insights'] = insights
            
            logger.debug(f"Generated performance analysis for job {job_number}")
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Failed to get performance analysis for job {job_number}: {e}")
            raise
    
    async def get_customer_job_analysis(self, customer_id: str) -> Dict[str, Any]:
        """
        Analyze job performance for a specific customer.
        
        Args:
            customer_id: Customer identifier
            
        Returns:
            Dict[str, Any]: Customer job analysis
        """
        try:
            # Get customer jobs
            customer_jobs = await self.job_repository.get_jobs_by_customer(customer_id)
            
            if not customer_jobs:
                return {
                    'customer_id': customer_id,
                    'message': 'No jobs found for this customer'
                }
            
            # Calculate customer metrics
            total_jobs = len(customer_jobs)
            completed_jobs = len([job for job in customer_jobs if job.job_status == 'COMPLETED'])
            overdue_jobs = len([job for job in customer_jobs if job.due_date and job.due_date < datetime.utcnow() and job.job_status != 'COMPLETED'])
            
            total_ordered = sum(job.quantity_ordered for job in customer_jobs)
            total_completed = sum(job.quantity_completed for job in customer_jobs)
            
            completion_rate = (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0
            quantity_completion_rate = (total_completed / total_ordered * 100) if total_ordered > 0 else 0
            
            # Calculate average lead times for completed jobs
            completed_with_dates = [
                job for job in customer_jobs 
                if job.job_status == 'COMPLETED' and job.start_date and job.completion_date
            ]
            
            avg_lead_time = 0
            if completed_with_dates:
                lead_times = [(job.completion_date - job.start_date).days for job in completed_with_dates]
                avg_lead_time = sum(lead_times) / len(lead_times)
            
            analysis = {
                'customer_id': customer_id,
                'customer_name': customer_jobs[0].customer_name if customer_jobs else None,
                'summary': {
                    'total_jobs': total_jobs,
                    'completed_jobs': completed_jobs,
                    'overdue_jobs': overdue_jobs,
                    'total_quantity_ordered': total_ordered,
                    'total_quantity_completed': total_completed,
                    'job_completion_rate': completion_rate,
                    'quantity_completion_rate': quantity_completion_rate,
                    'average_lead_time_days': avg_lead_time
                },
                'job_breakdown': [
                    {
                        'job_number': job.job_number,
                        'job_name': job.job_name,
                        'status': job.job_status,
                        'priority': job.priority,
                        'quantity_ordered': job.quantity_ordered,
                        'quantity_completed': job.quantity_completed,
                        'due_date': job.due_date.isoformat() if job.due_date else None,
                        'completion_percentage': (job.quantity_completed / job.quantity_ordered * 100) if job.quantity_ordered > 0 else 0
                    }
                    for job in customer_jobs
                ],
                'insights': self._generate_customer_insights(customer_jobs)
            }
            
            logger.debug(f"Generated customer job analysis for {customer_id}")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to get customer job analysis for {customer_id}: {e}")
            raise
    
    # Private helper methods
    
    def _calculate_urgency_level(self, job: Job) -> int:
        """
        Calculate urgency level for a job (1-10 scale).
        
        Args:
            job: Job entity
            
        Returns:
            int: Urgency level (10 = most urgent)
        """
        urgency = 1
        
        try:
            # Base urgency on days overdue
            if job.due_date:
                days_overdue = (datetime.utcnow() - job.due_date).days
                if days_overdue > 0:
                    urgency = min(5 + days_overdue, 10)
            
            # Increase urgency based on priority
            priority_multipliers = {
                'URGENT': 2.0,
                'HIGH': 1.5,
                'NORMAL': 1.0,
                'LOW': 0.8
            }
            
            multiplier = priority_multipliers.get(job.priority, 1.0)
            urgency = int(urgency * multiplier)
            
            # Cap at 10
            urgency = min(urgency, 10)
            
        except Exception as e:
            logger.warning(f"Error calculating urgency for job {job.job_number}: {e}")
        
        return urgency
    
    def _generate_schedule_insights(self, 
                                  status_summary: Dict[str, Any], 
                                  overdue_jobs: List[Job]) -> Dict[str, Any]:
        """
        Generate scheduling insights from job data.
        
        Args:
            status_summary: Job status summary
            overdue_jobs: List of overdue jobs
            
        Returns:
            Dict[str, Any]: Scheduling insights
        """
        insights = {
            'schedule_performance': 'Unknown',
            'bottlenecks': [],
            'recommendations': []
        }
        
        try:
            summary = status_summary.get('summary', {})
            total_jobs = summary.get('total_jobs', 0)
            overdue_count = len(overdue_jobs)
            
            # Schedule performance assessment
            if total_jobs > 0:
                overdue_percentage = (overdue_count / total_jobs) * 100
                
                if overdue_percentage <= 5:
                    insights['schedule_performance'] = 'Excellent'
                elif overdue_percentage <= 15:
                    insights['schedule_performance'] = 'Good'
                elif overdue_percentage <= 30:
                    insights['schedule_performance'] = 'Needs Improvement'
                    insights['recommendations'].append('Review scheduling processes and capacity planning')
                else:
                    insights['schedule_performance'] = 'Poor'
                    insights['recommendations'].append('Urgent review of scheduling and resource allocation needed')
            
            # Identify bottlenecks
            status_breakdown = status_summary.get('status_breakdown', [])
            
            for status_data in status_breakdown:
                status = status_data.get('status')
                job_count = status_data.get('job_count', 0)
                
                if status == 'PENDING' and job_count > total_jobs * 0.3:
                    insights['bottlenecks'].append('High number of pending jobs - possible capacity constraint')
                elif status == 'IN_PROGRESS' and job_count > total_jobs * 0.4:
                    insights['bottlenecks'].append('High number of in-progress jobs - possible completion issues')
            
            # Priority analysis
            priority_breakdown = status_summary.get('priority_breakdown', [])
            urgent_jobs = sum(p.get('job_count', 0) for p in priority_breakdown if p.get('priority') == 'URGENT')
            
            if urgent_jobs > total_jobs * 0.2:
                insights['bottlenecks'].append('High proportion of urgent jobs - review priority assignment')
                insights['recommendations'].append('Implement better demand forecasting and capacity planning')
            
            # Overdue analysis
            if overdue_count > 0:
                high_urgency_overdue = len([job for job in overdue_jobs if getattr(job, 'urgency_level', 0) >= 8])
                
                if high_urgency_overdue > 0:
                    insights['recommendations'].append(f'{high_urgency_overdue} high-urgency overdue jobs require immediate attention')
                
        except Exception as e:
            logger.warning(f"Error generating schedule insights: {e}")
        
        return insights
    
    def _generate_job_performance_insights(self, performance_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate performance insights for a job.
        
        Args:
            performance_metrics: Job performance metrics
            
        Returns:
            Dict[str, Any]: Performance insights
        """
        insights = {
            'efficiency_assessment': 'Unknown',
            'resource_utilization': 'Unknown',
            'quality_indicators': [],
            'recommendations': []
        }
        
        try:
            metrics = performance_metrics.get('performance_metrics', {})
            job_info = performance_metrics.get('job_info', {})
            schedule_performance = performance_metrics.get('schedule_performance', {})
            
            if not metrics:
                return insights
            
            efficiency = metrics.get('efficiency', 0)
            machines_used = metrics.get('machines_used', 0)
            operators_involved = metrics.get('operators_involved', 0)
            
            # Efficiency assessment
            if efficiency >= 0.85:
                insights['efficiency_assessment'] = 'Excellent'
            elif efficiency >= 0.70:
                insights['efficiency_assessment'] = 'Good'
            elif efficiency >= 0.50:
                insights['efficiency_assessment'] = 'Needs Improvement'
                insights['recommendations'].append('Investigate causes of low efficiency')
            else:
                insights['efficiency_assessment'] = 'Poor'
                insights['recommendations'].append('Urgent efficiency improvement needed')
            
            # Resource utilization
            if machines_used == 1 and operators_involved == 1:
                insights['resource_utilization'] = 'Focused - single machine and operator'
            elif machines_used > 3 or operators_involved > 5:
                insights['resource_utilization'] = 'Complex - multiple resources involved'
                insights['recommendations'].append('Consider resource optimization and coordination')
            else:
                insights['resource_utilization'] = 'Moderate resource usage'
            
            # Schedule performance
            if schedule_performance.get('on_schedule') is True:
                insights['quality_indicators'].append('Delivered on schedule')
            elif schedule_performance.get('on_schedule') is False:
                insights['quality_indicators'].append('Delivered late')
                insights['recommendations'].append('Review scheduling accuracy and capacity planning')
            
            # Estimated vs actual hours
            est_vs_actual = schedule_performance.get('estimated_vs_actual_hours', {})
            if est_vs_actual:
                variance = est_vs_actual.get('variance_percentage', 0)
                if abs(variance) <= 10:
                    insights['quality_indicators'].append('Accurate time estimation')
                elif variance > 20:
                    insights['quality_indicators'].append('Significant time overrun')
                    insights['recommendations'].append('Improve time estimation accuracy')
                elif variance < -20:
                    insights['quality_indicators'].append('Significant time underestimation')
                    insights['recommendations'].append('Review estimation methodology')
            
            # Completion rate
            completion_percentage = job_info.get('completion_percentage', 0)
            if completion_percentage == 100:
                insights['quality_indicators'].append('Fully completed')
            elif completion_percentage >= 90:
                insights['quality_indicators'].append('Near completion')
            elif completion_percentage < 50:
                insights['recommendations'].append('Job progress is behind schedule')
                
        except Exception as e:
            logger.warning(f"Error generating job performance insights: {e}")
        
        return insights
    
    def _generate_customer_insights(self, customer_jobs: List[Job]) -> Dict[str, Any]:
        """
        Generate insights for customer job analysis.
        
        Args:
            customer_jobs: List of customer jobs
            
        Returns:
            Dict[str, Any]: Customer insights
        """
        insights = {
            'customer_relationship': 'Unknown',
            'delivery_performance': 'Unknown',
            'job_complexity': 'Unknown',
            'recommendations': []
        }
        
        try:
            total_jobs = len(customer_jobs)
            completed_jobs = [job for job in customer_jobs if job.job_status == 'COMPLETED']
            overdue_jobs = [job for job in customer_jobs if job.due_date and job.due_date < datetime.utcnow() and job.job_status != 'COMPLETED']
            
            # Customer relationship assessment
            if total_jobs >= 20:
                insights['customer_relationship'] = 'Major customer - high volume'
            elif total_jobs >= 5:
                insights['customer_relationship'] = 'Regular customer - moderate volume'
            else:
                insights['customer_relationship'] = 'Occasional customer - low volume'
            
            # Delivery performance
            if len(overdue_jobs) == 0:
                insights['delivery_performance'] = 'Excellent - no overdue jobs'
            elif len(overdue_jobs) / total_jobs <= 0.1:
                insights['delivery_performance'] = 'Good - minimal delays'
            elif len(overdue_jobs) / total_jobs <= 0.2:
                insights['delivery_performance'] = 'Needs improvement - some delays'
                insights['recommendations'].append('Focus on improving delivery reliability for this customer')
            else:
                insights['delivery_performance'] = 'Poor - frequent delays'
                insights['recommendations'].append('Urgent attention needed for delivery performance')
            
            # Job complexity analysis
            priorities = [job.priority for job in customer_jobs]
            urgent_count = priorities.count('URGENT')
            high_count = priorities.count('HIGH')
            
            if urgent_count / total_jobs >= 0.3:
                insights['job_complexity'] = 'High urgency customer - frequent rush orders'
                insights['recommendations'].append('Consider capacity reservation or premium pricing for rush orders')
            elif (urgent_count + high_count) / total_jobs >= 0.5:
                insights['job_complexity'] = 'High priority customer - demanding requirements'
            else:
                insights['job_complexity'] = 'Standard complexity jobs'
            
            # Volume trends (if we have date information)
            recent_jobs = [job for job in customer_jobs if job.created_at and job.created_at >= datetime.utcnow() - timedelta(days=90)]
            older_jobs = [job for job in customer_jobs if job.created_at and job.created_at < datetime.utcnow() - timedelta(days=90)]
            
            if len(recent_jobs) > len(older_jobs) * 1.5:
                insights['recommendations'].append('Growing customer - consider account management focus')
            elif len(recent_jobs) < len(older_jobs) * 0.5:
                insights['recommendations'].append('Declining customer activity - investigate retention opportunities')
                
        except Exception as e:
            logger.warning(f"Error generating customer insights: {e}")
        
        return insights