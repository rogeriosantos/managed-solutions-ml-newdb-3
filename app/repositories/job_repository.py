"""
Job Repository Module

This module provides data access operations for Job entities,
including status filtering and performance tracking.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc, case
from sqlalchemy.orm import selectinload
import logging

from app.models.database_models import Job, JobLogOB, Machine, Operator, Part
from app.repositories.base_repository import (
    BaseRepository, FilterCondition, FilterOperator, 
    PaginationParams, PaginatedResult
)

logger = logging.getLogger(__name__)


class JobRepository(BaseRepository[Job]):
    """
    Repository for Job entity with specialized queries for performance tracking.
    
    Provides methods for job data retrieval, status filtering,
    and performance metrics calculation.
    """
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, Job)
    
    def get_primary_key_field(self) -> str:
        """Get the primary key field name for Job model."""
        return "job_number"
    
    # Job-specific CRUD operations
    
    async def get_job_by_number_with_relationships(self, job_number: str) -> Optional[Job]:
        """
        Get job by number with all related job logs loaded.
        
        Args:
            job_number: Job number identifier
            
        Returns:
            Optional[Job]: Job with relationships or None if not found
        """
        try:
            stmt = (select(Job)
                   .options(selectinload(Job.job_logs))
                   .where(Job.job_number == job_number))
            
            result = await self.session.execute(stmt)
            job = result.scalar_one_or_none()
            
            if job:
                logger.debug(f"Retrieved job {job_number} with {len(job.job_logs)} job logs")
            
            return job
        except Exception as e:
            logger.error(f"Failed to get job {job_number} with relationships: {e}")
            raise
    
    async def get_jobs_by_status(self, status: str) -> List[Job]:
        """
        Get jobs filtered by status.
        
        Args:
            status: Job status (PENDING, IN_PROGRESS, COMPLETED, CANCELLED)
            
        Returns:
            List[Job]: List of jobs with specified status
        """
        try:
            filters = [FilterCondition("job_status", FilterOperator.EQ, status.upper())]
            jobs = await self.get_all(filters=filters, order_by="due_date")
            
            logger.debug(f"Retrieved {len(jobs)} jobs with status {status}")
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get jobs by status {status}: {e}")
            raise
    
    async def get_jobs_by_priority(self, priority: str) -> List[Job]:
        """
        Get jobs filtered by priority.
        
        Args:
            priority: Job priority (LOW, NORMAL, HIGH, URGENT)
            
        Returns:
            List[Job]: List of jobs with specified priority
        """
        try:
            filters = [FilterCondition("priority", FilterOperator.EQ, priority.upper())]
            jobs = await self.get_all(filters=filters, order_by="due_date")
            
            logger.debug(f"Retrieved {len(jobs)} jobs with priority {priority}")
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get jobs by priority {priority}: {e}")
            raise
    
    async def get_overdue_jobs(self) -> List[Job]:
        """
        Get jobs that are overdue (past due date and not completed).
        
        Returns:
            List[Job]: List of overdue jobs
        """
        try:
            current_time = datetime.utcnow()
            filters = [
                FilterCondition("due_date", FilterOperator.LT, current_time),
                FilterCondition("job_status", FilterOperator.NE, "COMPLETED"),
                FilterCondition("job_status", FilterOperator.NE, "CANCELLED")
            ]
            
            jobs = await self.get_all(filters=filters, order_by="due_date")
            
            logger.debug(f"Retrieved {len(jobs)} overdue jobs")
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get overdue jobs: {e}")
            raise
    
    async def get_jobs_by_customer(self, customer_id: str) -> List[Job]:
        """
        Get jobs for a specific customer.
        
        Args:
            customer_id: Customer identifier
            
        Returns:
            List[Job]: List of jobs for the customer
        """
        try:
            filters = [FilterCondition("customer_id", FilterOperator.EQ, customer_id)]
            jobs = await self.get_all(filters=filters, order_by="due_date")
            
            logger.debug(f"Retrieved {len(jobs)} jobs for customer {customer_id}")
            return jobs
            
        except Exception as e:
            logger.error(f"Failed to get jobs for customer {customer_id}: {e}")
            raise
    
    # Performance analysis methods
    
    async def get_job_performance_metrics(self,
                                        job_number: str,
                                        include_details: bool = True) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics for a job.
        
        Args:
            job_number: Job number identifier
            include_details: Whether to include detailed breakdown
            
        Returns:
            Dict[str, Any]: Performance metrics and statistics
        """
        try:
            # Get job information
            job = await self.get_by_id(job_number)
            if not job:
                raise ValueError(f"Job {job_number} not found")
            
            # Build query for performance metrics
            stmt = select(
                func.count(JobLogOB.id).label('total_operations'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.sum(JobLogOB.job_duration).label('total_job_duration'),
                func.sum(JobLogOB.parts_produced).label('total_parts_produced'),
                func.avg(JobLogOB.running_time).label('avg_running_time'),
                func.avg(JobLogOB.job_duration).label('avg_job_duration'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_operation'),
                func.count(func.distinct(JobLogOB.machine)).label('machines_used'),
                func.count(func.distinct(JobLogOB.emp_id)).label('operators_involved'),
                func.count(func.distinct(JobLogOB.part_number)).label('unique_parts'),
                
                # Downtime metrics
                func.sum(JobLogOB.setup_time).label('total_setup_time'),
                func.sum(JobLogOB.maintenance_time).label('total_maintenance_time'),
                func.sum(JobLogOB.adjustment_time).label('total_adjustment_time'),
                func.sum(JobLogOB.tooling_time).label('total_tooling_time'),
                func.sum(JobLogOB.idle_time).label('total_idle_time'),
                
                # Time analysis
                func.min(JobLogOB.start_time).label('first_operation'),
                func.max(JobLogOB.end_time).label('last_operation')
            ).where(JobLogOB.job_number == job_number)
            
            result = await self.session.execute(stmt)
            row = result.first()
            
            if not row or row.total_operations == 0:
                return {
                    'job_number': job_number,
                    'job_info': {
                        'job_name': job.job_name,
                        'customer_name': job.customer_name,
                        'priority': job.priority,
                        'job_status': job.job_status,
                        'quantity_ordered': job.quantity_ordered,
                        'quantity_completed': job.quantity_completed
                    },
                    'performance_metrics': {},
                    'message': 'No operations data available for this job'
                }
            
            # Calculate performance metrics
            total_running_time = row.total_running_time or 0
            total_job_duration = row.total_job_duration or 0
            total_parts_produced = row.total_parts_produced or 0
            
            total_downtime = sum([
                row.total_setup_time or 0,
                row.total_maintenance_time or 0,
                row.total_adjustment_time or 0,
                row.total_tooling_time or 0,
                row.total_idle_time or 0
            ])
            
            efficiency = 0.0
            if total_job_duration > 0:
                efficiency = total_running_time / total_job_duration
            
            # Calculate completion percentage
            completion_percentage = 0.0
            if job.quantity_ordered > 0:
                completion_percentage = (job.quantity_completed / job.quantity_ordered) * 100
            
            # Calculate schedule performance
            schedule_performance = self._calculate_schedule_performance(job, row.first_operation, row.last_operation)
            
            performance_metrics = {
                'job_number': job_number,
                'job_info': {
                    'job_name': job.job_name,
                    'customer_name': job.customer_name,
                    'priority': job.priority,
                    'job_status': job.job_status,
                    'quantity_ordered': job.quantity_ordered,
                    'quantity_completed': job.quantity_completed,
                    'estimated_hours': job.estimated_hours,
                    'actual_hours': job.actual_hours,
                    'due_date': job.due_date.isoformat() if job.due_date else None,
                    'completion_percentage': completion_percentage
                },
                'performance_metrics': {
                    'total_operations': row.total_operations,
                    'total_running_time': total_running_time,
                    'total_job_duration': total_job_duration,
                    'total_parts_produced': total_parts_produced,
                    'avg_running_time': float(row.avg_running_time or 0),
                    'avg_job_duration': float(row.avg_job_duration or 0),
                    'avg_parts_per_operation': float(row.avg_parts_per_operation or 0),
                    'machines_used': row.machines_used,
                    'operators_involved': row.operators_involved,
                    'unique_parts': row.unique_parts,
                    'efficiency': efficiency,
                    'total_downtime': total_downtime,
                    'first_operation': row.first_operation.isoformat() if row.first_operation else None,
                    'last_operation': row.last_operation.isoformat() if row.last_operation else None
                },
                'downtime_breakdown': {
                    'setup_time': row.total_setup_time or 0,
                    'maintenance_time': row.total_maintenance_time or 0,
                    'adjustment_time': row.total_adjustment_time or 0,
                    'tooling_time': row.total_tooling_time or 0,
                    'idle_time': row.total_idle_time or 0
                },
                'schedule_performance': schedule_performance
            }
            
            # Add detailed breakdown if requested
            if include_details:
                performance_metrics['operation_details'] = await self._get_job_operation_details(job_number)
            
            logger.debug(f"Generated performance metrics for job {job_number}: "
                        f"{row.total_operations} operations, {efficiency:.3f} efficiency")
            
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Failed to get performance metrics for job {job_number}: {e}")
            raise
    
    def _calculate_schedule_performance(self, 
                                      job: Job, 
                                      first_operation: Optional[datetime], 
                                      last_operation: Optional[datetime]) -> Dict[str, Any]:
        """
        Calculate schedule performance metrics for a job.
        
        Args:
            job: Job entity
            first_operation: First operation start time
            last_operation: Last operation end time
            
        Returns:
            Dict[str, Any]: Schedule performance metrics
        """
        schedule_performance = {
            'on_schedule': None,
            'days_ahead_behind': None,
            'estimated_vs_actual_hours': None,
            'schedule_adherence': None
        }
        
        try:
            # Check if job is on schedule
            if job.due_date and last_operation:
                if job.job_status == 'COMPLETED':
                    days_difference = (job.due_date - last_operation).days
                    schedule_performance['on_schedule'] = days_difference >= 0
                    schedule_performance['days_ahead_behind'] = days_difference
                elif job.job_status in ['IN_PROGRESS', 'PENDING']:
                    current_time = datetime.utcnow()
                    days_until_due = (job.due_date - current_time).days
                    schedule_performance['days_until_due'] = days_until_due
                    schedule_performance['on_schedule'] = days_until_due >= 0
            
            # Compare estimated vs actual hours
            if job.estimated_hours and job.actual_hours:
                variance = ((job.actual_hours - job.estimated_hours) / job.estimated_hours) * 100
                schedule_performance['estimated_vs_actual_hours'] = {
                    'estimated_hours': job.estimated_hours,
                    'actual_hours': job.actual_hours,
                    'variance_percentage': variance
                }
            
            # Calculate overall schedule adherence
            if job.start_date and job.due_date and first_operation and last_operation:
                planned_duration = (job.due_date - job.start_date).days
                actual_duration = (last_operation - first_operation).days
                
                if planned_duration > 0:
                    adherence = (planned_duration / max(actual_duration, 1)) * 100
                    schedule_performance['schedule_adherence'] = min(adherence, 100)
            
        except Exception as e:
            logger.warning(f"Error calculating schedule performance: {e}")
        
        return schedule_performance
    
    async def _get_job_operation_details(self, job_number: str) -> List[Dict[str, Any]]:
        """
        Get detailed operation breakdown for a job.
        
        Args:
            job_number: Job number identifier
            
        Returns:
            List[Dict[str, Any]]: Detailed operation information
        """
        try:
            stmt = select(
                JobLogOB.id,
                JobLogOB.machine,
                JobLogOB.emp_id,
                JobLogOB.operator_name,
                JobLogOB.part_number,
                JobLogOB.start_time,
                JobLogOB.end_time,
                JobLogOB.running_time,
                JobLogOB.job_duration,
                JobLogOB.parts_produced,
                JobLogOB.state
            ).where(JobLogOB.job_number == job_number).order_by(JobLogOB.start_time)
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            operation_details = []
            for row in rows:
                efficiency = 0.0
                if row.job_duration and row.job_duration > 0:
                    efficiency = (row.running_time or 0) / row.job_duration
                
                operation_details.append({
                    'operation_id': row.id,
                    'machine': row.machine,
                    'operator': {
                        'emp_id': row.emp_id,
                        'name': row.operator_name
                    },
                    'part_number': row.part_number,
                    'start_time': row.start_time.isoformat() if row.start_time else None,
                    'end_time': row.end_time.isoformat() if row.end_time else None,
                    'running_time': row.running_time or 0,
                    'job_duration': row.job_duration or 0,
                    'parts_produced': row.parts_produced or 0,
                    'state': row.state,
                    'efficiency': efficiency
                })
            
            return operation_details
            
        except Exception as e:
            logger.error(f"Failed to get operation details for job {job_number}: {e}")
            raise
    
    async def get_job_status_summary(self,
                                   start_date: Optional[datetime] = None,
                                   end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get summary of job statuses and performance metrics.
        
        Args:
            start_date: Start date filter for job creation (inclusive)
            end_date: End date filter for job creation (inclusive)
            
        Returns:
            Dict[str, Any]: Job status summary and metrics
        """
        try:
            # Build base query
            stmt = select(
                Job.job_status,
                func.count(Job.job_number).label('job_count'),
                func.avg(Job.estimated_hours).label('avg_estimated_hours'),
                func.avg(Job.actual_hours).label('avg_actual_hours'),
                func.sum(Job.quantity_ordered).label('total_quantity_ordered'),
                func.sum(Job.quantity_completed).label('total_quantity_completed')
            )
            
            # Apply date filters if provided
            if start_date:
                stmt = stmt.where(Job.created_at >= start_date)
            if end_date:
                stmt = stmt.where(Job.created_at <= end_date)
            
            stmt = stmt.group_by(Job.job_status).order_by(Job.job_status)
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            # Get priority breakdown
            priority_stmt = select(
                Job.priority,
                func.count(Job.job_number).label('job_count')
            )
            
            if start_date:
                priority_stmt = priority_stmt.where(Job.created_at >= start_date)
            if end_date:
                priority_stmt = priority_stmt.where(Job.created_at <= end_date)
            
            priority_stmt = priority_stmt.group_by(Job.priority).order_by(Job.priority)
            
            priority_result = await self.session.execute(priority_stmt)
            priority_rows = priority_result.all()
            
            # Calculate summary metrics
            total_jobs = sum(row.job_count for row in rows)
            total_ordered = sum(row.total_quantity_ordered or 0 for row in rows)
            total_completed = sum(row.total_quantity_completed or 0 for row in rows)
            
            overall_completion_rate = 0.0
            if total_ordered > 0:
                overall_completion_rate = (total_completed / total_ordered) * 100
            
            status_summary = {
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'summary': {
                    'total_jobs': total_jobs,
                    'total_quantity_ordered': total_ordered,
                    'total_quantity_completed': total_completed,
                    'overall_completion_rate': overall_completion_rate
                },
                'status_breakdown': [],
                'priority_breakdown': []
            }
            
            # Add status breakdown
            for row in rows:
                completion_rate = 0.0
                if row.total_quantity_ordered and row.total_quantity_ordered > 0:
                    completion_rate = ((row.total_quantity_completed or 0) / row.total_quantity_ordered) * 100
                
                status_summary['status_breakdown'].append({
                    'status': row.job_status,
                    'job_count': row.job_count,
                    'avg_estimated_hours': float(row.avg_estimated_hours or 0),
                    'avg_actual_hours': float(row.avg_actual_hours or 0),
                    'total_quantity_ordered': row.total_quantity_ordered or 0,
                    'total_quantity_completed': row.total_quantity_completed or 0,
                    'completion_rate': completion_rate
                })
            
            # Add priority breakdown
            for row in priority_rows:
                status_summary['priority_breakdown'].append({
                    'priority': row.priority,
                    'job_count': row.job_count
                })
            
            logger.debug(f"Generated job status summary: {total_jobs} total jobs")
            
            return status_summary
            
        except Exception as e:
            logger.error(f"Failed to get job status summary: {e}")
            raise
    
    # Utility methods
    
    async def update_job_progress(self, job_number: str, quantity_completed: int) -> Optional[Job]:
        """
        Update job progress and automatically update status if completed.
        
        Args:
            job_number: Job number identifier
            quantity_completed: New quantity completed
            
        Returns:
            Optional[Job]: Updated job or None if not found
        """
        try:
            job = await self.get_by_id(job_number)
            if not job:
                return None
            
            update_data = {'quantity_completed': quantity_completed}
            
            # Auto-update status if job is completed
            if quantity_completed >= job.quantity_ordered:
                update_data['job_status'] = 'COMPLETED'
                update_data['completion_date'] = datetime.utcnow()
            elif quantity_completed > 0 and job.job_status == 'PENDING':
                update_data['job_status'] = 'IN_PROGRESS'
                if not job.start_date:
                    update_data['start_date'] = datetime.utcnow()
            
            updated_job = await self.update(job_number, **update_data)
            
            logger.debug(f"Updated job {job_number} progress: {quantity_completed}/{job.quantity_ordered}")
            
            return updated_job
            
        except Exception as e:
            logger.error(f"Failed to update job progress for {job_number}: {e}")
            raise