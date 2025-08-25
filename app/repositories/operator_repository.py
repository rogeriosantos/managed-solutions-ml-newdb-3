"""
Operator Repository Module

This module provides data access operations for Operator entities,
including skill-based queries and performance metrics.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc, case
from sqlalchemy.orm import selectinload
import logging

from app.models.database_models import Operator, JobLogOB, Machine, Job, Part
from app.repositories.base_repository import (
    BaseRepository, FilterCondition, FilterOperator, 
    PaginationParams, PaginatedResult
)

logger = logging.getLogger(__name__)


class OperatorRepository(BaseRepository[Operator]):
    """
    Repository for Operator entity with specialized queries for performance analysis.
    
    Provides methods for operator data retrieval, skill-based filtering,
    and performance metrics calculation.
    """
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, Operator)
    
    def get_primary_key_field(self) -> str:
        """Get the primary key field name for Operator model."""
        return "emp_id"
    
    # Operator-specific CRUD operations
    
    async def get_operator_by_id_with_relationships(self, emp_id: str) -> Optional[Operator]:
        """
        Get operator by ID with all related job logs loaded.
        
        Args:
            emp_id: Employee identifier
            
        Returns:
            Optional[Operator]: Operator with relationships or None if not found
        """
        try:
            stmt = (select(Operator)
                   .options(selectinload(Operator.job_logs))
                   .where(Operator.emp_id == emp_id))
            
            result = await self.session.execute(stmt)
            operator = result.scalar_one_or_none()
            
            if operator:
                logger.debug(f"Retrieved operator {emp_id} with {len(operator.job_logs)} job logs")
            
            return operator
        except Exception as e:
            logger.error(f"Failed to get operator {emp_id} with relationships: {e}")
            raise
    
    async def get_active_operators(self) -> List[Operator]:
        """
        Get all active operators.
        
        Returns:
            List[Operator]: List of active operators
        """
        filters = [FilterCondition("status", FilterOperator.EQ, "ACTIVE")]
        return await self.get_all(filters=filters, order_by="operator_name")
    
    async def get_operators_by_skill_level(self, skill_level: str) -> List[Operator]:
        """
        Get operators filtered by skill level.
        
        Args:
            skill_level: Skill level (BEGINNER, INTERMEDIATE, ADVANCED, EXPERT)
            
        Returns:
            List[Operator]: List of operators with specified skill level
        """
        try:
            filters = [
                FilterCondition("skill_level", FilterOperator.EQ, skill_level.upper()),
                FilterCondition("status", FilterOperator.EQ, "ACTIVE")
            ]
            
            operators = await self.get_all(filters=filters, order_by="operator_name")
            
            logger.debug(f"Retrieved {len(operators)} operators with skill level {skill_level}")
            return operators
            
        except Exception as e:
            logger.error(f"Failed to get operators by skill level {skill_level}: {e}")
            raise
    
    async def get_operators_by_department(self, department: str) -> List[Operator]:
        """
        Get operators filtered by department.
        
        Args:
            department: Department name
            
        Returns:
            List[Operator]: List of operators in specified department
        """
        try:
            filters = [
                FilterCondition("department", FilterOperator.EQ, department),
                FilterCondition("status", FilterOperator.EQ, "ACTIVE")
            ]
            
            operators = await self.get_all(filters=filters, order_by="operator_name")
            
            logger.debug(f"Retrieved {len(operators)} operators in department {department}")
            return operators
            
        except Exception as e:
            logger.error(f"Failed to get operators by department {department}: {e}")
            raise
    
    # Performance analysis methods
    
    async def get_operator_performance_metrics(self,
                                             emp_id: str,
                                             start_date: Optional[datetime] = None,
                                             end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get comprehensive performance metrics for an operator.
        
        Args:
            emp_id: Employee identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Performance metrics and statistics
        """
        try:
            # Build query for performance metrics
            stmt = select(
                func.count(JobLogOB.id).label('total_jobs'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.sum(JobLogOB.job_duration).label('total_job_duration'),
                func.sum(JobLogOB.parts_produced).label('total_parts_produced'),
                func.avg(JobLogOB.running_time).label('avg_running_time'),
                func.avg(JobLogOB.job_duration).label('avg_job_duration'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_job'),
                func.max(JobLogOB.parts_produced).label('max_parts_per_job'),
                func.min(JobLogOB.parts_produced).label('min_parts_per_job'),
                func.count(func.distinct(JobLogOB.machine)).label('machines_operated'),
                func.count(func.distinct(JobLogOB.job_number)).label('unique_jobs'),
                func.count(func.distinct(JobLogOB.part_number)).label('unique_parts'),
                
                # Downtime metrics
                func.sum(JobLogOB.setup_time).label('total_setup_time'),
                func.sum(JobLogOB.maintenance_time).label('total_maintenance_time'),
                func.sum(JobLogOB.adjustment_time).label('total_adjustment_time'),
                func.sum(JobLogOB.tooling_time).label('total_tooling_time'),
                func.sum(JobLogOB.idle_time).label('total_idle_time')
            ).where(JobLogOB.emp_id == emp_id)
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            result = await self.session.execute(stmt)
            row = result.first()
            
            if not row or row.total_jobs == 0:
                return {
                    'emp_id': emp_id,
                    'period': {
                        'start_date': start_date.isoformat() if start_date else None,
                        'end_date': end_date.isoformat() if end_date else None
                    },
                    'performance_metrics': {},
                    'message': 'No data available for the specified period'
                }
            
            # Calculate efficiency metrics
            total_running_time = row.total_running_time or 0
            total_job_duration = row.total_job_duration or 0
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
            
            productivity = 0.0
            if total_running_time > 0:
                productivity = (row.total_parts_produced or 0) / (total_running_time / 3600)
            
            # Get machine performance breakdown
            machine_performance = await self._get_operator_machine_performance(
                emp_id, start_date, end_date
            )
            
            performance_metrics = {
                'emp_id': emp_id,
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'performance_metrics': {
                    'total_jobs': row.total_jobs,
                    'total_running_time': total_running_time,
                    'total_job_duration': total_job_duration,
                    'total_parts_produced': row.total_parts_produced or 0,
                    'avg_running_time': float(row.avg_running_time or 0),
                    'avg_job_duration': float(row.avg_job_duration or 0),
                    'avg_parts_per_job': float(row.avg_parts_per_job or 0),
                    'max_parts_per_job': row.max_parts_per_job or 0,
                    'min_parts_per_job': row.min_parts_per_job or 0,
                    'machines_operated': row.machines_operated,
                    'unique_jobs': row.unique_jobs,
                    'unique_parts': row.unique_parts,
                    'efficiency': efficiency,
                    'productivity_per_hour': productivity,
                    'total_downtime': total_downtime
                },
                'downtime_breakdown': {
                    'setup_time': row.total_setup_time or 0,
                    'maintenance_time': row.total_maintenance_time or 0,
                    'adjustment_time': row.total_adjustment_time or 0,
                    'tooling_time': row.total_tooling_time or 0,
                    'idle_time': row.total_idle_time or 0
                },
                'machine_performance': machine_performance
            }
            
            logger.debug(f"Generated performance metrics for operator {emp_id}: "
                        f"{row.total_jobs} jobs, {efficiency:.3f} efficiency")
            
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Failed to get performance metrics for operator {emp_id}: {e}")
            raise
    
    async def _get_operator_machine_performance(self,
                                              emp_id: str,
                                              start_date: Optional[datetime] = None,
                                              end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get operator performance breakdown by machine.
        
        Args:
            emp_id: Employee identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            List[Dict[str, Any]]: Performance metrics by machine
        """
        try:
            stmt = select(
                JobLogOB.machine,
                func.count(JobLogOB.id).label('job_count'),
                func.sum(JobLogOB.running_time).label('running_time'),
                func.sum(JobLogOB.job_duration).label('job_duration'),
                func.sum(JobLogOB.parts_produced).label('parts_produced'),
                func.avg(JobLogOB.running_time).label('avg_running_time')
            ).where(JobLogOB.emp_id == emp_id)
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            stmt = (stmt.group_by(JobLogOB.machine)
                   .order_by(desc(func.count(JobLogOB.id))))
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            machine_performance = []
            for row in rows:
                running_time = row.running_time or 0
                job_duration = row.job_duration or 0
                
                efficiency = 0.0
                if job_duration > 0:
                    efficiency = running_time / job_duration
                
                productivity = 0.0
                if running_time > 0:
                    productivity = (row.parts_produced or 0) / (running_time / 3600)
                
                machine_performance.append({
                    'machine': row.machine,
                    'job_count': row.job_count,
                    'running_time': running_time,
                    'job_duration': job_duration,
                    'parts_produced': row.parts_produced or 0,
                    'avg_running_time': float(row.avg_running_time or 0),
                    'efficiency': efficiency,
                    'productivity_per_hour': productivity
                })
            
            return machine_performance
            
        except Exception as e:
            logger.error(f"Failed to get machine performance for operator {emp_id}: {e}")
            raise
    
    async def get_operator_skill_analysis(self,
                                        start_date: Optional[datetime] = None,
                                        end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Analyze performance metrics by skill level.
        
        Args:
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Performance analysis by skill level
        """
        try:
            # Build query joining operators with job logs
            stmt = select(
                Operator.skill_level,
                func.count(func.distinct(Operator.emp_id)).label('operator_count'),
                func.count(JobLogOB.id).label('total_jobs'),
                func.avg(JobLogOB.running_time).label('avg_running_time'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_job'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.sum(JobLogOB.job_duration).label('total_job_duration'),
                func.sum(JobLogOB.parts_produced).label('total_parts_produced')
            ).select_from(
                Operator.__table__.join(JobLogOB, Operator.emp_id == JobLogOB.emp_id)
            ).where(
                and_(
                    Operator.status == "ACTIVE",
                    Operator.skill_level.is_not(None)
                )
            )
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            stmt = stmt.group_by(Operator.skill_level).order_by(Operator.skill_level)
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            skill_analysis = {
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'skill_levels': []
            }
            
            for row in rows:
                total_running_time = row.total_running_time or 0
                total_job_duration = row.total_job_duration or 0
                
                efficiency = 0.0
                if total_job_duration > 0:
                    efficiency = total_running_time / total_job_duration
                
                productivity = 0.0
                if total_running_time > 0:
                    productivity = (row.total_parts_produced or 0) / (total_running_time / 3600)
                
                skill_analysis['skill_levels'].append({
                    'skill_level': row.skill_level,
                    'operator_count': row.operator_count,
                    'total_jobs': row.total_jobs,
                    'avg_running_time': float(row.avg_running_time or 0),
                    'avg_parts_per_job': float(row.avg_parts_per_job or 0),
                    'total_running_time': total_running_time,
                    'total_job_duration': total_job_duration,
                    'total_parts_produced': row.total_parts_produced or 0,
                    'efficiency': efficiency,
                    'productivity_per_hour': productivity
                })
            
            logger.debug(f"Generated skill analysis for {len(rows)} skill levels")
            
            return skill_analysis
            
        except Exception as e:
            logger.error(f"Failed to get operator skill analysis: {e}")
            raise
    
    async def get_top_performers(self,
                               metric: str = 'productivity',
                               limit: int = 10,
                               start_date: Optional[datetime] = None,
                               end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get top performing operators based on specified metric.
        
        Args:
            metric: Performance metric ('productivity', 'efficiency', 'parts_produced')
            limit: Number of top performers to return
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            List[Dict[str, Any]]: Top performing operators
        """
        try:
            # Build base query
            stmt = select(
                Operator.emp_id,
                Operator.operator_name,
                Operator.skill_level,
                Operator.department,
                func.count(JobLogOB.id).label('total_jobs'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.sum(JobLogOB.job_duration).label('total_job_duration'),
                func.sum(JobLogOB.parts_produced).label('total_parts_produced'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_job')
            ).select_from(
                Operator.__table__.join(JobLogOB, Operator.emp_id == JobLogOB.emp_id)
            ).where(Operator.status == "ACTIVE")
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            stmt = stmt.group_by(
                Operator.emp_id, Operator.operator_name, 
                Operator.skill_level, Operator.department
            )
            
            # Order by specified metric
            if metric == 'productivity':
                # Parts per hour
                stmt = stmt.order_by(desc(func.sum(JobLogOB.parts_produced) / 
                                        (func.sum(JobLogOB.running_time) / 3600)))
            elif metric == 'efficiency':
                # Running time / job duration
                stmt = stmt.order_by(desc(func.sum(JobLogOB.running_time) / 
                                        func.sum(JobLogOB.job_duration)))
            elif metric == 'parts_produced':
                stmt = stmt.order_by(desc(func.sum(JobLogOB.parts_produced)))
            else:
                raise ValueError(f"Unsupported metric: {metric}")
            
            stmt = stmt.limit(limit)
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            top_performers = []
            for row in rows:
                total_running_time = row.total_running_time or 0
                total_job_duration = row.total_job_duration or 0
                total_parts_produced = row.total_parts_produced or 0
                
                efficiency = 0.0
                if total_job_duration > 0:
                    efficiency = total_running_time / total_job_duration
                
                productivity = 0.0
                if total_running_time > 0:
                    productivity = total_parts_produced / (total_running_time / 3600)
                
                top_performers.append({
                    'emp_id': row.emp_id,
                    'operator_name': row.operator_name,
                    'skill_level': row.skill_level,
                    'department': row.department,
                    'total_jobs': row.total_jobs,
                    'total_running_time': total_running_time,
                    'total_job_duration': total_job_duration,
                    'total_parts_produced': total_parts_produced,
                    'avg_parts_per_job': float(row.avg_parts_per_job or 0),
                    'efficiency': efficiency,
                    'productivity_per_hour': productivity
                })
            
            logger.debug(f"Retrieved top {len(top_performers)} performers by {metric}")
            
            return top_performers
            
        except Exception as e:
            logger.error(f"Failed to get top performers by {metric}: {e}")
            raise
    
    # Utility methods
    
    async def get_operator_summary_statistics(self) -> Dict[str, Any]:
        """
        Get summary statistics for all operators.
        
        Returns:
            Dict[str, Any]: Summary statistics
        """
        try:
            # Get operator counts by status
            status_stmt = select(
                Operator.status,
                func.count(Operator.emp_id).label('count')
            ).group_by(Operator.status)
            
            status_result = await self.session.execute(status_stmt)
            status_counts = {row.status: row.count for row in status_result.all()}
            
            # Get operator counts by skill level
            skill_stmt = select(
                Operator.skill_level,
                func.count(Operator.emp_id).label('count')
            ).where(Operator.status == "ACTIVE").group_by(Operator.skill_level)
            
            skill_result = await self.session.execute(skill_stmt)
            skill_counts = {row.skill_level or 'Unknown': row.count for row in skill_result.all()}
            
            # Get operator counts by department
            dept_stmt = select(
                Operator.department,
                func.count(Operator.emp_id).label('count')
            ).where(Operator.status == "ACTIVE").group_by(Operator.department)
            
            dept_result = await self.session.execute(dept_stmt)
            dept_counts = {row.department or 'Unknown': row.count for row in dept_result.all()}
            
            summary = {
                'total_operators': sum(status_counts.values()),
                'status_breakdown': status_counts,
                'skill_level_breakdown': skill_counts,
                'department_breakdown': dept_counts
            }
            
            logger.debug(f"Generated operator summary statistics: {summary['total_operators']} total operators")
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get operator summary statistics: {e}")
            raise