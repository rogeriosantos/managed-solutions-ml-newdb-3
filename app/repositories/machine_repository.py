"""
Machine Repository Module

This module provides data access operations for Machine and JobLogOB entities,
including specialized queries for downtime analysis, OEE calculations, and
machine performance metrics.
"""

from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, case, desc, asc
from sqlalchemy.orm import selectinload
import logging

from app.models.database_models import Machine, JobLogOB, Operator, Job, Part
from app.repositories.base_repository import (
    BaseRepository, FilterCondition, FilterOperator, 
    PaginationParams, PaginatedResult
)

logger = logging.getLogger(__name__)


class MachineRepository(BaseRepository[Machine]):
    """
    Repository for Machine entity with specialized queries for CNC machine operations.
    
    Provides methods for machine data retrieval, downtime analysis, OEE calculations,
    and performance statistics.
    """
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, Machine)
    
    def get_primary_key_field(self) -> str:
        """Get the primary key field name for Machine model."""
        return "machine_id"
    
    # Machine-specific CRUD operations
    
    async def get_machine_by_id_with_relationships(self, machine_id: str) -> Optional[Machine]:
        """
        Get machine by ID with all related job logs loaded.
        
        Args:
            machine_id: Machine identifier
            
        Returns:
            Optional[Machine]: Machine with relationships or None if not found
        """
        try:
            stmt = (select(Machine)
                   .options(selectinload(Machine.job_logs))
                   .where(Machine.machine_id == machine_id))
            
            result = await self.session.execute(stmt)
            machine = result.scalar_one_or_none()
            
            if machine:
                logger.debug(f"Retrieved machine {machine_id} with {len(machine.job_logs)} job logs")
            
            return machine
        except Exception as e:
            logger.error(f"Failed to get machine {machine_id} with relationships: {e}")
            raise
    
    async def get_active_machines(self) -> List[Machine]:
        """
        Get all active machines.
        
        Returns:
            List[Machine]: List of active machines
        """
        filters = [FilterCondition("status", FilterOperator.EQ, "ACTIVE")]
        return await self.get_all(filters=filters, order_by="machine_name")
    
    # Job log data retrieval methods
    
    async def get_machine_job_logs(self,
                                  machine_id: str,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None,
                                  pagination: Optional[PaginationParams] = None) -> Union[List[JobLogOB], PaginatedResult]:
        """
        Get job logs for a specific machine within a date range.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            pagination: Pagination parameters (optional)
            
        Returns:
            Union[List[JobLogOB], PaginatedResult]: Job logs or paginated results
        """
        try:
            # Build base query
            stmt = (select(JobLogOB)
                   .options(
                       selectinload(JobLogOB.machine_ref),
                       selectinload(JobLogOB.operator_ref),
                       selectinload(JobLogOB.job_ref),
                       selectinload(JobLogOB.part_ref)
                   )
                   .where(JobLogOB.machine == machine_id))
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            # Order by start time (most recent first)
            stmt = stmt.order_by(desc(JobLogOB.start_time))
            
            if pagination:
                # Get total count
                count_stmt = (select(func.count())
                             .select_from(JobLogOB)
                             .where(JobLogOB.machine == machine_id))
                
                if start_date:
                    count_stmt = count_stmt.where(JobLogOB.start_time >= start_date)
                if end_date:
                    count_stmt = count_stmt.where(JobLogOB.start_time <= end_date)
                
                count_result = await self.session.execute(count_stmt)
                total_count = count_result.scalar()
                
                # Apply pagination
                stmt = stmt.offset(pagination.offset).limit(pagination.limit)
                
                result = await self.session.execute(stmt)
                job_logs = result.scalars().all()
                
                logger.debug(f"Retrieved {len(job_logs)}/{total_count} job logs for machine {machine_id}")
                
                return PaginatedResult(
                    items=list(job_logs),
                    total_count=total_count,
                    pagination=pagination
                )
            else:
                result = await self.session.execute(stmt)
                job_logs = result.scalars().all()
                
                logger.debug(f"Retrieved {len(job_logs)} job logs for machine {machine_id}")
                return list(job_logs)
                
        except Exception as e:
            logger.error(f"Failed to get job logs for machine {machine_id}: {e}")
            raise
    
    # Downtime analysis methods
    
    async def get_machine_downtime_summary(self,
                                          machine_id: str,
                                          start_date: Optional[datetime] = None,
                                          end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get comprehensive downtime summary for a machine.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Downtime summary with totals and breakdowns
        """
        try:
            # Build query for downtime aggregation
            stmt = select(
                func.count(JobLogOB.id).label('total_records'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.sum(JobLogOB.job_duration).label('total_job_duration'),
                func.sum(JobLogOB.setup_time).label('total_setup_time'),
                func.sum(JobLogOB.waiting_setup_time).label('total_waiting_setup_time'),
                func.sum(JobLogOB.not_feeding_time).label('total_not_feeding_time'),
                func.sum(JobLogOB.adjustment_time).label('total_adjustment_time'),
                func.sum(JobLogOB.dressing_time).label('total_dressing_time'),
                func.sum(JobLogOB.tooling_time).label('total_tooling_time'),
                func.sum(JobLogOB.engineering_time).label('total_engineering_time'),
                func.sum(JobLogOB.maintenance_time).label('total_maintenance_time'),
                func.sum(JobLogOB.buy_in_time).label('total_buy_in_time'),
                func.sum(JobLogOB.break_shift_change_time).label('total_break_shift_change_time'),
                func.sum(JobLogOB.idle_time).label('total_idle_time'),
                func.sum(JobLogOB.parts_produced).label('total_parts_produced')
            ).where(JobLogOB.machine == machine_id)
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            result = await self.session.execute(stmt)
            row = result.first()
            
            if not row or row.total_records == 0:
                return {
                    'machine_id': machine_id,
                    'period': {
                        'start_date': start_date.isoformat() if start_date else None,
                        'end_date': end_date.isoformat() if end_date else None
                    },
                    'summary': {
                        'total_records': 0,
                        'total_running_time': 0,
                        'total_job_duration': 0,
                        'total_downtime': 0,
                        'total_parts_produced': 0
                    },
                    'downtime_breakdown': {},
                    'efficiency_metrics': {}
                }
            
            # Calculate totals
            total_running_time = row.total_running_time or 0
            total_job_duration = row.total_job_duration or 0
            
            # Calculate downtime breakdown
            downtime_breakdown = {
                'setup_time': row.total_setup_time or 0,
                'waiting_setup_time': row.total_waiting_setup_time or 0,
                'not_feeding_time': row.total_not_feeding_time or 0,
                'adjustment_time': row.total_adjustment_time or 0,
                'dressing_time': row.total_dressing_time or 0,
                'tooling_time': row.total_tooling_time or 0,
                'engineering_time': row.total_engineering_time or 0,
                'maintenance_time': row.total_maintenance_time or 0,
                'buy_in_time': row.total_buy_in_time or 0,
                'break_shift_change_time': row.total_break_shift_change_time or 0,
                'idle_time': row.total_idle_time or 0
            }
            
            total_downtime = sum(downtime_breakdown.values())
            total_parts_produced = row.total_parts_produced or 0
            
            # Calculate efficiency metrics
            efficiency_metrics = {}
            if total_job_duration > 0:
                efficiency_metrics['overall_efficiency'] = total_running_time / total_job_duration
                efficiency_metrics['downtime_percentage'] = total_downtime / total_job_duration
            else:
                efficiency_metrics['overall_efficiency'] = 0.0
                efficiency_metrics['downtime_percentage'] = 0.0
            
            if total_running_time > 0:
                efficiency_metrics['parts_per_hour'] = total_parts_produced / (total_running_time / 3600)
            else:
                efficiency_metrics['parts_per_hour'] = 0.0
            
            # Calculate downtime percentages
            downtime_percentages = {}
            if total_downtime > 0:
                for category, time_value in downtime_breakdown.items():
                    downtime_percentages[f"{category}_percentage"] = time_value / total_downtime
            
            summary = {
                'machine_id': machine_id,
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'summary': {
                    'total_records': row.total_records,
                    'total_running_time': total_running_time,
                    'total_job_duration': total_job_duration,
                    'total_downtime': total_downtime,
                    'total_parts_produced': total_parts_produced
                },
                'downtime_breakdown': downtime_breakdown,
                'downtime_percentages': downtime_percentages,
                'efficiency_metrics': efficiency_metrics
            }
            
            logger.debug(f"Generated downtime summary for machine {machine_id}: "
                        f"{total_downtime}s total downtime from {row.total_records} records")
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get downtime summary for machine {machine_id}: {e}")
            raise
    
    async def get_downtime_trends(self,
                                 machine_id: str,
                                 start_date: datetime,
                                 end_date: datetime,
                                 interval: str = 'daily') -> List[Dict[str, Any]]:
        """
        Get downtime trends over time for a machine.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date for trend analysis
            end_date: End date for trend analysis
            interval: Time interval ('daily', 'weekly', 'monthly')
            
        Returns:
            List[Dict[str, Any]]: Time series data of downtime trends
        """
        try:
            # Determine date truncation based on interval
            if interval == 'daily':
                date_trunc = func.date(JobLogOB.start_time)
            elif interval == 'weekly':
                # PostgreSQL/MySQL week truncation
                date_trunc = func.date_sub(
                    func.date(JobLogOB.start_time),
                    func.interval(func.weekday(JobLogOB.start_time), 'DAY')
                )
            elif interval == 'monthly':
                date_trunc = func.date_format(JobLogOB.start_time, '%Y-%m-01')
            else:
                raise ValueError(f"Unsupported interval: {interval}")
            
            stmt = select(
                date_trunc.label('period'),
                func.count(JobLogOB.id).label('record_count'),
                func.sum(JobLogOB.running_time).label('running_time'),
                func.sum(JobLogOB.setup_time + JobLogOB.waiting_setup_time + 
                        JobLogOB.not_feeding_time + JobLogOB.adjustment_time +
                        JobLogOB.dressing_time + JobLogOB.tooling_time +
                        JobLogOB.engineering_time + JobLogOB.maintenance_time +
                        JobLogOB.buy_in_time + JobLogOB.break_shift_change_time +
                        JobLogOB.idle_time).label('total_downtime'),
                func.sum(JobLogOB.parts_produced).label('parts_produced')
            ).where(
                and_(
                    JobLogOB.machine == machine_id,
                    JobLogOB.start_time >= start_date,
                    JobLogOB.start_time <= end_date
                )
            ).group_by(date_trunc).order_by(date_trunc)
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            trends = []
            for row in rows:
                running_time = row.running_time or 0
                total_downtime = row.total_downtime or 0
                parts_produced = row.parts_produced or 0
                
                efficiency = 0.0
                if (running_time + total_downtime) > 0:
                    efficiency = running_time / (running_time + total_downtime)
                
                trends.append({
                    'period': str(row.period),
                    'record_count': row.record_count,
                    'running_time': running_time,
                    'total_downtime': total_downtime,
                    'parts_produced': parts_produced,
                    'efficiency': efficiency
                })
            
            logger.debug(f"Generated {len(trends)} trend points for machine {machine_id} "
                        f"({interval} interval)")
            
            return trends
            
        except Exception as e:
            logger.error(f"Failed to get downtime trends for machine {machine_id}: {e}")
            raise
    
    # OEE calculation methods
    
    async def calculate_machine_oee(self,
                                   machine_id: str,
                                   start_date: Optional[datetime] = None,
                                   end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Calculate Overall Equipment Effectiveness (OEE) for a machine.
        
        OEE = Availability × Performance × Quality
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: OEE metrics and component calculations
        """
        try:
            # Get machine information for ideal rates
            machine = await self.get_by_id(machine_id)
            if not machine:
                raise ValueError(f"Machine {machine_id} not found")
            
            # Get downtime summary for the period
            downtime_summary = await self.get_machine_downtime_summary(
                machine_id, start_date, end_date
            )
            
            summary = downtime_summary['summary']
            total_running_time = summary['total_running_time']
            total_job_duration = summary['total_job_duration']
            total_downtime = summary['total_downtime']
            total_parts_produced = summary['total_parts_produced']
            
            # Calculate planned production time (assuming 24/7 operation for now)
            if start_date and end_date:
                planned_production_time = (end_date - start_date).total_seconds()
            else:
                # Use job duration as planned time if no date range specified
                planned_production_time = total_job_duration
            
            # Calculate OEE components
            
            # 1. Availability = (Planned Production Time - Downtime) / Planned Production Time
            availability = 0.0
            if planned_production_time > 0:
                availability = max(0.0, (planned_production_time - total_downtime) / planned_production_time)
            
            # 2. Performance = (Total Count / Run Time) / Ideal Run Rate
            # For now, we'll use a simplified calculation: actual vs theoretical production
            performance = 0.0
            if total_running_time > 0 and total_parts_produced > 0:
                # Assume ideal cycle time from part standards or use a default
                # This would ideally come from part specifications
                actual_rate = total_parts_produced / (total_running_time / 3600)  # parts per hour
                
                # Use a theoretical ideal rate (this should come from machine/part specs)
                # For now, use a conservative estimate based on actual performance
                theoretical_rate = actual_rate * 1.2  # Assume 20% improvement potential
                performance = min(1.0, actual_rate / theoretical_rate)
            
            # 3. Quality = Good Count / Total Count
            # For now, assume all produced parts are good (would need quality data)
            quality = 1.0 if total_parts_produced > 0 else 0.0
            
            # Calculate overall OEE
            oee = availability * performance * quality
            
            oee_metrics = {
                'machine_id': machine_id,
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'oee_components': {
                    'availability': availability,
                    'performance': performance,
                    'quality': quality
                },
                'oee_score': oee,
                'oee_percentage': oee * 100,
                'calculations': {
                    'planned_production_time': planned_production_time,
                    'actual_production_time': total_running_time,
                    'downtime': total_downtime,
                    'parts_produced': total_parts_produced,
                    'production_rate_per_hour': total_parts_produced / (total_running_time / 3600) if total_running_time > 0 else 0
                },
                'classification': self._classify_oee_score(oee)
            }
            
            logger.debug(f"Calculated OEE for machine {machine_id}: {oee:.3f} "
                        f"(A:{availability:.3f} × P:{performance:.3f} × Q:{quality:.3f})")
            
            return oee_metrics
            
        except Exception as e:
            logger.error(f"Failed to calculate OEE for machine {machine_id}: {e}")
            raise
    
    def _classify_oee_score(self, oee: float) -> Dict[str, str]:
        """
        Classify OEE score according to industry standards.
        
        Args:
            oee: OEE score (0.0 to 1.0)
            
        Returns:
            Dict[str, str]: Classification and description
        """
        if oee >= 0.85:
            return {
                'level': 'World Class',
                'description': 'Excellent performance, world-class manufacturing'
            }
        elif oee >= 0.60:
            return {
                'level': 'Acceptable',
                'description': 'Good performance, room for improvement'
            }
        elif oee >= 0.40:
            return {
                'level': 'Low',
                'description': 'Poor performance, significant improvement needed'
            }
        else:
            return {
                'level': 'Unacceptable',
                'description': 'Very poor performance, immediate action required'
            }
    
    # Performance statistics methods
    
    async def get_machine_performance_statistics(self,
                                               machine_id: str,
                                               start_date: Optional[datetime] = None,
                                               end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get comprehensive performance statistics for a machine.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Performance statistics and metrics
        """
        try:
            # Get basic statistics
            stmt = select(
                func.count(JobLogOB.id).label('total_jobs'),
                func.avg(JobLogOB.running_time).label('avg_running_time'),
                func.avg(JobLogOB.job_duration).label('avg_job_duration'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_job'),
                func.max(JobLogOB.parts_produced).label('max_parts_per_job'),
                func.min(JobLogOB.parts_produced).label('min_parts_per_job'),
                func.count(func.distinct(JobLogOB.emp_id)).label('unique_operators'),
                func.count(func.distinct(JobLogOB.job_number)).label('unique_jobs'),
                func.count(func.distinct(JobLogOB.part_number)).label('unique_parts')
            ).where(JobLogOB.machine == machine_id)
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            result = await self.session.execute(stmt)
            stats = result.first()
            
            if not stats or stats.total_jobs == 0:
                return {
                    'machine_id': machine_id,
                    'period': {
                        'start_date': start_date.isoformat() if start_date else None,
                        'end_date': end_date.isoformat() if end_date else None
                    },
                    'statistics': {},
                    'message': 'No data available for the specified period'
                }
            
            # Get top operators by job count
            top_operators_stmt = select(
                JobLogOB.emp_id,
                JobLogOB.operator_name,
                func.count(JobLogOB.id).label('job_count'),
                func.avg(JobLogOB.running_time).label('avg_running_time'),
                func.sum(JobLogOB.parts_produced).label('total_parts')
            ).where(JobLogOB.machine == machine_id)
            
            if start_date:
                top_operators_stmt = top_operators_stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                top_operators_stmt = top_operators_stmt.where(JobLogOB.start_time <= end_date)
            
            top_operators_stmt = (top_operators_stmt
                                 .group_by(JobLogOB.emp_id, JobLogOB.operator_name)
                                 .order_by(desc(func.count(JobLogOB.id)))
                                 .limit(5))
            
            operators_result = await self.session.execute(top_operators_stmt)
            top_operators = [
                {
                    'emp_id': row.emp_id,
                    'operator_name': row.operator_name,
                    'job_count': row.job_count,
                    'avg_running_time': float(row.avg_running_time or 0),
                    'total_parts': row.total_parts or 0
                }
                for row in operators_result.all()
            ]
            
            # Get most common parts
            top_parts_stmt = select(
                JobLogOB.part_number,
                func.count(JobLogOB.id).label('job_count'),
                func.sum(JobLogOB.parts_produced).label('total_produced'),
                func.avg(JobLogOB.running_time).label('avg_running_time')
            ).where(JobLogOB.machine == machine_id)
            
            if start_date:
                top_parts_stmt = top_parts_stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                top_parts_stmt = top_parts_stmt.where(JobLogOB.start_time <= end_date)
            
            top_parts_stmt = (top_parts_stmt
                             .group_by(JobLogOB.part_number)
                             .order_by(desc(func.count(JobLogOB.id)))
                             .limit(5))
            
            parts_result = await self.session.execute(top_parts_stmt)
            top_parts = [
                {
                    'part_number': row.part_number,
                    'job_count': row.job_count,
                    'total_produced': row.total_produced or 0,
                    'avg_running_time': float(row.avg_running_time or 0)
                }
                for row in parts_result.all()
            ]
            
            performance_stats = {
                'machine_id': machine_id,
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'statistics': {
                    'total_jobs': stats.total_jobs,
                    'avg_running_time': float(stats.avg_running_time or 0),
                    'avg_job_duration': float(stats.avg_job_duration or 0),
                    'avg_parts_per_job': float(stats.avg_parts_per_job or 0),
                    'max_parts_per_job': stats.max_parts_per_job or 0,
                    'min_parts_per_job': stats.min_parts_per_job or 0,
                    'unique_operators': stats.unique_operators,
                    'unique_jobs': stats.unique_jobs,
                    'unique_parts': stats.unique_parts
                },
                'top_operators': top_operators,
                'top_parts': top_parts
            }
            
            logger.debug(f"Generated performance statistics for machine {machine_id}: "
                        f"{stats.total_jobs} jobs, {stats.unique_operators} operators")
            
            return performance_stats
            
        except Exception as e:
            logger.error(f"Failed to get performance statistics for machine {machine_id}: {e}")
            raise
    
    # Utility methods
    
    async def get_machine_utilization(self,
                                     machine_id: str,
                                     start_date: datetime,
                                     end_date: datetime) -> Dict[str, Any]:
        """
        Calculate machine utilization metrics for a given period.
        
        Args:
            machine_id: Machine identifier
            start_date: Start date for calculation
            end_date: End date for calculation
            
        Returns:
            Dict[str, Any]: Utilization metrics
        """
        try:
            total_period_seconds = (end_date - start_date).total_seconds()
            
            # Get actual usage time
            stmt = select(
                func.sum(JobLogOB.job_duration).label('total_usage_time'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.count(JobLogOB.id).label('total_jobs')
            ).where(
                and_(
                    JobLogOB.machine == machine_id,
                    JobLogOB.start_time >= start_date,
                    JobLogOB.start_time <= end_date
                )
            )
            
            result = await self.session.execute(stmt)
            row = result.first()
            
            total_usage_time = row.total_usage_time or 0
            total_running_time = row.total_running_time or 0
            total_jobs = row.total_jobs or 0
            
            utilization_metrics = {
                'machine_id': machine_id,
                'period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'total_period_hours': total_period_seconds / 3600
                },
                'utilization': {
                    'total_usage_time': total_usage_time,
                    'total_running_time': total_running_time,
                    'total_jobs': total_jobs,
                    'usage_percentage': (total_usage_time / total_period_seconds) * 100 if total_period_seconds > 0 else 0,
                    'efficiency_percentage': (total_running_time / total_usage_time) * 100 if total_usage_time > 0 else 0
                }
            }
            
            return utilization_metrics
            
        except Exception as e:
            logger.error(f"Failed to calculate utilization for machine {machine_id}: {e}")
            raise