"""
Part Repository Module

This module provides data access operations for Part entities,
including material-based queries and production history tracking.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc, case
from sqlalchemy.orm import selectinload
import logging

from app.models.database_models import Part, JobLogOB, Machine, Operator, Job
from app.repositories.base_repository import (
    BaseRepository, FilterCondition, FilterOperator, 
    PaginationParams, PaginatedResult
)

logger = logging.getLogger(__name__)


class PartRepository(BaseRepository[Part]):
    """
    Repository for Part entity with specialized queries for production tracking.
    
    Provides methods for part data retrieval, material-based filtering,
    and production history analysis.
    """
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, Part)
    
    def get_primary_key_field(self) -> str:
        """Get the primary key field name for Part model."""
        return "part_number"
    
    # Part-specific CRUD operations
    
    async def get_part_by_number_with_relationships(self, part_number: str) -> Optional[Part]:
        """
        Get part by number with all related job logs loaded.
        
        Args:
            part_number: Part number identifier
            
        Returns:
            Optional[Part]: Part with relationships or None if not found
        """
        try:
            stmt = (select(Part)
                   .options(selectinload(Part.job_logs))
                   .where(Part.part_number == part_number))
            
            result = await self.session.execute(stmt)
            part = result.scalar_one_or_none()
            
            if part:
                logger.debug(f"Retrieved part {part_number} with {len(part.job_logs)} job logs")
            
            return part
        except Exception as e:
            logger.error(f"Failed to get part {part_number} with relationships: {e}")
            raise
    
    async def get_parts_by_material_type(self, material_type: str) -> List[Part]:
        """
        Get parts filtered by material type.
        
        Args:
            material_type: Material type (e.g., 'Steel', 'Aluminum', 'Plastic')
            
        Returns:
            List[Part]: List of parts with specified material type
        """
        try:
            filters = [FilterCondition("material_type", FilterOperator.EQ, material_type)]
            parts = await self.get_all(filters=filters, order_by="part_name")
            
            logger.debug(f"Retrieved {len(parts)} parts with material type {material_type}")
            return parts
            
        except Exception as e:
            logger.error(f"Failed to get parts by material type {material_type}: {e}")
            raise
    
    async def get_parts_by_material_hardness(self, material_hardness: str) -> List[Part]:
        """
        Get parts filtered by material hardness.
        
        Args:
            material_hardness: Material hardness specification
            
        Returns:
            List[Part]: List of parts with specified material hardness
        """
        try:
            filters = [FilterCondition("material_hardness", FilterOperator.EQ, material_hardness)]
            parts = await self.get_all(filters=filters, order_by="part_name")
            
            logger.debug(f"Retrieved {len(parts)} parts with material hardness {material_hardness}")
            return parts
            
        except Exception as e:
            logger.error(f"Failed to get parts by material hardness {material_hardness}: {e}")
            raise
    
    async def get_parts_by_tolerance_class(self, tolerance_class: str) -> List[Part]:
        """
        Get parts filtered by tolerance class.
        
        Args:
            tolerance_class: Tolerance class specification
            
        Returns:
            List[Part]: List of parts with specified tolerance class
        """
        try:
            filters = [FilterCondition("tolerance_class", FilterOperator.EQ, tolerance_class)]
            parts = await self.get_all(filters=filters, order_by="part_name")
            
            logger.debug(f"Retrieved {len(parts)} parts with tolerance class {tolerance_class}")
            return parts
            
        except Exception as e:
            logger.error(f"Failed to get parts by tolerance class {tolerance_class}: {e}")
            raise
    
    async def search_parts_by_dimensions(self,
                                       min_length: Optional[float] = None,
                                       max_length: Optional[float] = None,
                                       min_width: Optional[float] = None,
                                       max_width: Optional[float] = None,
                                       min_height: Optional[float] = None,
                                       max_height: Optional[float] = None) -> List[Part]:
        """
        Search parts by dimensional constraints.
        
        Args:
            min_length: Minimum length constraint
            max_length: Maximum length constraint
            min_width: Minimum width constraint
            max_width: Maximum width constraint
            min_height: Minimum height constraint
            max_height: Maximum height constraint
            
        Returns:
            List[Part]: List of parts matching dimensional constraints
        """
        try:
            filters = []
            
            if min_length is not None:
                filters.append(FilterCondition("dimensions_length", FilterOperator.GTE, min_length))
            if max_length is not None:
                filters.append(FilterCondition("dimensions_length", FilterOperator.LTE, max_length))
            if min_width is not None:
                filters.append(FilterCondition("dimensions_width", FilterOperator.GTE, min_width))
            if max_width is not None:
                filters.append(FilterCondition("dimensions_width", FilterOperator.LTE, max_width))
            if min_height is not None:
                filters.append(FilterCondition("dimensions_height", FilterOperator.GTE, min_height))
            if max_height is not None:
                filters.append(FilterCondition("dimensions_height", FilterOperator.LTE, max_height))
            
            parts = await self.get_all(filters=filters, order_by="part_name")
            
            logger.debug(f"Retrieved {len(parts)} parts matching dimensional constraints")
            return parts
            
        except Exception as e:
            logger.error(f"Failed to search parts by dimensions: {e}")
            raise
    
    # Production history and analysis methods
    
    async def get_part_production_history(self,
                                        part_number: str,
                                        start_date: Optional[datetime] = None,
                                        end_date: Optional[datetime] = None,
                                        pagination: Optional[PaginationParams] = None) -> Dict[str, Any]:
        """
        Get comprehensive production history for a part.
        
        Args:
            part_number: Part number identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            pagination: Pagination parameters (optional)
            
        Returns:
            Dict[str, Any]: Production history with summary and details
        """
        try:
            # Get part information
            part = await self.get_by_id(part_number)
            if not part:
                raise ValueError(f"Part {part_number} not found")
            
            # Build summary query
            summary_stmt = select(
                func.count(JobLogOB.id).label('total_operations'),
                func.sum(JobLogOB.parts_produced).label('total_parts_produced'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.sum(JobLogOB.job_duration).label('total_job_duration'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_operation'),
                func.avg(JobLogOB.running_time).label('avg_running_time'),
                func.count(func.distinct(JobLogOB.machine)).label('machines_used'),
                func.count(func.distinct(JobLogOB.emp_id)).label('operators_involved'),
                func.count(func.distinct(JobLogOB.job_number)).label('jobs_involved'),
                func.min(JobLogOB.start_time).label('first_production'),
                func.max(JobLogOB.start_time).label('last_production')
            ).where(JobLogOB.part_number == part_number)
            
            # Apply date filters
            if start_date:
                summary_stmt = summary_stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                summary_stmt = summary_stmt.where(JobLogOB.start_time <= end_date)
            
            summary_result = await self.session.execute(summary_stmt)
            summary_row = summary_result.first()
            
            if not summary_row or summary_row.total_operations == 0:
                return {
                    'part_number': part_number,
                    'part_info': {
                        'part_name': part.part_name,
                        'material_type': part.material_type,
                        'standard_cycle_time': part.standard_cycle_time
                    },
                    'production_summary': {},
                    'message': 'No production history available for this part'
                }
            
            # Calculate performance metrics
            total_running_time = summary_row.total_running_time or 0
            total_job_duration = summary_row.total_job_duration or 0
            total_parts_produced = summary_row.total_parts_produced or 0
            
            efficiency = 0.0
            if total_job_duration > 0:
                efficiency = total_running_time / total_job_duration
            
            actual_cycle_time = 0.0
            if total_parts_produced > 0 and total_running_time > 0:
                actual_cycle_time = total_running_time / total_parts_produced
            
            cycle_time_variance = 0.0
            if part.standard_cycle_time and actual_cycle_time > 0:
                cycle_time_variance = ((actual_cycle_time - part.standard_cycle_time) / part.standard_cycle_time) * 100
            
            production_history = {
                'part_number': part_number,
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'part_info': {
                    'part_name': part.part_name,
                    'part_description': part.part_description,
                    'material_type': part.material_type,
                    'material_hardness': part.material_hardness,
                    'weight': part.weight,
                    'dimensions': {
                        'length': part.dimensions_length,
                        'width': part.dimensions_width,
                        'height': part.dimensions_height
                    },
                    'tolerance_class': part.tolerance_class,
                    'surface_finish': part.surface_finish,
                    'standard_cycle_time': part.standard_cycle_time,
                    'cost_per_unit': part.cost_per_unit
                },
                'production_summary': {
                    'total_operations': summary_row.total_operations,
                    'total_parts_produced': total_parts_produced,
                    'total_running_time': total_running_time,
                    'total_job_duration': total_job_duration,
                    'avg_parts_per_operation': float(summary_row.avg_parts_per_operation or 0),
                    'avg_running_time': float(summary_row.avg_running_time or 0),
                    'machines_used': summary_row.machines_used,
                    'operators_involved': summary_row.operators_involved,
                    'jobs_involved': summary_row.jobs_involved,
                    'efficiency': efficiency,
                    'actual_cycle_time': actual_cycle_time,
                    'cycle_time_variance_percentage': cycle_time_variance,
                    'first_production': summary_row.first_production.isoformat() if summary_row.first_production else None,
                    'last_production': summary_row.last_production.isoformat() if summary_row.last_production else None
                }
            }
            
            # Add detailed production records if pagination is provided
            if pagination:
                production_history['production_details'] = await self._get_paginated_production_details(
                    part_number, start_date, end_date, pagination
                )
            
            # Add machine performance breakdown
            production_history['machine_performance'] = await self._get_part_machine_performance(
                part_number, start_date, end_date
            )
            
            logger.debug(f"Generated production history for part {part_number}: "
                        f"{total_parts_produced} parts produced in {summary_row.total_operations} operations")
            
            return production_history
            
        except Exception as e:
            logger.error(f"Failed to get production history for part {part_number}: {e}")
            raise
    
    async def _get_paginated_production_details(self,
                                              part_number: str,
                                              start_date: Optional[datetime],
                                              end_date: Optional[datetime],
                                              pagination: PaginationParams) -> PaginatedResult:
        """
        Get paginated production details for a part.
        
        Args:
            part_number: Part number identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            pagination: Pagination parameters
            
        Returns:
            PaginatedResult: Paginated production details
        """
        try:
            # Build detail query
            detail_stmt = select(
                JobLogOB.id,
                JobLogOB.machine,
                JobLogOB.emp_id,
                JobLogOB.operator_name,
                JobLogOB.job_number,
                JobLogOB.start_time,
                JobLogOB.end_time,
                JobLogOB.running_time,
                JobLogOB.job_duration,
                JobLogOB.parts_produced,
                JobLogOB.state
            ).where(JobLogOB.part_number == part_number)
            
            # Apply date filters
            if start_date:
                detail_stmt = detail_stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                detail_stmt = detail_stmt.where(JobLogOB.start_time <= end_date)
            
            # Get total count
            count_stmt = select(func.count()).select_from(JobLogOB).where(JobLogOB.part_number == part_number)
            if start_date:
                count_stmt = count_stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                count_stmt = count_stmt.where(JobLogOB.start_time <= end_date)
            
            count_result = await self.session.execute(count_stmt)
            total_count = count_result.scalar()
            
            # Apply ordering and pagination
            detail_stmt = (detail_stmt.order_by(desc(JobLogOB.start_time))
                          .offset(pagination.offset)
                          .limit(pagination.limit))
            
            detail_result = await self.session.execute(detail_stmt)
            detail_rows = detail_result.all()
            
            # Format production details
            production_details = []
            for row in detail_rows:
                efficiency = 0.0
                if row.job_duration and row.job_duration > 0:
                    efficiency = (row.running_time or 0) / row.job_duration
                
                cycle_time = 0.0
                if row.parts_produced and row.parts_produced > 0 and row.running_time:
                    cycle_time = row.running_time / row.parts_produced
                
                production_details.append({
                    'operation_id': row.id,
                    'machine': row.machine,
                    'operator': {
                        'emp_id': row.emp_id,
                        'name': row.operator_name
                    },
                    'job_number': row.job_number,
                    'start_time': row.start_time.isoformat() if row.start_time else None,
                    'end_time': row.end_time.isoformat() if row.end_time else None,
                    'running_time': row.running_time or 0,
                    'job_duration': row.job_duration or 0,
                    'parts_produced': row.parts_produced or 0,
                    'state': row.state,
                    'efficiency': efficiency,
                    'cycle_time': cycle_time
                })
            
            return PaginatedResult(
                items=production_details,
                total_count=total_count,
                pagination=pagination
            )
            
        except Exception as e:
            logger.error(f"Failed to get paginated production details for part {part_number}: {e}")
            raise
    
    async def _get_part_machine_performance(self,
                                          part_number: str,
                                          start_date: Optional[datetime] = None,
                                          end_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get part production performance breakdown by machine.
        
        Args:
            part_number: Part number identifier
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            List[Dict[str, Any]]: Performance metrics by machine
        """
        try:
            stmt = select(
                JobLogOB.machine,
                func.count(JobLogOB.id).label('operation_count'),
                func.sum(JobLogOB.parts_produced).label('parts_produced'),
                func.sum(JobLogOB.running_time).label('running_time'),
                func.sum(JobLogOB.job_duration).label('job_duration'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_operation'),
                func.avg(JobLogOB.running_time).label('avg_running_time')
            ).where(JobLogOB.part_number == part_number)
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            stmt = (stmt.group_by(JobLogOB.machine)
                   .order_by(desc(func.sum(JobLogOB.parts_produced))))
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            machine_performance = []
            for row in rows:
                running_time = row.running_time or 0
                job_duration = row.job_duration or 0
                parts_produced = row.parts_produced or 0
                
                efficiency = 0.0
                if job_duration > 0:
                    efficiency = running_time / job_duration
                
                cycle_time = 0.0
                if parts_produced > 0 and running_time > 0:
                    cycle_time = running_time / parts_produced
                
                productivity = 0.0
                if running_time > 0:
                    productivity = parts_produced / (running_time / 3600)
                
                machine_performance.append({
                    'machine': row.machine,
                    'operation_count': row.operation_count,
                    'parts_produced': parts_produced,
                    'running_time': running_time,
                    'job_duration': job_duration,
                    'avg_parts_per_operation': float(row.avg_parts_per_operation or 0),
                    'avg_running_time': float(row.avg_running_time or 0),
                    'efficiency': efficiency,
                    'avg_cycle_time': cycle_time,
                    'productivity_per_hour': productivity
                })
            
            return machine_performance
            
        except Exception as e:
            logger.error(f"Failed to get machine performance for part {part_number}: {e}")
            raise
    
    async def get_material_analysis(self,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Analyze production metrics by material type.
        
        Args:
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            
        Returns:
            Dict[str, Any]: Production analysis by material type
        """
        try:
            # Build query joining parts with job logs
            stmt = select(
                Part.material_type,
                func.count(func.distinct(Part.part_number)).label('unique_parts'),
                func.count(JobLogOB.id).label('total_operations'),
                func.sum(JobLogOB.parts_produced).label('total_parts_produced'),
                func.sum(JobLogOB.running_time).label('total_running_time'),
                func.sum(JobLogOB.job_duration).label('total_job_duration'),
                func.avg(JobLogOB.parts_produced).label('avg_parts_per_operation'),
                func.avg(JobLogOB.running_time).label('avg_running_time')
            ).select_from(
                Part.__table__.join(JobLogOB, Part.part_number == JobLogOB.part_number)
            ).where(Part.material_type.is_not(None))
            
            # Apply date filters
            if start_date:
                stmt = stmt.where(JobLogOB.start_time >= start_date)
            if end_date:
                stmt = stmt.where(JobLogOB.start_time <= end_date)
            
            stmt = stmt.group_by(Part.material_type).order_by(Part.material_type)
            
            result = await self.session.execute(stmt)
            rows = result.all()
            
            material_analysis = {
                'period': {
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                },
                'material_types': []
            }
            
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
                
                avg_cycle_time = 0.0
                if total_parts_produced > 0 and total_running_time > 0:
                    avg_cycle_time = total_running_time / total_parts_produced
                
                material_analysis['material_types'].append({
                    'material_type': row.material_type,
                    'unique_parts': row.unique_parts,
                    'total_operations': row.total_operations,
                    'total_parts_produced': total_parts_produced,
                    'total_running_time': total_running_time,
                    'total_job_duration': total_job_duration,
                    'avg_parts_per_operation': float(row.avg_parts_per_operation or 0),
                    'avg_running_time': float(row.avg_running_time or 0),
                    'efficiency': efficiency,
                    'productivity_per_hour': productivity,
                    'avg_cycle_time': avg_cycle_time
                })
            
            logger.debug(f"Generated material analysis for {len(rows)} material types")
            
            return material_analysis
            
        except Exception as e:
            logger.error(f"Failed to get material analysis: {e}")
            raise
    
    async def get_part_complexity_analysis(self) -> Dict[str, Any]:
        """
        Analyze parts by complexity metrics (dimensions, tolerance, etc.).
        
        Returns:
            Dict[str, Any]: Part complexity analysis
        """
        try:
            # Get complexity distribution
            complexity_stmt = select(
                case(
                    (Part.tolerance_class.in_(['IT6', 'IT7', 'IT8']), 'High Precision'),
                    (Part.tolerance_class.in_(['IT9', 'IT10', 'IT11']), 'Medium Precision'),
                    (Part.tolerance_class.in_(['IT12', 'IT13', 'IT14']), 'Low Precision'),
                    else_='Unknown'
                ).label('precision_category'),
                func.count(Part.part_number).label('part_count'),
                func.avg(Part.standard_cycle_time).label('avg_cycle_time'),
                func.avg(Part.cost_per_unit).label('avg_cost_per_unit')
            ).group_by('precision_category').order_by('precision_category')
            
            complexity_result = await self.session.execute(complexity_stmt)
            complexity_rows = complexity_result.all()
            
            # Get material hardness distribution
            hardness_stmt = select(
                Part.material_hardness,
                func.count(Part.part_number).label('part_count'),
                func.avg(Part.standard_cycle_time).label('avg_cycle_time')
            ).where(Part.material_hardness.is_not(None)).group_by(Part.material_hardness)
            
            hardness_result = await self.session.execute(hardness_stmt)
            hardness_rows = hardness_result.all()
            
            # Get size distribution
            size_stmt = select(
                case(
                    (and_(Part.dimensions_length <= 50, Part.dimensions_width <= 50, Part.dimensions_height <= 50), 'Small'),
                    (and_(Part.dimensions_length <= 200, Part.dimensions_width <= 200, Part.dimensions_height <= 200), 'Medium'),
                    (and_(Part.dimensions_length > 200, Part.dimensions_width > 200, Part.dimensions_height > 200), 'Large'),
                    else_='Unknown'
                ).label('size_category'),
                func.count(Part.part_number).label('part_count'),
                func.avg(Part.standard_cycle_time).label('avg_cycle_time'),
                func.avg(Part.weight).label('avg_weight')
            ).where(
                and_(
                    Part.dimensions_length.is_not(None),
                    Part.dimensions_width.is_not(None),
                    Part.dimensions_height.is_not(None)
                )
            ).group_by('size_category')
            
            size_result = await self.session.execute(size_stmt)
            size_rows = size_result.all()
            
            complexity_analysis = {
                'precision_distribution': [
                    {
                        'precision_category': row.precision_category,
                        'part_count': row.part_count,
                        'avg_cycle_time': float(row.avg_cycle_time or 0),
                        'avg_cost_per_unit': float(row.avg_cost_per_unit or 0)
                    }
                    for row in complexity_rows
                ],
                'hardness_distribution': [
                    {
                        'material_hardness': row.material_hardness,
                        'part_count': row.part_count,
                        'avg_cycle_time': float(row.avg_cycle_time or 0)
                    }
                    for row in hardness_rows
                ],
                'size_distribution': [
                    {
                        'size_category': row.size_category,
                        'part_count': row.part_count,
                        'avg_cycle_time': float(row.avg_cycle_time or 0),
                        'avg_weight': float(row.avg_weight or 0)
                    }
                    for row in size_rows
                ]
            }
            
            logger.debug("Generated part complexity analysis")
            
            return complexity_analysis
            
        except Exception as e:
            logger.error(f"Failed to get part complexity analysis: {e}")
            raise
    
    # Utility methods
    
    async def get_part_summary_statistics(self) -> Dict[str, Any]:
        """
        Get summary statistics for all parts.
        
        Returns:
            Dict[str, Any]: Summary statistics
        """
        try:
            # Get basic counts
            total_stmt = select(func.count(Part.part_number))
            total_result = await self.session.execute(total_stmt)
            total_parts = total_result.scalar()
            
            # Get material type distribution
            material_stmt = select(
                Part.material_type,
                func.count(Part.part_number).label('count')
            ).where(Part.material_type.is_not(None)).group_by(Part.material_type)
            
            material_result = await self.session.execute(material_stmt)
            material_counts = {row.material_type: row.count for row in material_result.all()}
            
            # Get average metrics
            metrics_stmt = select(
                func.avg(Part.standard_cycle_time).label('avg_cycle_time'),
                func.avg(Part.cost_per_unit).label('avg_cost_per_unit'),
                func.avg(Part.weight).label('avg_weight'),
                func.count(Part.part_number).filter(Part.standard_cycle_time.is_not(None)).label('parts_with_cycle_time'),
                func.count(Part.part_number).filter(Part.cost_per_unit.is_not(None)).label('parts_with_cost')
            )
            
            metrics_result = await self.session.execute(metrics_stmt)
            metrics_row = metrics_result.first()
            
            summary = {
                'total_parts': total_parts,
                'material_type_distribution': material_counts,
                'average_metrics': {
                    'avg_cycle_time': float(metrics_row.avg_cycle_time or 0),
                    'avg_cost_per_unit': float(metrics_row.avg_cost_per_unit or 0),
                    'avg_weight': float(metrics_row.avg_weight or 0)
                },
                'data_completeness': {
                    'parts_with_cycle_time': metrics_row.parts_with_cycle_time,
                    'parts_with_cost': metrics_row.parts_with_cost,
                    'cycle_time_completeness': (metrics_row.parts_with_cycle_time / total_parts) * 100 if total_parts > 0 else 0,
                    'cost_completeness': (metrics_row.parts_with_cost / total_parts) * 100 if total_parts > 0 else 0
                }
            }
            
            logger.debug(f"Generated part summary statistics: {total_parts} total parts")
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get part summary statistics: {e}")
            raise