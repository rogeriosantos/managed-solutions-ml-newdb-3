"""
Machine API Routes

This module provides REST API endpoints for machine management operations,
including CRUD operations and data retrieval with filtering capabilities.
"""

from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_database_session_dependency
from app.services.machine_service import MachineService
from app.models.pydantic_models import (
    MachineCreate, MachineUpdate, MachineResponse,
    MachineDataRequest, MachineDataResponse,
    DowntimeAnalysisRequest, DowntimeAnalysisResponse,
    OEEMetrics, ErrorResponse
)
from app.repositories.base_repository import PaginationParams

router = APIRouter()


@router.get("/machines", response_model=List[MachineResponse])
async def list_machines(
    active_only: bool = Query(True, description="Filter to active machines only"),
    machine_type: Optional[str] = Query(None, description="Filter by machine type"),
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    List all machines with optional filtering.
    
    Args:
        active_only: Whether to return only active machines
        machine_type: Optional machine type filter
        db: Database session
        
    Returns:
        List[MachineResponse]: List of machines
    """
    try:
        machine_service = MachineService(db)
        machines = await machine_service.get_all_machines(
            active_only=active_only,
            machine_type=machine_type
        )
        
        return [MachineResponse.model_validate(machine) for machine in machines]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve machines: {str(e)}"
        )


@router.post("/machines", response_model=MachineResponse, status_code=status.HTTP_201_CREATED)
async def create_machine(
    machine_data: MachineCreate,
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    Create a new machine.
    
    Args:
        machine_data: Machine creation data
        db: Database session
        
    Returns:
        MachineResponse: Created machine
    """
    try:
        machine_service = MachineService(db)
        machine = await machine_service.create_machine(machine_data.model_dump())
        
        return MachineResponse.model_validate(machine)
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create machine: {str(e)}"
        )


@router.get("/machines/{machine_id}", response_model=MachineResponse)
async def get_machine(
    machine_id: str,
    include_relationships: bool = Query(False, description="Include job log relationships"),
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    Get machine details by ID.
    
    Args:
        machine_id: Machine identifier
        include_relationships: Whether to include job log relationships
        db: Database session
        
    Returns:
        MachineResponse: Machine details
    """
    try:
        machine_service = MachineService(db)
        machine = await machine_service.get_machine_by_id(
            machine_id, 
            include_relationships=include_relationships
        )
        
        if not machine:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Machine {machine_id} not found"
            )
        
        return MachineResponse.model_validate(machine)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve machine: {str(e)}"
        )


@router.put("/machines/{machine_id}", response_model=MachineResponse)
async def update_machine(
    machine_id: str,
    machine_data: MachineUpdate,
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    Update machine information.
    
    Args:
        machine_id: Machine identifier
        machine_data: Machine update data
        db: Database session
        
    Returns:
        MachineResponse: Updated machine
    """
    try:
        machine_service = MachineService(db)
        
        # Only include non-None fields in update
        update_data = {k: v for k, v in machine_data.model_dump().items() if v is not None}
        
        if not update_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid fields provided for update"
            )
        
        machine = await machine_service.update_machine(machine_id, update_data)
        
        if not machine:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Machine {machine_id} not found"
            )
        
        return MachineResponse.model_validate(machine)
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update machine: {str(e)}"
        )


@router.delete("/machines/{machine_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_machine(
    machine_id: str,
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    Delete machine (soft delete by setting status to RETIRED).
    
    Args:
        machine_id: Machine identifier
        db: Database session
    """
    try:
        machine_service = MachineService(db)
        deleted = await machine_service.delete_machine(machine_id)
        
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Machine {machine_id} not found"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete machine: {str(e)}"
        )


@router.get("/machines/{machine_id}/data", response_model=MachineDataResponse)
async def get_machine_data(
    machine_id: str,
    start_date: datetime = Query(..., description="Start date for data retrieval"),
    end_date: datetime = Query(..., description="End date for data retrieval"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=1000, description="Number of records per page"),
    include_relationships: bool = Query(True, description="Include related entities"),
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    Get machine operational data with filtering and pagination.
    
    Args:
        machine_id: Machine identifier
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        page: Page number for pagination
        page_size: Number of records per page
        include_relationships: Whether to include related entities
        db: Database session
        
    Returns:
        MachineDataResponse: Paginated machine data
    """
    try:
        # Validate date range
        if start_date >= end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date must be before end date"
            )
        
        machine_service = MachineService(db)
        # Convert page-based pagination to skip-based
        skip = (page - 1) * page_size
        pagination_params = PaginationParams(skip=skip, limit=page_size)
        
        result = await machine_service.get_machine_data(
            machine_id=machine_id,
            start_date=start_date,
            end_date=end_date,
            pagination=pagination_params,
            include_relationships=include_relationships
        )
        
        # Convert to response format
        from app.models.pydantic_models import JobLogResponse
        
        job_logs_data = []
        for job_log in result.items:
            # Calculate derived fields for response
            downtime_fields = [
                'setup_time', 'waiting_setup_time', 'not_feeding_time', 
                'adjustment_time', 'dressing_time', 'tooling_time',
                'engineering_time', 'maintenance_time', 'buy_in_time',
                'break_shift_change_time', 'idle_time'
            ]
            
            total_downtime = sum(getattr(job_log, field, 0) or 0 for field in downtime_fields)
            
            downtime_breakdown = {
                field: getattr(job_log, field, 0) or 0 
                for field in downtime_fields
            }
            
            # Calculate efficiency
            total_time = job_log.job_duration or 0
            running_time = job_log.running_time or 0
            efficiency = (running_time / total_time) if total_time > 0 else 0
            
            job_log_dict = {
                **{k: v for k, v in job_log.__dict__.items() if not k.startswith('_')},
                'total_downtime': total_downtime,
                'downtime_breakdown': downtime_breakdown,
                'efficiency': efficiency
            }
            
            job_logs_data.append(JobLogResponse.model_validate(job_log_dict))
        
        return MachineDataResponse(
            data=job_logs_data,
            total_count=result.total_count,
            page=result.page_number,
            page_size=pagination_params.limit,
            total_pages=result.total_pages
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve machine data: {str(e)}"
        )


@router.get("/machines/{machine_id}/downtime", response_model=DowntimeAnalysisResponse)
async def get_machine_downtime_analysis(
    machine_id: str,
    start_date: datetime = Query(..., description="Start date for analysis"),
    end_date: datetime = Query(..., description="End date for analysis"),
    include_trends: bool = Query(True, description="Include trend analysis"),
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    Get comprehensive downtime analysis for a machine.
    
    Args:
        machine_id: Machine identifier
        start_date: Start date for analysis
        end_date: End date for analysis
        include_trends: Whether to include trend analysis
        db: Database session
        
    Returns:
        DowntimeAnalysisResponse: Downtime analysis results
    """
    try:
        # Validate date range
        if start_date >= end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date must be before end date"
            )
        
        machine_service = MachineService(db)
        analysis = await machine_service.analyze_machine_downtime(
            machine_id=machine_id,
            start_date=start_date,
            end_date=end_date,
            include_trends=include_trends
        )
        
        # Convert to response format
        downtime_summary = analysis.get('downtime_summary', {})
        downtime_insights = analysis.get('downtime_insights', {})
        
        return DowntimeAnalysisResponse(
            machine_id=machine_id,
            analysis_period={
                'start_date': start_date,
                'end_date': end_date
            },
            total_downtime=downtime_summary.get('total_downtime', 0),
            downtime_by_category=downtime_summary.get('downtime_breakdown', {}),
            downtime_trends=analysis.get('downtime_trends', {}),
            recommendations=downtime_insights.get('recommendations', [])
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze machine downtime: {str(e)}"
        )


@router.get("/machines/{machine_id}/oee", response_model=OEEMetrics)
async def get_machine_oee(
    machine_id: str,
    start_date: Optional[datetime] = Query(None, description="Start date for OEE calculation"),
    end_date: Optional[datetime] = Query(None, description="End date for OEE calculation"),
    include_benchmarks: bool = Query(True, description="Include industry benchmarks"),
    db: AsyncSession = Depends(get_database_session_dependency)
):
    """
    Get Overall Equipment Effectiveness (OEE) metrics for a machine.
    
    Args:
        machine_id: Machine identifier
        start_date: Start date for OEE calculation (optional)
        end_date: End date for OEE calculation (optional)
        include_benchmarks: Whether to include industry benchmarks
        db: Database session
        
    Returns:
        OEEMetrics: OEE calculation results
    """
    try:
        # Validate date range if provided
        if start_date and end_date and start_date >= end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date must be before end date"
            )
        
        machine_service = MachineService(db)
        oee_data = await machine_service.calculate_machine_oee(
            machine_id=machine_id,
            start_date=start_date,
            end_date=end_date,
            include_benchmarks=include_benchmarks
        )
        
        # Extract OEE components
        oee_components = oee_data.get('oee_components', {})
        
        return OEEMetrics(
            availability=oee_components.get('availability', 0.0),
            performance=oee_components.get('performance', 0.0),
            quality=oee_components.get('quality', 0.0),
            oee=oee_data.get('oee_score', 0.0)
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate machine OEE: {str(e)}"
        )