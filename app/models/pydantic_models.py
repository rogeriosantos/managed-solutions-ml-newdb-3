"""
Pydantic models for API request/response validation and serialization.

This module contains all Pydantic schemas for data validation, 
request/response models, and business logic validation.
"""

from datetime import datetime, date
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
from enum import Enum


# Enums for validation
class SkillLevel(str, Enum):
    BEGINNER = "BEGINNER"
    INTERMEDIATE = "INTERMEDIATE"
    ADVANCED = "ADVANCED"
    EXPERT = "EXPERT"


class ShiftPreference(str, Enum):
    DAY = "DAY"
    NIGHT = "NIGHT"
    ROTATING = "ROTATING"


class Priority(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    URGENT = "URGENT"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class MachineStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    MAINTENANCE = "MAINTENANCE"


class OperatorStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"


# Base schemas
class BaseSchema(BaseModel):
    """Base schema with common configuration."""
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())


# Machine schemas
class MachineBase(BaseSchema):
    """Base machine schema with common fields."""
    machine_name: str = Field(..., min_length=1, max_length=100)
    machine_type: str = Field(..., min_length=1, max_length=50)
    manufacturer: Optional[str] = Field(None, max_length=100)
    model: Optional[str] = Field(None, max_length=100)
    year_installed: Optional[int] = Field(None, ge=1900, le=2030)
    max_spindle_speed: Optional[int] = Field(None, gt=0)
    max_feed_rate: Optional[float] = Field(None, gt=0)
    work_envelope_x: Optional[float] = Field(None, gt=0)
    work_envelope_y: Optional[float] = Field(None, gt=0)
    work_envelope_z: Optional[float] = Field(None, gt=0)
    maintenance_schedule_hours: Optional[int] = Field(None, gt=0)
    last_maintenance_date: Optional[datetime] = None
    status: MachineStatus = MachineStatus.ACTIVE


class MachineCreate(MachineBase):
    """Schema for creating a new machine."""
    machine_id: str = Field(..., min_length=1, max_length=50)


class MachineUpdate(BaseSchema):
    """Schema for updating machine information."""
    machine_name: Optional[str] = Field(None, min_length=1, max_length=100)
    machine_type: Optional[str] = Field(None, min_length=1, max_length=50)
    manufacturer: Optional[str] = Field(None, max_length=100)
    model: Optional[str] = Field(None, max_length=100)
    year_installed: Optional[int] = Field(None, ge=1900, le=2030)
    max_spindle_speed: Optional[int] = Field(None, gt=0)
    max_feed_rate: Optional[float] = Field(None, gt=0)
    work_envelope_x: Optional[float] = Field(None, gt=0)
    work_envelope_y: Optional[float] = Field(None, gt=0)
    work_envelope_z: Optional[float] = Field(None, gt=0)
    maintenance_schedule_hours: Optional[int] = Field(None, gt=0)
    last_maintenance_date: Optional[datetime] = None
    status: Optional[MachineStatus] = None


class MachineResponse(MachineBase):
    """Schema for machine response data."""
    machine_id: str
    created_at: datetime
    updated_at: datetime


# Operator schemas
class OperatorBase(BaseSchema):
    """Base operator schema with common fields."""
    operator_name: str = Field(..., min_length=1, max_length=100)
    skill_level: Optional[SkillLevel] = None
    hire_date: Optional[date] = None
    shift_preference: Optional[ShiftPreference] = None
    certifications: Optional[str] = Field(None, description="JSON string of certifications")
    hourly_rate: Optional[float] = Field(None, gt=0)
    department: Optional[str] = Field(None, max_length=50)
    supervisor_id: Optional[str] = Field(None, max_length=20)
    status: OperatorStatus = OperatorStatus.ACTIVE


class OperatorCreate(OperatorBase):
    """Schema for creating a new operator."""
    emp_id: str = Field(..., min_length=1, max_length=20)


class OperatorUpdate(BaseSchema):
    """Schema for updating operator information."""
    operator_name: Optional[str] = Field(None, min_length=1, max_length=100)
    skill_level: Optional[SkillLevel] = None
    hire_date: Optional[date] = None
    shift_preference: Optional[ShiftPreference] = None
    certifications: Optional[str] = None
    hourly_rate: Optional[float] = Field(None, gt=0)
    department: Optional[str] = Field(None, max_length=50)
    supervisor_id: Optional[str] = Field(None, max_length=20)
    status: Optional[OperatorStatus] = None


class OperatorResponse(OperatorBase):
    """Schema for operator response data."""
    emp_id: str
    created_at: datetime
    updated_at: datetime


# Job schemas
class JobBase(BaseSchema):
    """Base job schema with common fields."""
    job_name: str = Field(..., min_length=1, max_length=200)
    customer_id: Optional[str] = Field(None, max_length=50)
    customer_name: Optional[str] = Field(None, max_length=200)
    priority: Priority = Priority.NORMAL
    estimated_hours: Optional[float] = Field(None, gt=0)
    actual_hours: Optional[float] = Field(None, ge=0)
    quantity_ordered: int = Field(..., gt=0)
    quantity_completed: int = Field(default=0, ge=0)
    due_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    completion_date: Optional[datetime] = None
    job_status: JobStatus = JobStatus.PENDING
    complexity_rating: Optional[int] = Field(None, ge=1, le=10)
    setup_complexity: Optional[int] = Field(None, ge=1, le=10)

    @field_validator('quantity_completed')
    @classmethod
    def validate_quantity_completed(cls, v, info):
        """Ensure quantity_completed doesn't exceed quantity_ordered."""
        if info.data and 'quantity_ordered' in info.data and v > info.data['quantity_ordered']:
            raise ValueError('quantity_completed cannot exceed quantity_ordered')
        return v


class JobCreate(JobBase):
    """Schema for creating a new job."""
    job_number: str = Field(..., min_length=1, max_length=50)


class JobUpdate(BaseSchema):
    """Schema for updating job information."""
    job_name: Optional[str] = Field(None, min_length=1, max_length=200)
    customer_id: Optional[str] = Field(None, max_length=50)
    customer_name: Optional[str] = Field(None, max_length=200)
    priority: Optional[Priority] = None
    estimated_hours: Optional[float] = Field(None, gt=0)
    actual_hours: Optional[float] = Field(None, ge=0)
    quantity_ordered: Optional[int] = Field(None, gt=0)
    quantity_completed: Optional[int] = Field(None, ge=0)
    due_date: Optional[datetime] = None
    start_date: Optional[datetime] = None
    completion_date: Optional[datetime] = None
    job_status: Optional[JobStatus] = None
    complexity_rating: Optional[int] = Field(None, ge=1, le=10)
    setup_complexity: Optional[int] = Field(None, ge=1, le=10)


class JobResponse(JobBase):
    """Schema for job response data."""
    job_number: str
    created_at: datetime
    updated_at: datetime


# Part schemas
class PartBase(BaseSchema):
    """Base part schema with common fields."""
    part_name: str = Field(..., min_length=1, max_length=200)
    part_description: Optional[str] = None
    material_type: Optional[str] = Field(None, max_length=100)
    material_hardness: Optional[str] = Field(None, max_length=50)
    weight: Optional[float] = Field(None, gt=0)
    dimensions_length: Optional[float] = Field(None, gt=0)
    dimensions_width: Optional[float] = Field(None, gt=0)
    dimensions_height: Optional[float] = Field(None, gt=0)
    tolerance_class: Optional[str] = Field(None, max_length=20)
    surface_finish: Optional[str] = Field(None, max_length=50)
    standard_cycle_time: Optional[int] = Field(None, gt=0, description="Standard cycle time in seconds")
    setup_time_standard: Optional[int] = Field(None, gt=0, description="Standard setup time in seconds")
    tooling_requirements: Optional[str] = Field(None, description="JSON string of tooling requirements")
    quality_requirements: Optional[str] = Field(None, description="JSON string of quality requirements")
    cost_per_unit: Optional[float] = Field(None, gt=0)
    revision: Optional[str] = Field(None, max_length=10)


class PartCreate(PartBase):
    """Schema for creating a new part."""
    part_number: str = Field(..., min_length=1, max_length=50)


class PartUpdate(BaseSchema):
    """Schema for updating part information."""
    part_name: Optional[str] = Field(None, min_length=1, max_length=200)
    part_description: Optional[str] = None
    material_type: Optional[str] = Field(None, max_length=100)
    material_hardness: Optional[str] = Field(None, max_length=50)
    weight: Optional[float] = Field(None, gt=0)
    dimensions_length: Optional[float] = Field(None, gt=0)
    dimensions_width: Optional[float] = Field(None, gt=0)
    dimensions_height: Optional[float] = Field(None, gt=0)
    tolerance_class: Optional[str] = Field(None, max_length=20)
    surface_finish: Optional[str] = Field(None, max_length=50)
    standard_cycle_time: Optional[int] = Field(None, gt=0)
    setup_time_standard: Optional[int] = Field(None, gt=0)
    tooling_requirements: Optional[str] = None
    quality_requirements: Optional[str] = None
    cost_per_unit: Optional[float] = Field(None, gt=0)
    revision: Optional[str] = Field(None, max_length=10)


class PartResponse(PartBase):
    """Schema for part response data."""
    part_number: str
    created_at: datetime
    updated_at: datetime


# JobLogOB schemas
class JobLogBase(BaseSchema):
    """Base job log schema with common fields."""
    machine: str = Field(..., max_length=50)
    start_time: datetime
    end_time: Optional[datetime] = None
    job_number: str = Field(..., max_length=50)
    state: str = Field(..., max_length=20)
    part_number: str = Field(..., max_length=50)
    emp_id: str = Field(..., max_length=20)
    operator_name: str = Field(..., max_length=50)
    op_number: int
    parts_produced: Optional[int] = Field(None, ge=0)
    job_duration: Optional[int] = Field(None, ge=0)
    running_time: Optional[int] = Field(None, ge=0)
    
    # Downtime fields
    setup_time: Optional[int] = Field(None, ge=0)
    waiting_setup_time: Optional[int] = Field(None, ge=0)
    not_feeding_time: Optional[int] = Field(None, ge=0)
    adjustment_time: Optional[int] = Field(None, ge=0)
    dressing_time: Optional[int] = Field(None, ge=0)
    tooling_time: Optional[int] = Field(None, ge=0)
    engineering_time: Optional[int] = Field(None, ge=0)
    maintenance_time: Optional[int] = Field(None, ge=0)
    buy_in_time: Optional[int] = Field(None, ge=0)
    break_shift_change_time: Optional[int] = Field(None, ge=0)
    idle_time: Optional[int] = Field(None, ge=0)


class JobLogCreate(JobLogBase):
    """Schema for creating a new job log entry."""
    pass


class JobLogUpdate(BaseSchema):
    """Schema for updating job log information."""
    end_time: Optional[datetime] = None
    state: Optional[str] = Field(None, max_length=20)
    parts_produced: Optional[int] = Field(None, ge=0)
    job_duration: Optional[int] = Field(None, ge=0)
    running_time: Optional[int] = Field(None, ge=0)
    
    # Downtime fields
    setup_time: Optional[int] = Field(None, ge=0)
    waiting_setup_time: Optional[int] = Field(None, ge=0)
    not_feeding_time: Optional[int] = Field(None, ge=0)
    adjustment_time: Optional[int] = Field(None, ge=0)
    dressing_time: Optional[int] = Field(None, ge=0)
    tooling_time: Optional[int] = Field(None, ge=0)
    engineering_time: Optional[int] = Field(None, ge=0)
    maintenance_time: Optional[int] = Field(None, ge=0)
    buy_in_time: Optional[int] = Field(None, ge=0)
    break_shift_change_time: Optional[int] = Field(None, ge=0)
    idle_time: Optional[int] = Field(None, ge=0)


class JobLogResponse(JobLogBase):
    """Schema for job log response data."""
    id: int
    total_downtime: int = Field(..., description="Calculated total downtime")
    downtime_breakdown: Dict[str, int] = Field(..., description="Breakdown of all downtime categories")
    efficiency: float = Field(..., description="Calculated efficiency ratio")


# Analytics and reporting schemas
class OEEMetrics(BaseSchema):
    """Schema for Overall Equipment Effectiveness metrics."""
    availability: float = Field(..., ge=0, le=1, description="Availability percentage (0-1)")
    performance: float = Field(..., ge=0, le=1, description="Performance percentage (0-1)")
    quality: float = Field(..., ge=0, le=1, description="Quality percentage (0-1)")
    oee: float = Field(..., ge=0, le=1, description="Overall OEE (0-1)")


class MachineDataRequest(BaseSchema):
    """Schema for requesting machine data with filters."""
    machine_id: Optional[str] = Field(None, max_length=50)
    start_date: datetime
    end_date: datetime
    include_downtime: bool = True
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=100, ge=1, le=1000)

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        """Ensure end_date is after start_date."""
        if info.data and 'start_date' in info.data and v <= info.data['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class MachineDataResponse(BaseSchema):
    """Schema for machine data response."""
    data: List[JobLogResponse]
    total_count: int
    page: int
    page_size: int
    total_pages: int


class DowntimeAnalysisRequest(BaseSchema):
    """Schema for requesting downtime analysis."""
    machine_id: str = Field(..., max_length=50)
    start_date: datetime
    end_date: datetime
    downtime_types: Optional[List[str]] = None

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        """Ensure end_date is after start_date."""
        if info.data and 'start_date' in info.data and v <= info.data['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class DowntimeAnalysisResponse(BaseSchema):
    """Schema for downtime analysis response."""
    machine_id: str
    analysis_period: Dict[str, datetime]
    total_downtime: int
    downtime_by_category: Dict[str, int]
    downtime_trends: Dict[str, Any]
    recommendations: List[str]


class PerformanceMetricsRequest(BaseSchema):
    """Schema for requesting performance metrics."""
    entity_type: str = Field(..., pattern="^(machine|operator|job|part)$")
    entity_id: str = Field(..., max_length=50)
    start_date: datetime
    end_date: datetime
    metrics: Optional[List[str]] = None

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, v, info):
        """Ensure end_date is after start_date."""
        if info.data and 'start_date' in info.data and v <= info.data['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class PerformanceMetricsResponse(BaseSchema):
    """Schema for performance metrics response."""
    entity_type: str
    entity_id: str
    analysis_period: Dict[str, datetime]
    metrics: Dict[str, Any]
    oee_metrics: Optional[OEEMetrics] = None


# ML-related schemas
class MLTrainingRequest(BaseSchema):
    """Schema for ML model training requests."""
    model_type: str = Field(..., pattern="^(downtime_predictor|oee_optimizer)$")
    training_data_filter: MachineDataRequest
    hyperparameters: Optional[Dict[str, Any]] = None
    validation_split: float = Field(default=0.2, gt=0, lt=1)


class MLTrainingResponse(BaseSchema):
    """Schema for ML training response."""
    training_id: str
    model_type: str
    status: str
    started_at: datetime
    estimated_completion: Optional[datetime] = None


class PredictionRequest(BaseSchema):
    """Schema for prediction requests."""
    machine_id: str = Field(..., max_length=50)
    features: Dict[str, Any]
    prediction_horizon: int = Field(..., gt=0, description="Prediction horizon in hours")
    confidence_threshold: float = Field(default=0.8, ge=0, le=1)


class PredictionResponse(BaseSchema):
    """Schema for prediction response."""
    machine_id: str
    prediction_type: str
    prediction_value: Any
    confidence_score: float = Field(..., ge=0, le=1)
    prediction_horizon: int
    generated_at: datetime
    model_version: str


# Error response schemas
class ErrorDetail(BaseSchema):
    """Schema for error details."""
    field: Optional[str] = None
    message: str
    error_code: Optional[str] = None


class ErrorResponse(BaseSchema):
    """Schema for error responses."""
    error: str
    message: str
    details: Optional[List[ErrorDetail]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# Pagination schemas
class PaginationInfo(BaseSchema):
    """Schema for pagination information."""
    page: int = Field(..., ge=1)
    page_size: int = Field(..., ge=1, le=1000)
    total_count: int = Field(..., ge=0)
    total_pages: int = Field(..., ge=0)


class PaginatedResponse(BaseSchema):
    """Generic schema for paginated responses."""
    data: List[Any]
    pagination: PaginationInfo