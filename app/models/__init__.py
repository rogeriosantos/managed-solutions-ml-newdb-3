# Data models module

from .database_models import Base, Machine, Operator, Job, Part, JobLogOB
from .pydantic_models import (
    # Machine schemas
    MachineCreate, MachineUpdate, MachineResponse,
    # Operator schemas
    OperatorCreate, OperatorUpdate, OperatorResponse,
    # Job schemas
    JobCreate, JobUpdate, JobResponse,
    # Part schemas
    PartCreate, PartUpdate, PartResponse,
    # JobLog schemas
    JobLogCreate, JobLogUpdate, JobLogResponse,
    # Analytics schemas
    MachineDataRequest, MachineDataResponse,
    DowntimeAnalysisRequest, DowntimeAnalysisResponse,
    PerformanceMetricsRequest, PerformanceMetricsResponse,
    OEEMetrics,
    # ML schemas
    MLTrainingRequest, MLTrainingResponse,
    PredictionRequest, PredictionResponse,
    # Error schemas
    ErrorResponse, ErrorDetail,
    # Enums
    SkillLevel, Priority, JobStatus, MachineStatus, OperatorStatus
)

__all__ = [
    # Database models
    "Base", "Machine", "Operator", "Job", "Part", "JobLogOB",
    # Pydantic schemas
    "MachineCreate", "MachineUpdate", "MachineResponse",
    "OperatorCreate", "OperatorUpdate", "OperatorResponse",
    "JobCreate", "JobUpdate", "JobResponse",
    "PartCreate", "PartUpdate", "PartResponse",
    "JobLogCreate", "JobLogUpdate", "JobLogResponse",
    "MachineDataRequest", "MachineDataResponse",
    "DowntimeAnalysisRequest", "DowntimeAnalysisResponse",
    "PerformanceMetricsRequest", "PerformanceMetricsResponse",
    "OEEMetrics", "MLTrainingRequest", "MLTrainingResponse",
    "PredictionRequest", "PredictionResponse",
    "ErrorResponse", "ErrorDetail",
    "SkillLevel", "Priority", "JobStatus", "MachineStatus", "OperatorStatus"
]