"""
Unit tests for Pydantic models and schemas.

Tests validation rules, serialization, and business logic validation.
"""

import pytest
from datetime import datetime, date
from pydantic import ValidationError

from app.models.pydantic_models import (
    MachineCreate, MachineUpdate, MachineResponse,
    OperatorCreate, OperatorUpdate, OperatorResponse,
    JobCreate, JobUpdate, JobResponse,
    PartCreate, PartUpdate, PartResponse,
    JobLogCreate, JobLogUpdate, JobLogResponse,
    MachineDataRequest, DowntimeAnalysisRequest,
    PerformanceMetricsRequest, MLTrainingRequest,
    PredictionRequest, OEEMetrics,
    SkillLevel, Priority, JobStatus, MachineStatus
)


class TestMachineSchemas:
    """Test cases for Machine Pydantic schemas."""
    
    def test_machine_create_valid(self):
        """Test creating a valid machine."""
        machine_data = {
            "machine_id": "CNC001",
            "machine_name": "Haas VF-2",
            "machine_type": "Vertical Mill",
            "manufacturer": "Haas Automation",
            "model": "VF-2",
            "year_installed": 2020,
            "max_spindle_speed": 8100,
            "max_feed_rate": 1000.0,
            "status": "ACTIVE"
        }
        
        machine = MachineCreate(**machine_data)
        assert machine.machine_id == "CNC001"
        assert machine.machine_name == "Haas VF-2"
        assert machine.status == MachineStatus.ACTIVE
    
    def test_machine_create_required_fields(self):
        """Test that required fields are enforced."""
        with pytest.raises(ValidationError) as exc_info:
            MachineCreate(machine_id="CNC001")
        
        errors = exc_info.value.errors()
        required_fields = {error['loc'][0] for error in errors}
        assert 'machine_name' in required_fields
        assert 'machine_type' in required_fields
    
    def test_machine_create_field_validation(self):
        """Test field validation rules."""
        # Test empty machine_name
        with pytest.raises(ValidationError):
            MachineCreate(
                machine_id="CNC001",
                machine_name="",
                machine_type="Mill"
            )
        
        # Test invalid year
        with pytest.raises(ValidationError):
            MachineCreate(
                machine_id="CNC001",
                machine_name="Test Machine",
                machine_type="Mill",
                year_installed=1800  # Too old
            )
        
        # Test negative spindle speed
        with pytest.raises(ValidationError):
            MachineCreate(
                machine_id="CNC001",
                machine_name="Test Machine",
                machine_type="Mill",
                max_spindle_speed=-100
            )
    
    def test_machine_update_optional_fields(self):
        """Test that all fields are optional in update schema."""
        update_data = {"machine_name": "Updated Name"}
        machine_update = MachineUpdate(**update_data)
        assert machine_update.machine_name == "Updated Name"
        assert machine_update.machine_type is None


class TestOperatorSchemas:
    """Test cases for Operator Pydantic schemas."""
    
    def test_operator_create_valid(self):
        """Test creating a valid operator."""
        operator_data = {
            "emp_id": "EMP001",
            "operator_name": "John Smith",
            "skill_level": "ADVANCED",
            "hire_date": "2020-01-15",
            "shift_preference": "DAY",
            "hourly_rate": 25.50,
            "department": "Manufacturing"
        }
        
        operator = OperatorCreate(**operator_data)
        assert operator.emp_id == "EMP001"
        assert operator.skill_level == SkillLevel.ADVANCED
        assert operator.hourly_rate == 25.50
    
    def test_operator_skill_level_enum(self):
        """Test skill level enum validation."""
        # Valid skill level
        operator = OperatorCreate(
            emp_id="EMP001",
            operator_name="John Smith",
            skill_level="EXPERT"
        )
        assert operator.skill_level == SkillLevel.EXPERT
        
        # Invalid skill level
        with pytest.raises(ValidationError):
            OperatorCreate(
                emp_id="EMP001",
                operator_name="John Smith",
                skill_level="INVALID_LEVEL"
            )
    
    def test_operator_hourly_rate_validation(self):
        """Test hourly rate validation."""
        # Negative rate should fail
        with pytest.raises(ValidationError):
            OperatorCreate(
                emp_id="EMP001",
                operator_name="John Smith",
                hourly_rate=-10.0
            )


class TestJobSchemas:
    """Test cases for Job Pydantic schemas."""
    
    def test_job_create_valid(self):
        """Test creating a valid job."""
        job_data = {
            "job_number": "JOB001",
            "job_name": "Aluminum Bracket Production",
            "customer_name": "ABC Manufacturing",
            "priority": "HIGH",
            "quantity_ordered": 100,
            "due_date": "2024-12-31T23:59:59"
        }
        
        job = JobCreate(**job_data)
        assert job.job_number == "JOB001"
        assert job.priority == Priority.HIGH
        assert job.quantity_ordered == 100
    
    def test_job_quantity_validation(self):
        """Test quantity validation rules."""
        # quantity_completed > quantity_ordered should fail
        with pytest.raises(ValidationError):
            JobCreate(
                job_number="JOB001",
                job_name="Test Job",
                quantity_ordered=50,
                quantity_completed=100
            )
        
        # Negative quantities should fail
        with pytest.raises(ValidationError):
            JobCreate(
                job_number="JOB001",
                job_name="Test Job",
                quantity_ordered=-10
            )
    
    def test_job_priority_enum(self):
        """Test priority enum validation."""
        job = JobCreate(
            job_number="JOB001",
            job_name="Test Job",
            quantity_ordered=50,
            priority="URGENT"
        )
        assert job.priority == Priority.URGENT
        
        # Invalid priority
        with pytest.raises(ValidationError):
            JobCreate(
                job_number="JOB001",
                job_name="Test Job",
                quantity_ordered=50,
                priority="INVALID_PRIORITY"
            )
    
    def test_job_complexity_rating_validation(self):
        """Test complexity rating validation."""
        # Valid rating (1-10)
        job = JobCreate(
            job_number="JOB001",
            job_name="Test Job",
            quantity_ordered=50,
            complexity_rating=7
        )
        assert job.complexity_rating == 7
        
        # Invalid rating (out of range)
        with pytest.raises(ValidationError):
            JobCreate(
                job_number="JOB001",
                job_name="Test Job",
                quantity_ordered=50,
                complexity_rating=15
            )


class TestPartSchemas:
    """Test cases for Part Pydantic schemas."""
    
    def test_part_create_valid(self):
        """Test creating a valid part."""
        part_data = {
            "part_number": "PART001",
            "part_name": "Aluminum Bracket",
            "material_type": "Aluminum 6061",
            "weight": 0.5,
            "dimensions_length": 100.0,
            "cost_per_unit": 15.75
        }
        
        part = PartCreate(**part_data)
        assert part.part_number == "PART001"
        assert part.weight == 0.5
        assert part.cost_per_unit == 15.75
    
    def test_part_dimension_validation(self):
        """Test dimension validation rules."""
        # Negative dimensions should fail
        with pytest.raises(ValidationError):
            PartCreate(
                part_number="PART001",
                part_name="Test Part",
                dimensions_length=-10.0
            )
        
        # Zero dimensions should fail
        with pytest.raises(ValidationError):
            PartCreate(
                part_number="PART001",
                part_name="Test Part",
                weight=0
            )
    
    def test_part_time_validation(self):
        """Test time field validation."""
        # Negative cycle time should fail
        with pytest.raises(ValidationError):
            PartCreate(
                part_number="PART001",
                part_name="Test Part",
                standard_cycle_time=-100
            )


class TestJobLogSchemas:
    """Test cases for JobLog Pydantic schemas."""
    
    def test_joblog_create_valid(self):
        """Test creating a valid job log."""
        joblog_data = {
            "machine": "CNC001",
            "start_time": "2024-01-15T08:00:00",
            "end_time": "2024-01-15T16:00:00",
            "job_number": "JOB001",
            "state": "RUNNING",
            "part_number": "PART001",
            "emp_id": "EMP001",
            "operator_name": "John Smith",
            "op_number": 10,
            "parts_produced": 25,
            "running_time": 400,
            "setup_time": 60
        }
        
        joblog = JobLogCreate(**joblog_data)
        assert joblog.machine == "CNC001"
        assert joblog.parts_produced == 25
        assert joblog.setup_time == 60
    
    def test_joblog_negative_values_validation(self):
        """Test that negative values are rejected."""
        base_data = {
            "machine": "CNC001",
            "start_time": "2024-01-15T08:00:00",
            "job_number": "JOB001",
            "state": "RUNNING",
            "part_number": "PART001",
            "emp_id": "EMP001",
            "operator_name": "John Smith",
            "op_number": 10
        }
        
        # Negative parts_produced should fail
        with pytest.raises(ValidationError):
            JobLogCreate(**{**base_data, "parts_produced": -5})
        
        # Negative downtime should fail
        with pytest.raises(ValidationError):
            JobLogCreate(**{**base_data, "setup_time": -10})


class TestAnalyticsSchemas:
    """Test cases for analytics and reporting schemas."""
    
    def test_machine_data_request_valid(self):
        """Test valid machine data request."""
        request_data = {
            "start_date": "2024-01-01T00:00:00",
            "end_date": "2024-01-31T23:59:59",
            "machine_id": "CNC001",
            "page": 1,
            "page_size": 50
        }
        
        request = MachineDataRequest(**request_data)
        assert request.machine_id == "CNC001"
        assert request.page == 1
        assert request.include_downtime is True  # Default value
    
    def test_machine_data_request_date_validation(self):
        """Test date range validation."""
        # end_date before start_date should fail
        with pytest.raises(ValidationError):
            MachineDataRequest(
                start_date="2024-01-31T00:00:00",
                end_date="2024-01-01T00:00:00"
            )
    
    def test_machine_data_request_pagination_validation(self):
        """Test pagination validation."""
        # Invalid page number
        with pytest.raises(ValidationError):
            MachineDataRequest(
                start_date="2024-01-01T00:00:00",
                end_date="2024-01-31T23:59:59",
                page=0
            )
        
        # Page size too large
        with pytest.raises(ValidationError):
            MachineDataRequest(
                start_date="2024-01-01T00:00:00",
                end_date="2024-01-31T23:59:59",
                page_size=2000
            )
    
    def test_oee_metrics_validation(self):
        """Test OEE metrics validation."""
        # Valid OEE metrics
        oee = OEEMetrics(
            availability=0.85,
            performance=0.92,
            quality=0.98,
            oee=0.77
        )
        assert oee.availability == 0.85
        assert oee.oee == 0.77
        
        # Invalid values (out of range)
        with pytest.raises(ValidationError):
            OEEMetrics(
                availability=1.5,  # > 1
                performance=0.92,
                quality=0.98,
                oee=0.77
            )
        
        with pytest.raises(ValidationError):
            OEEMetrics(
                availability=0.85,
                performance=-0.1,  # < 0
                quality=0.98,
                oee=0.77
            )
    
    def test_downtime_analysis_request(self):
        """Test downtime analysis request validation."""
        request = DowntimeAnalysisRequest(
            machine_id="CNC001",
            start_date="2024-01-01T00:00:00",
            end_date="2024-01-31T23:59:59",
            downtime_types=["setup_time", "maintenance_time"]
        )
        
        assert request.machine_id == "CNC001"
        assert len(request.downtime_types) == 2
    
    def test_performance_metrics_request(self):
        """Test performance metrics request validation."""
        # Valid entity type
        request = PerformanceMetricsRequest(
            entity_type="machine",
            entity_id="CNC001",
            start_date="2024-01-01T00:00:00",
            end_date="2024-01-31T23:59:59"
        )
        assert request.entity_type == "machine"
        
        # Invalid entity type
        with pytest.raises(ValidationError):
            PerformanceMetricsRequest(
                entity_type="invalid_type",
                entity_id="CNC001",
                start_date="2024-01-01T00:00:00",
                end_date="2024-01-31T23:59:59"
            )


class TestMLSchemas:
    """Test cases for ML-related schemas."""
    
    def test_ml_training_request_valid(self):
        """Test valid ML training request."""
        training_data_filter = {
            "start_date": "2024-01-01T00:00:00",
            "end_date": "2024-01-31T23:59:59"
        }
        
        request = MLTrainingRequest(
            model_type="downtime_predictor",
            training_data_filter=training_data_filter,
            validation_split=0.3
        )
        
        assert request.model_type == "downtime_predictor"
        assert request.validation_split == 0.3
    
    def test_ml_training_request_model_type_validation(self):
        """Test model type validation."""
        training_data_filter = {
            "start_date": "2024-01-01T00:00:00",
            "end_date": "2024-01-31T23:59:59"
        }
        
        # Invalid model type
        with pytest.raises(ValidationError):
            MLTrainingRequest(
                model_type="invalid_model",
                training_data_filter=training_data_filter
            )
    
    def test_ml_training_request_validation_split(self):
        """Test validation split validation."""
        training_data_filter = {
            "start_date": "2024-01-01T00:00:00",
            "end_date": "2024-01-31T23:59:59"
        }
        
        # Invalid validation split (>= 1)
        with pytest.raises(ValidationError):
            MLTrainingRequest(
                model_type="downtime_predictor",
                training_data_filter=training_data_filter,
                validation_split=1.0
            )
        
        # Invalid validation split (<= 0)
        with pytest.raises(ValidationError):
            MLTrainingRequest(
                model_type="downtime_predictor",
                training_data_filter=training_data_filter,
                validation_split=0.0
            )
    
    def test_prediction_request_valid(self):
        """Test valid prediction request."""
        request = PredictionRequest(
            machine_id="CNC001",
            features={"avg_setup_time": 60, "last_maintenance_days": 30},
            prediction_horizon=24,
            confidence_threshold=0.85
        )
        
        assert request.machine_id == "CNC001"
        assert request.prediction_horizon == 24
        assert request.confidence_threshold == 0.85
    
    def test_prediction_request_validation(self):
        """Test prediction request validation."""
        # Invalid prediction horizon
        with pytest.raises(ValidationError):
            PredictionRequest(
                machine_id="CNC001",
                features={},
                prediction_horizon=0
            )
        
        # Invalid confidence threshold
        with pytest.raises(ValidationError):
            PredictionRequest(
                machine_id="CNC001",
                features={},
                prediction_horizon=24,
                confidence_threshold=1.5
            )


class TestSchemaSerializationDeserialization:
    """Test schema serialization and deserialization."""
    
    def test_machine_response_serialization(self):
        """Test machine response serialization."""
        machine_data = {
            "machine_id": "CNC001",
            "machine_name": "Haas VF-2",
            "machine_type": "Vertical Mill",
            "manufacturer": "Haas Automation",
            "status": "ACTIVE",
            "created_at": datetime(2024, 1, 1, 12, 0, 0),
            "updated_at": datetime(2024, 1, 1, 12, 0, 0)
        }
        
        response = MachineResponse(**machine_data)
        json_data = response.model_dump()
        
        assert json_data["machine_id"] == "CNC001"
        assert json_data["status"] == "ACTIVE"
        assert "created_at" in json_data
    
    def test_joblog_response_with_calculated_fields(self):
        """Test job log response with calculated fields."""
        joblog_data = {
            "id": 1,
            "machine": "CNC001",
            "start_time": datetime(2024, 1, 15, 8, 0, 0),
            "job_number": "JOB001",
            "state": "RUNNING",
            "part_number": "PART001",
            "emp_id": "EMP001",
            "operator_name": "John Smith",
            "op_number": 10,
            "running_time": 400,
            "setup_time": 60,
            "maintenance_time": 40,
            "total_downtime": 100,
            "downtime_breakdown": {
                "setup_time": 60,
                "maintenance_time": 40,
                "idle_time": 0
            },
            "efficiency": 0.8
        }
        
        response = JobLogResponse(**joblog_data)
        assert response.total_downtime == 100
        assert response.efficiency == 0.8
        assert response.downtime_breakdown["setup_time"] == 60


class TestValidationEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_empty_string_validation(self):
        """Test empty string validation."""
        with pytest.raises(ValidationError):
            MachineCreate(
                machine_id="",  # Empty string should fail
                machine_name="Test",
                machine_type="Mill"
            )
    
    def test_whitespace_string_validation(self):
        """Test whitespace-only string validation."""
        # Pydantic v2 allows whitespace strings by default
        # This test verifies the current behavior
        machine = MachineCreate(
            machine_id="CNC001",
            machine_name="   ",  # Whitespace-only is allowed
            machine_type="Mill"
        )
        assert machine.machine_name == "   "
    
    def test_boundary_values(self):
        """Test boundary values for numeric fields."""
        # Test minimum valid year
        machine = MachineCreate(
            machine_id="CNC001",
            machine_name="Test Machine",
            machine_type="Mill",
            year_installed=1900  # Minimum valid year
        )
        assert machine.year_installed == 1900
        
        # Test maximum valid year
        machine = MachineCreate(
            machine_id="CNC001",
            machine_name="Test Machine",
            machine_type="Mill",
            year_installed=2030  # Maximum valid year
        )
        assert machine.year_installed == 2030
    
    def test_none_vs_missing_fields(self):
        """Test difference between None and missing optional fields."""
        # Explicit None should be allowed for optional fields
        machine = MachineUpdate(manufacturer=None)
        assert machine.manufacturer is None
        
        # Missing field should also result in None
        machine = MachineUpdate()
        assert machine.manufacturer is None