"""
Unit tests for SQLAlchemy database models.

Tests model validation, relationships, and business logic methods.
"""

import pytest
from datetime import datetime, date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

from app.models.database_models import Base, Machine, Operator, Job, Part, JobLogOB


@pytest.fixture
def db_session():
    """Create an in-memory SQLite database for testing."""
    from sqlalchemy import event
    
    engine = create_engine("sqlite:///:memory:", echo=False)
    
    # Enable foreign key constraints in SQLite
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()
    
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_machine():
    """Create a sample machine for testing."""
    return Machine(
        machine_id="CNC001",
        machine_name="Haas VF-2",
        machine_type="Vertical Mill",
        manufacturer="Haas Automation",
        model="VF-2",
        year_installed=2020,
        max_spindle_speed=8100,
        max_feed_rate=1000.0,
        work_envelope_x=762.0,
        work_envelope_y=406.0,
        work_envelope_z=508.0,
        maintenance_schedule_hours=500,
        status="ACTIVE"
    )


@pytest.fixture
def sample_operator():
    """Create a sample operator for testing."""
    return Operator(
        emp_id="EMP001",
        operator_name="John Smith",
        skill_level="ADVANCED",
        hire_date=date(2020, 1, 15),
        shift_preference="DAY",
        certifications='["CNC Programming", "Quality Control"]',
        hourly_rate=25.50,
        department="Manufacturing",
        status="ACTIVE"
    )


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    return Job(
        job_number="JOB001",
        job_name="Aluminum Bracket Production",
        customer_id="CUST001",
        customer_name="ABC Manufacturing",
        priority="HIGH",
        estimated_hours=10.5,
        quantity_ordered=100,
        quantity_completed=0,
        due_date=datetime(2024, 12, 31),
        job_status="PENDING",
        complexity_rating=7,
        setup_complexity=5
    )


@pytest.fixture
def sample_part():
    """Create a sample part for testing."""
    return Part(
        part_number="PART001",
        part_name="Aluminum Bracket",
        part_description="L-shaped aluminum bracket for mounting",
        material_type="Aluminum 6061",
        material_hardness="T6",
        weight=0.5,
        dimensions_length=100.0,
        dimensions_width=50.0,
        dimensions_height=25.0,
        tolerance_class="±0.1mm",
        surface_finish="Mill finish",
        standard_cycle_time=300,
        setup_time_standard=1800,
        cost_per_unit=15.75,
        revision="A"
    )


class TestMachine:
    """Test cases for Machine model."""
    
    def test_machine_creation(self, db_session, sample_machine):
        """Test creating a machine record."""
        db_session.add(sample_machine)
        db_session.commit()
        
        retrieved = db_session.query(Machine).filter_by(machine_id="CNC001").first()
        assert retrieved is not None
        assert retrieved.machine_name == "Haas VF-2"
        assert retrieved.machine_type == "Vertical Mill"
        assert retrieved.status == "ACTIVE"
    
    def test_machine_repr(self, sample_machine):
        """Test machine string representation."""
        expected = "<Machine(machine_id='CNC001', name='Haas VF-2')>"
        assert repr(sample_machine) == expected
    
    def test_machine_required_fields(self, db_session):
        """Test that required fields are enforced."""
        # Missing machine_name should fail
        machine = Machine(machine_id="CNC002", machine_type="Lathe")
        db_session.add(machine)
        
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestOperator:
    """Test cases for Operator model."""
    
    def test_operator_creation(self, db_session, sample_operator):
        """Test creating an operator record."""
        db_session.add(sample_operator)
        db_session.commit()
        
        retrieved = db_session.query(Operator).filter_by(emp_id="EMP001").first()
        assert retrieved is not None
        assert retrieved.operator_name == "John Smith"
        assert retrieved.skill_level == "ADVANCED"
        assert retrieved.hourly_rate == 25.50
    
    def test_operator_repr(self, sample_operator):
        """Test operator string representation."""
        expected = "<Operator(emp_id='EMP001', name='John Smith')>"
        assert repr(sample_operator) == expected
    
    def test_operator_defaults(self, db_session):
        """Test operator default values."""
        operator = Operator(emp_id="EMP002", operator_name="Jane Doe")
        db_session.add(operator)
        db_session.commit()
        
        retrieved = db_session.query(Operator).filter_by(emp_id="EMP002").first()
        assert retrieved.status == "ACTIVE"


class TestJob:
    """Test cases for Job model."""
    
    def test_job_creation(self, db_session, sample_job):
        """Test creating a job record."""
        db_session.add(sample_job)
        db_session.commit()
        
        retrieved = db_session.query(Job).filter_by(job_number="JOB001").first()
        assert retrieved is not None
        assert retrieved.job_name == "Aluminum Bracket Production"
        assert retrieved.priority == "HIGH"
        assert retrieved.quantity_ordered == 100
    
    def test_job_repr(self, sample_job):
        """Test job string representation."""
        expected = "<Job(job_number='JOB001', name='Aluminum Bracket Production')>"
        assert repr(sample_job) == expected
    
    def test_job_defaults(self, db_session):
        """Test job default values."""
        job = Job(job_number="JOB002", job_name="Test Job", quantity_ordered=50)
        db_session.add(job)
        db_session.commit()
        
        retrieved = db_session.query(Job).filter_by(job_number="JOB002").first()
        assert retrieved.priority == "NORMAL"
        assert retrieved.job_status == "PENDING"
        assert retrieved.quantity_completed == 0


class TestPart:
    """Test cases for Part model."""
    
    def test_part_creation(self, db_session, sample_part):
        """Test creating a part record."""
        db_session.add(sample_part)
        db_session.commit()
        
        retrieved = db_session.query(Part).filter_by(part_number="PART001").first()
        assert retrieved is not None
        assert retrieved.part_name == "Aluminum Bracket"
        assert retrieved.material_type == "Aluminum 6061"
        assert retrieved.cost_per_unit == 15.75
    
    def test_part_repr(self, sample_part):
        """Test part string representation."""
        expected = "<Part(part_number='PART001', name='Aluminum Bracket')>"
        assert repr(sample_part) == expected


class TestJobLogOB:
    """Test cases for JobLogOB model."""
    
    def test_joblog_creation_with_relationships(self, db_session, sample_machine, 
                                               sample_operator, sample_job, sample_part):
        """Test creating a job log with all relationships."""
        # Add related entities first
        db_session.add_all([sample_machine, sample_operator, sample_job, sample_part])
        db_session.commit()
        
        # Create job log
        job_log = JobLogOB(
            machine="CNC001",
            start_time=datetime(2024, 1, 15, 8, 0, 0),
            end_time=datetime(2024, 1, 15, 16, 0, 0),
            job_number="JOB001",
            state="RUNNING",
            part_number="PART001",
            emp_id="EMP001",
            operator_name="John Smith",
            op_number=10,
            parts_produced=25,
            job_duration=480,
            running_time=400,
            setup_time=60,
            maintenance_time=20
        )
        
        db_session.add(job_log)
        db_session.commit()
        
        retrieved = db_session.query(JobLogOB).first()
        assert retrieved is not None
        assert retrieved.machine == "CNC001"
        assert retrieved.job_number == "JOB001"
        assert retrieved.parts_produced == 25
        
        # Test relationships
        assert retrieved.machine_ref.machine_name == "Haas VF-2"
        assert retrieved.operator_ref.operator_name == "John Smith"
        assert retrieved.job_ref.job_name == "Aluminum Bracket Production"
        assert retrieved.part_ref.part_name == "Aluminum Bracket"
    
    def test_joblog_repr(self, db_session, sample_machine, sample_operator, 
                        sample_job, sample_part):
        """Test job log string representation."""
        db_session.add_all([sample_machine, sample_operator, sample_job, sample_part])
        db_session.commit()
        
        job_log = JobLogOB(
            machine="CNC001",
            start_time=datetime(2024, 1, 15, 8, 0, 0),
            job_number="JOB001",
            state="RUNNING",
            part_number="PART001",
            emp_id="EMP001",
            operator_name="John Smith",
            op_number=10
        )
        
        db_session.add(job_log)
        db_session.commit()
        
        expected = "<JobLogOB(id=1, machine='CNC001', job='JOB001')>"
        assert repr(job_log) == expected
    
    def test_total_downtime_calculation(self):
        """Test total downtime calculation property."""
        job_log = JobLogOB(
            machine="CNC001",
            start_time=datetime.now(),
            job_number="JOB001",
            state="RUNNING",
            part_number="PART001",
            emp_id="EMP001",
            operator_name="John Smith",
            op_number=10,
            setup_time=60,
            maintenance_time=30,
            adjustment_time=15,
            idle_time=10
        )
        
        assert job_log.total_downtime == 115
    
    def test_total_downtime_with_none_values(self):
        """Test total downtime calculation with None values."""
        job_log = JobLogOB(
            machine="CNC001",
            start_time=datetime.now(),
            job_number="JOB001",
            state="RUNNING",
            part_number="PART001",
            emp_id="EMP001",
            operator_name="John Smith",
            op_number=10,
            setup_time=60,
            maintenance_time=None,
            adjustment_time=15
        )
        
        assert job_log.total_downtime == 75
    
    def test_downtime_breakdown(self):
        """Test downtime breakdown property."""
        job_log = JobLogOB(
            machine="CNC001",
            start_time=datetime.now(),
            job_number="JOB001",
            state="RUNNING",
            part_number="PART001",
            emp_id="EMP001",
            operator_name="John Smith",
            op_number=10,
            setup_time=60,
            maintenance_time=30,
            adjustment_time=15
        )
        
        breakdown = job_log.downtime_breakdown
        assert breakdown['setup_time'] == 60
        assert breakdown['maintenance_time'] == 30
        assert breakdown['adjustment_time'] == 15
        assert breakdown['idle_time'] == 0  # None should become 0
    
    def test_efficiency_calculation(self):
        """Test efficiency calculation method."""
        job_log = JobLogOB(
            machine="CNC001",
            start_time=datetime.now(),
            job_number="JOB001",
            state="RUNNING",
            part_number="PART001",
            emp_id="EMP001",
            operator_name="John Smith",
            op_number=10,
            running_time=400,
            setup_time=60,
            maintenance_time=40
        )
        
        # Efficiency = running_time / (running_time + total_downtime)
        # = 400 / (400 + 100) = 0.8
        assert job_log.calculate_efficiency() == 0.8
    
    def test_efficiency_with_no_running_time(self):
        """Test efficiency calculation with no running time."""
        job_log = JobLogOB(
            machine="CNC001",
            start_time=datetime.now(),
            job_number="JOB001",
            state="RUNNING",
            part_number="PART001",
            emp_id="EMP001",
            operator_name="John Smith",
            op_number=10,
            running_time=None,
            setup_time=60
        )
        
        assert job_log.calculate_efficiency() == 0.0
    
    def test_foreign_key_constraints(self, db_session):
        """Test that foreign key constraints are enforced."""
        # Try to create job log without related entities
        job_log = JobLogOB(
            machine="NONEXISTENT",
            start_time=datetime.now(),
            job_number="NONEXISTENT",
            state="RUNNING",
            part_number="NONEXISTENT",
            emp_id="NONEXISTENT",
            operator_name="Test",
            op_number=10
        )
        
        db_session.add(job_log)
        
        # This should fail due to foreign key constraints
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestModelRelationships:
    """Test cases for model relationships."""
    
    def test_machine_job_logs_relationship(self, db_session, sample_machine, 
                                          sample_operator, sample_job, sample_part):
        """Test machine to job logs relationship."""
        db_session.add_all([sample_machine, sample_operator, sample_job, sample_part])
        db_session.commit()
        
        # Create multiple job logs for the same machine
        job_log1 = JobLogOB(
            machine="CNC001", start_time=datetime.now(), job_number="JOB001",
            state="RUNNING", part_number="PART001", emp_id="EMP001",
            operator_name="John Smith", op_number=10
        )
        job_log2 = JobLogOB(
            machine="CNC001", start_time=datetime.now(), job_number="JOB001",
            state="SETUP", part_number="PART001", emp_id="EMP001",
            operator_name="John Smith", op_number=20
        )
        
        db_session.add_all([job_log1, job_log2])
        db_session.commit()
        
        machine = db_session.query(Machine).filter_by(machine_id="CNC001").first()
        assert len(machine.job_logs) == 2
    
    def test_operator_job_logs_relationship(self, db_session, sample_machine, 
                                           sample_operator, sample_job, sample_part):
        """Test operator to job logs relationship."""
        db_session.add_all([sample_machine, sample_operator, sample_job, sample_part])
        db_session.commit()
        
        job_log = JobLogOB(
            machine="CNC001", start_time=datetime.now(), job_number="JOB001",
            state="RUNNING", part_number="PART001", emp_id="EMP001",
            operator_name="John Smith", op_number=10
        )
        
        db_session.add(job_log)
        db_session.commit()
        
        operator = db_session.query(Operator).filter_by(emp_id="EMP001").first()
        assert len(operator.job_logs) == 1
        assert operator.job_logs[0].machine == "CNC001"