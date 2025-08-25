"""
SQLAlchemy database models for CNC ML Monitoring Application.

This module contains all database models including the main JobLogOB table
and auxiliary tables for better data normalization and ML features.
"""

from datetime import datetime, date
from sqlalchemy import (
    Column, Integer, String, DateTime, Date, Float, Text, 
    ForeignKey, Boolean, create_engine
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class Machine(Base):
    """Machine model for CNC machine information and specifications."""
    
    __tablename__ = "machines"
    
    machine_id = Column(String(50), primary_key=True)
    machine_name = Column(String(100), nullable=False)
    machine_type = Column(String(50), nullable=False)
    manufacturer = Column(String(100), nullable=True)
    model = Column(String(100), nullable=True)
    year_installed = Column(Integer, nullable=True)
    max_spindle_speed = Column(Integer, nullable=True)
    max_feed_rate = Column(Float, nullable=True)
    work_envelope_x = Column(Float, nullable=True)
    work_envelope_y = Column(Float, nullable=True)
    work_envelope_z = Column(Float, nullable=True)
    maintenance_schedule_hours = Column(Integer, nullable=True)
    last_maintenance_date = Column(DateTime, nullable=True)
    status = Column(String(20), default="ACTIVE")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    job_logs = relationship("JobLogOB", back_populates="machine_ref")
    
    def __repr__(self):
        return f"<Machine(machine_id='{self.machine_id}', name='{self.machine_name}')>"


class Operator(Base):
    """Operator model for CNC machine operators and their information."""
    
    __tablename__ = "operators"
    
    emp_id = Column(String(20), primary_key=True)
    operator_name = Column(String(100), nullable=False)
    skill_level = Column(String(20), nullable=True)  # BEGINNER, INTERMEDIATE, ADVANCED, EXPERT
    hire_date = Column(Date, nullable=True)
    shift_preference = Column(String(20), nullable=True)  # DAY, NIGHT, ROTATING
    certifications = Column(Text, nullable=True)  # JSON string of certifications
    hourly_rate = Column(Float, nullable=True)
    department = Column(String(50), nullable=True)
    supervisor_id = Column(String(20), nullable=True)
    status = Column(String(20), default="ACTIVE")
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    job_logs = relationship("JobLogOB", back_populates="operator_ref")
    
    def __repr__(self):
        return f"<Operator(emp_id='{self.emp_id}', name='{self.operator_name}')>"


class Job(Base):
    """Job model for manufacturing jobs and their specifications."""
    
    __tablename__ = "jobs"
    
    job_number = Column(String(50), primary_key=True)
    job_name = Column(String(200), nullable=False)
    customer_id = Column(String(50), nullable=True)
    customer_name = Column(String(200), nullable=True)
    priority = Column(String(20), default="NORMAL")  # LOW, NORMAL, HIGH, URGENT
    estimated_hours = Column(Float, nullable=True)
    actual_hours = Column(Float, nullable=True)
    quantity_ordered = Column(Integer, nullable=False)
    quantity_completed = Column(Integer, default=0)
    due_date = Column(DateTime, nullable=True)
    start_date = Column(DateTime, nullable=True)
    completion_date = Column(DateTime, nullable=True)
    job_status = Column(String(20), default="PENDING")  # PENDING, IN_PROGRESS, COMPLETED, CANCELLED
    complexity_rating = Column(Integer, nullable=True)  # 1-10 scale
    setup_complexity = Column(Integer, nullable=True)  # 1-10 scale
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    job_logs = relationship("JobLogOB", back_populates="job_ref")
    
    def __repr__(self):
        return f"<Job(job_number='{self.job_number}', name='{self.job_name}')>"


class Part(Base):
    """Part model for manufactured parts and their specifications."""
    
    __tablename__ = "parts"
    
    part_number = Column(String(50), primary_key=True)
    part_name = Column(String(200), nullable=False)
    part_description = Column(Text, nullable=True)
    material_type = Column(String(100), nullable=True)
    material_hardness = Column(String(50), nullable=True)
    weight = Column(Float, nullable=True)
    dimensions_length = Column(Float, nullable=True)
    dimensions_width = Column(Float, nullable=True)
    dimensions_height = Column(Float, nullable=True)
    tolerance_class = Column(String(20), nullable=True)
    surface_finish = Column(String(50), nullable=True)
    standard_cycle_time = Column(Integer, nullable=True)  # in seconds
    setup_time_standard = Column(Integer, nullable=True)  # in seconds
    tooling_requirements = Column(Text, nullable=True)  # JSON string
    quality_requirements = Column(Text, nullable=True)  # JSON string
    cost_per_unit = Column(Float, nullable=True)
    revision = Column(String(10), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    job_logs = relationship("JobLogOB", back_populates="part_ref")
    
    def __repr__(self):
        return f"<Part(part_number='{self.part_number}', name='{self.part_name}')>"


class JobLogOB(Base):
    """
    Main job log table matching the existing database structure.
    
    This model represents the core operational data from CNC machines
    including all downtime metrics and production information.
    """
    
    __tablename__ = "joblog_ob"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    machine = Column(String(50), ForeignKey("machines.machine_id"), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)
    job_number = Column(String(50), ForeignKey("jobs.job_number"), nullable=False)
    state = Column(String(20), nullable=False)
    part_number = Column(String(50), ForeignKey("parts.part_number"), nullable=False)
    emp_id = Column(String(20), ForeignKey("operators.emp_id"), nullable=False)
    operator_name = Column(String(50), nullable=False)
    op_number = Column(Integer, nullable=False)
    parts_produced = Column(Integer, nullable=True)
    job_duration = Column(Integer, nullable=True)
    running_time = Column(Integer, nullable=True)
    
    # Downtime metrics - all the different types of downtime
    setup_time = Column(Integer, nullable=True)
    waiting_setup_time = Column(Integer, nullable=True)
    not_feeding_time = Column(Integer, nullable=True)
    adjustment_time = Column(Integer, nullable=True)
    dressing_time = Column(Integer, nullable=True)
    tooling_time = Column(Integer, nullable=True)
    engineering_time = Column(Integer, nullable=True)
    maintenance_time = Column(Integer, nullable=True)
    buy_in_time = Column(Integer, nullable=True)
    break_shift_change_time = Column(Integer, nullable=True)
    idle_time = Column(Integer, nullable=True)
    
    # Relationships
    machine_ref = relationship("Machine", back_populates="job_logs")
    job_ref = relationship("Job", back_populates="job_logs")
    part_ref = relationship("Part", back_populates="job_logs")
    operator_ref = relationship("Operator", back_populates="job_logs")
    
    def __repr__(self):
        return f"<JobLogOB(id={self.id}, machine='{self.machine}', job='{self.job_number}')>"
    
    @property
    def total_downtime(self) -> int:
        """Calculate total downtime from all downtime categories."""
        downtime_fields = [
            self.setup_time, self.waiting_setup_time, self.not_feeding_time,
            self.adjustment_time, self.dressing_time, self.tooling_time,
            self.engineering_time, self.maintenance_time, self.buy_in_time,
            self.break_shift_change_time, self.idle_time
        ]
        return sum(field or 0 for field in downtime_fields)
    
    @property
    def downtime_breakdown(self) -> dict:
        """Get a dictionary of all downtime categories and their values."""
        return {
            'setup_time': self.setup_time or 0,
            'waiting_setup_time': self.waiting_setup_time or 0,
            'not_feeding_time': self.not_feeding_time or 0,
            'adjustment_time': self.adjustment_time or 0,
            'dressing_time': self.dressing_time or 0,
            'tooling_time': self.tooling_time or 0,
            'engineering_time': self.engineering_time or 0,
            'maintenance_time': self.maintenance_time or 0,
            'buy_in_time': self.buy_in_time or 0,
            'break_shift_change_time': self.break_shift_change_time or 0,
            'idle_time': self.idle_time or 0
        }
    
    def calculate_efficiency(self) -> float:
        """Calculate efficiency as running_time / (running_time + total_downtime)."""
        if not self.running_time:
            return 0.0
        
        total_time = self.running_time + self.total_downtime
        if total_time == 0:
            return 0.0
            
        return self.running_time / total_time