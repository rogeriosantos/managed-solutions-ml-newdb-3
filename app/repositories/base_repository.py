"""
Base Repository Module

This module provides an abstract base repository class with common CRUD operations
and utilities for pagination, filtering, and database operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import select, update, delete, func, and_, or_
from sqlalchemy.exc import SQLAlchemyError
import logging

logger = logging.getLogger(__name__)

# Generic type for SQLAlchemy models
ModelType = TypeVar("ModelType", bound=DeclarativeBase)


class FilterOperator:
    """Enumeration of supported filter operators."""
    EQ = "eq"           # Equal
    NE = "ne"           # Not equal
    GT = "gt"           # Greater than
    GTE = "gte"         # Greater than or equal
    LT = "lt"           # Less than
    LTE = "lte"         # Less than or equal
    LIKE = "like"       # SQL LIKE pattern matching
    ILIKE = "ilike"     # Case-insensitive LIKE
    IN = "in"           # IN clause
    NOT_IN = "not_in"   # NOT IN clause
    IS_NULL = "is_null" # IS NULL
    IS_NOT_NULL = "is_not_null"  # IS NOT NULL


class FilterCondition:
    """Represents a single filter condition."""
    
    def __init__(self, field: str, operator: str, value: Any = None):
        self.field = field
        self.operator = operator
        self.value = value
    
    def __repr__(self):
        return f"FilterCondition(field='{self.field}', operator='{self.operator}', value={self.value})"


class PaginationParams:
    """Parameters for pagination."""
    
    def __init__(self, skip: int = 0, limit: int = 100, max_limit: int = 1000):
        self.skip = max(0, skip)
        self.limit = min(max(1, limit), max_limit)
        self.max_limit = max_limit
    
    @property
    def offset(self) -> int:
        """Alias for skip to match SQLAlchemy terminology."""
        return self.skip
    
    def __repr__(self):
        return f"PaginationParams(skip={self.skip}, limit={self.limit})"


class PaginatedResult:
    """Container for paginated query results."""
    
    def __init__(self, items: List[Any], total_count: int, pagination: PaginationParams):
        self.items = items
        self.total_count = total_count
        self.pagination = pagination
    
    @property
    def has_next(self) -> bool:
        """Check if there are more items after the current page."""
        return (self.pagination.skip + self.pagination.limit) < self.total_count
    
    @property
    def has_previous(self) -> bool:
        """Check if there are items before the current page."""
        return self.pagination.skip > 0
    
    @property
    def page_number(self) -> int:
        """Calculate current page number (1-based)."""
        return (self.pagination.skip // self.pagination.limit) + 1
    
    @property
    def total_pages(self) -> int:
        """Calculate total number of pages."""
        return (self.total_count + self.pagination.limit - 1) // self.pagination.limit
    
    def __repr__(self):
        return (f"PaginatedResult(items={len(self.items)}, total_count={self.total_count}, "
                f"page={self.page_number}/{self.total_pages})")


class BaseRepository(Generic[ModelType], ABC):
    """
    Abstract base repository class providing common CRUD operations.
    
    This class implements the Repository pattern and provides a consistent
    interface for database operations across all entity types.
    """
    
    def __init__(self, session: AsyncSession, model_class: Type[ModelType]):
        """
        Initialize the repository with a database session and model class.
        
        Args:
            session: SQLAlchemy async session
            model_class: SQLAlchemy model class for this repository
        """
        self.session = session
        self.model_class = model_class
    
    # Abstract methods that must be implemented by subclasses
    
    @abstractmethod
    def get_primary_key_field(self) -> str:
        """
        Get the name of the primary key field for this model.
        
        Returns:
            str: Name of the primary key field
        """
        pass
    
    # Common CRUD operations
    
    async def create(self, **kwargs) -> ModelType:
        """
        Create a new record in the database.
        
        Args:
            **kwargs: Field values for the new record
            
        Returns:
            ModelType: The created record
            
        Raises:
            SQLAlchemyError: If database operation fails
        """
        try:
            instance = self.model_class(**kwargs)
            self.session.add(instance)
            await self.session.flush()  # Flush to get the ID without committing
            await self.session.refresh(instance)  # Refresh to get all computed fields
            logger.debug(f"Created {self.model_class.__name__} with data: {kwargs}")
            return instance
        except SQLAlchemyError as e:
            logger.error(f"Failed to create {self.model_class.__name__}: {e}")
            raise
    
    async def get_by_id(self, record_id: Any) -> Optional[ModelType]:
        """
        Retrieve a record by its primary key.
        
        Args:
            record_id: Primary key value
            
        Returns:
            Optional[ModelType]: The record if found, None otherwise
        """
        try:
            pk_field = getattr(self.model_class, self.get_primary_key_field())
            stmt = select(self.model_class).where(pk_field == record_id)
            result = await self.session.execute(stmt)
            record = result.scalar_one_or_none()
            logger.debug(f"Retrieved {self.model_class.__name__} with ID: {record_id}")
            return record
        except SQLAlchemyError as e:
            logger.error(f"Failed to get {self.model_class.__name__} by ID {record_id}: {e}")
            raise
    
    async def get_all(self, 
                     filters: Optional[List[FilterCondition]] = None,
                     order_by: Optional[str] = None,
                     order_desc: bool = False) -> List[ModelType]:
        """
        Retrieve all records matching the given filters.
        
        Args:
            filters: List of filter conditions
            order_by: Field name to order by
            order_desc: Whether to order in descending order
            
        Returns:
            List[ModelType]: List of matching records
        """
        try:
            stmt = select(self.model_class)
            
            # Apply filters
            if filters:
                stmt = self._apply_filters(stmt, filters)
            
            # Apply ordering
            if order_by:
                order_field = getattr(self.model_class, order_by, None)
                if order_field is not None:
                    if order_desc:
                        stmt = stmt.order_by(order_field.desc())
                    else:
                        stmt = stmt.order_by(order_field)
            
            result = await self.session.execute(stmt)
            records = result.scalars().all()
            logger.debug(f"Retrieved {len(records)} {self.model_class.__name__} records")
            return list(records)
        except SQLAlchemyError as e:
            logger.error(f"Failed to get all {self.model_class.__name__} records: {e}")
            raise
    
    async def get_paginated(self,
                           pagination: PaginationParams,
                           filters: Optional[List[FilterCondition]] = None,
                           order_by: Optional[str] = None,
                           order_desc: bool = False) -> PaginatedResult:
        """
        Retrieve paginated records matching the given filters.
        
        Args:
            pagination: Pagination parameters
            filters: List of filter conditions
            order_by: Field name to order by
            order_desc: Whether to order in descending order
            
        Returns:
            PaginatedResult: Paginated results with metadata
        """
        try:
            # Build base query
            stmt = select(self.model_class)
            count_stmt = select(func.count()).select_from(self.model_class)
            
            # Apply filters to both queries
            if filters:
                stmt = self._apply_filters(stmt, filters)
                count_stmt = self._apply_filters(count_stmt, filters)
            
            # Get total count
            count_result = await self.session.execute(count_stmt)
            total_count = count_result.scalar()
            
            # Apply ordering and pagination to main query
            if order_by:
                order_field = getattr(self.model_class, order_by, None)
                if order_field is not None:
                    if order_desc:
                        stmt = stmt.order_by(order_field.desc())
                    else:
                        stmt = stmt.order_by(order_field)
            
            stmt = stmt.offset(pagination.offset).limit(pagination.limit)
            
            # Execute main query
            result = await self.session.execute(stmt)
            records = result.scalars().all()
            
            logger.debug(f"Retrieved paginated {self.model_class.__name__} records: "
                        f"{len(records)}/{total_count} (page {pagination.skip//pagination.limit + 1})")
            
            return PaginatedResult(
                items=list(records),
                total_count=total_count,
                pagination=pagination
            )
        except SQLAlchemyError as e:
            logger.error(f"Failed to get paginated {self.model_class.__name__} records: {e}")
            raise
    
    async def update(self, record_id: Any, **kwargs) -> Optional[ModelType]:
        """
        Update a record by its primary key.
        
        Args:
            record_id: Primary key value
            **kwargs: Fields to update
            
        Returns:
            Optional[ModelType]: Updated record if found, None otherwise
        """
        try:
            pk_field = getattr(self.model_class, self.get_primary_key_field())
            
            # Add updated_at timestamp if the model has this field
            if hasattr(self.model_class, 'updated_at'):
                kwargs['updated_at'] = datetime.utcnow()
            
            stmt = (update(self.model_class)
                   .where(pk_field == record_id)
                   .values(**kwargs)
                   .returning(self.model_class))
            
            result = await self.session.execute(stmt)
            updated_record = result.scalar_one_or_none()
            
            if updated_record:
                await self.session.refresh(updated_record)
                logger.debug(f"Updated {self.model_class.__name__} with ID: {record_id}")
            else:
                logger.warning(f"{self.model_class.__name__} with ID {record_id} not found for update")
            
            return updated_record
        except SQLAlchemyError as e:
            logger.error(f"Failed to update {self.model_class.__name__} with ID {record_id}: {e}")
            raise
    
    async def delete(self, record_id: Any) -> bool:
        """
        Delete a record by its primary key.
        
        Args:
            record_id: Primary key value
            
        Returns:
            bool: True if record was deleted, False if not found
        """
        try:
            pk_field = getattr(self.model_class, self.get_primary_key_field())
            stmt = delete(self.model_class).where(pk_field == record_id)
            result = await self.session.execute(stmt)
            
            deleted = result.rowcount > 0
            if deleted:
                logger.debug(f"Deleted {self.model_class.__name__} with ID: {record_id}")
            else:
                logger.warning(f"{self.model_class.__name__} with ID {record_id} not found for deletion")
            
            return deleted
        except SQLAlchemyError as e:
            logger.error(f"Failed to delete {self.model_class.__name__} with ID {record_id}: {e}")
            raise
    
    async def exists(self, record_id: Any) -> bool:
        """
        Check if a record exists by its primary key.
        
        Args:
            record_id: Primary key value
            
        Returns:
            bool: True if record exists, False otherwise
        """
        try:
            pk_field = getattr(self.model_class, self.get_primary_key_field())
            stmt = select(func.count()).select_from(self.model_class).where(pk_field == record_id)
            result = await self.session.execute(stmt)
            count = result.scalar()
            return count > 0
        except SQLAlchemyError as e:
            logger.error(f"Failed to check existence of {self.model_class.__name__} with ID {record_id}: {e}")
            raise
    
    async def count(self, filters: Optional[List[FilterCondition]] = None) -> int:
        """
        Count records matching the given filters.
        
        Args:
            filters: List of filter conditions
            
        Returns:
            int: Number of matching records
        """
        try:
            stmt = select(func.count()).select_from(self.model_class)
            
            if filters:
                stmt = self._apply_filters(stmt, filters)
            
            result = await self.session.execute(stmt)
            count = result.scalar()
            logger.debug(f"Counted {count} {self.model_class.__name__} records")
            return count
        except SQLAlchemyError as e:
            logger.error(f"Failed to count {self.model_class.__name__} records: {e}")
            raise
    
    # Utility methods
    
    def _apply_filters(self, stmt, filters: List[FilterCondition]):
        """
        Apply filter conditions to a SQLAlchemy statement.
        
        Args:
            stmt: SQLAlchemy statement
            filters: List of filter conditions
            
        Returns:
            Modified SQLAlchemy statement
        """
        conditions = []
        
        for filter_condition in filters:
            field = getattr(self.model_class, filter_condition.field, None)
            if field is None:
                logger.warning(f"Field '{filter_condition.field}' not found in {self.model_class.__name__}")
                continue
            
            operator = filter_condition.operator
            value = filter_condition.value
            
            if operator == FilterOperator.EQ:
                conditions.append(field == value)
            elif operator == FilterOperator.NE:
                conditions.append(field != value)
            elif operator == FilterOperator.GT:
                conditions.append(field > value)
            elif operator == FilterOperator.GTE:
                conditions.append(field >= value)
            elif operator == FilterOperator.LT:
                conditions.append(field < value)
            elif operator == FilterOperator.LTE:
                conditions.append(field <= value)
            elif operator == FilterOperator.LIKE:
                conditions.append(field.like(value))
            elif operator == FilterOperator.ILIKE:
                conditions.append(field.ilike(value))
            elif operator == FilterOperator.IN:
                if isinstance(value, (list, tuple)):
                    conditions.append(field.in_(value))
            elif operator == FilterOperator.NOT_IN:
                if isinstance(value, (list, tuple)):
                    conditions.append(~field.in_(value))
            elif operator == FilterOperator.IS_NULL:
                conditions.append(field.is_(None))
            elif operator == FilterOperator.IS_NOT_NULL:
                conditions.append(field.is_not(None))
            else:
                logger.warning(f"Unsupported filter operator: {operator}")
        
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        return stmt
    
    def create_filter(self, field: str, operator: str, value: Any = None) -> FilterCondition:
        """
        Create a filter condition.
        
        Args:
            field: Field name
            operator: Filter operator
            value: Filter value
            
        Returns:
            FilterCondition: Created filter condition
        """
        return FilterCondition(field, operator, value)
    
    def create_date_range_filters(self, 
                                 date_field: str, 
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> List[FilterCondition]:
        """
        Create date range filter conditions.
        
        Args:
            date_field: Name of the date field
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)
            
        Returns:
            List[FilterCondition]: List of date range filters
        """
        filters = []
        
        if start_date:
            filters.append(FilterCondition(date_field, FilterOperator.GTE, start_date))
        
        if end_date:
            filters.append(FilterCondition(date_field, FilterOperator.LTE, end_date))
        
        return filters