"""
Unit tests for BaseRepository class.

This module tests the common CRUD operations, pagination, filtering,
and utility methods provided by the BaseRepository base class.
"""

import pytest
from datetime import datetime, date
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from app.repositories.base_repository import (
    BaseRepository, FilterCondition, FilterOperator, 
    PaginationParams, PaginatedResult
)

# Test model for repository testing
Base = declarative_base()


class MockTestModel:
    """Mock test model for repository unit tests."""
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def __repr__(self):
        return f"MockTestModel({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"


class TestModel(Base):
    """Test model for repository unit tests."""
    __tablename__ = "test_model"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)


class TestRepository(BaseRepository[TestModel]):
    """Concrete implementation of BaseRepository for testing."""
    
    def get_primary_key_field(self) -> str:
        return "id"


class TestFilterCondition:
    """Test cases for FilterCondition class."""
    
    def test_filter_condition_creation(self):
        """Test FilterCondition creation and representation."""
        filter_cond = FilterCondition("name", FilterOperator.EQ, "test")
        
        assert filter_cond.field == "name"
        assert filter_cond.operator == FilterOperator.EQ
        assert filter_cond.value == "test"
        assert "FilterCondition" in repr(filter_cond)
    
    def test_filter_condition_without_value(self):
        """Test FilterCondition creation without value (for IS_NULL operations)."""
        filter_cond = FilterCondition("name", FilterOperator.IS_NULL)
        
        assert filter_cond.field == "name"
        assert filter_cond.operator == FilterOperator.IS_NULL
        assert filter_cond.value is None


class TestPaginationParams:
    """Test cases for PaginationParams class."""
    
    def test_pagination_params_creation(self):
        """Test PaginationParams creation with default values."""
        pagination = PaginationParams()
        
        assert pagination.skip == 0
        assert pagination.limit == 100
        assert pagination.max_limit == 1000
        assert pagination.offset == 0
    
    def test_pagination_params_custom_values(self):
        """Test PaginationParams creation with custom values."""
        pagination = PaginationParams(skip=20, limit=50, max_limit=500)
        
        assert pagination.skip == 20
        assert pagination.limit == 50
        assert pagination.max_limit == 500
        assert pagination.offset == 20
    
    def test_pagination_params_validation(self):
        """Test PaginationParams validation of input values."""
        # Test negative skip becomes 0
        pagination = PaginationParams(skip=-10)
        assert pagination.skip == 0
        
        # Test zero limit becomes 1
        pagination = PaginationParams(limit=0)
        assert pagination.limit == 1
        
        # Test limit exceeding max_limit is capped
        pagination = PaginationParams(limit=2000, max_limit=1000)
        assert pagination.limit == 1000
    
    def test_pagination_params_repr(self):
        """Test PaginationParams string representation."""
        pagination = PaginationParams(skip=10, limit=25)
        repr_str = repr(pagination)
        
        assert "PaginationParams" in repr_str
        assert "skip=10" in repr_str
        assert "limit=25" in repr_str


class TestPaginatedResult:
    """Test cases for PaginatedResult class."""
    
    def test_paginated_result_creation(self):
        """Test PaginatedResult creation and basic properties."""
        items = [1, 2, 3, 4, 5]
        pagination = PaginationParams(skip=0, limit=5)
        result = PaginatedResult(items, total_count=20, pagination=pagination)
        
        assert result.items == items
        assert result.total_count == 20
        assert result.pagination == pagination
    
    def test_paginated_result_navigation_properties(self):
        """Test navigation properties of PaginatedResult."""
        items = [1, 2, 3, 4, 5]
        
        # First page
        pagination = PaginationParams(skip=0, limit=5)
        result = PaginatedResult(items, total_count=20, pagination=pagination)
        
        assert result.has_next is True
        assert result.has_previous is False
        assert result.page_number == 1
        assert result.total_pages == 4
        
        # Middle page
        pagination = PaginationParams(skip=5, limit=5)
        result = PaginatedResult(items, total_count=20, pagination=pagination)
        
        assert result.has_next is True
        assert result.has_previous is True
        assert result.page_number == 2
        assert result.total_pages == 4
        
        # Last page
        pagination = PaginationParams(skip=15, limit=5)
        result = PaginatedResult(items, total_count=20, pagination=pagination)
        
        assert result.has_next is False
        assert result.has_previous is True
        assert result.page_number == 4
        assert result.total_pages == 4
    
    def test_paginated_result_edge_cases(self):
        """Test PaginatedResult with edge cases."""
        # Empty result
        pagination = PaginationParams(skip=0, limit=10)
        result = PaginatedResult([], total_count=0, pagination=pagination)
        
        assert result.has_next is False
        assert result.has_previous is False
        assert result.page_number == 1
        assert result.total_pages == 0
        
        # Single item
        pagination = PaginationParams(skip=0, limit=10)
        result = PaginatedResult([1], total_count=1, pagination=pagination)
        
        assert result.has_next is False
        assert result.has_previous is False
        assert result.page_number == 1
        assert result.total_pages == 1
    
    def test_paginated_result_repr(self):
        """Test PaginatedResult string representation."""
        items = [1, 2, 3]
        pagination = PaginationParams(skip=0, limit=5)
        result = PaginatedResult(items, total_count=10, pagination=pagination)
        repr_str = repr(result)
        
        assert "PaginatedResult" in repr_str
        assert "items=3" in repr_str
        assert "total_count=10" in repr_str


@pytest.mark.asyncio
class TestBaseRepository:
    """Test cases for BaseRepository class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock AsyncSession for testing."""
        session = AsyncMock(spec=AsyncSession)
        return session
    
    @pytest.fixture
    def repository(self, mock_session):
        """Create a TestRepository instance for testing."""
        return TestRepository(mock_session, TestModel)
    
    def test_repository_initialization(self, mock_session):
        """Test repository initialization."""
        repo = TestRepository(mock_session, TestModel)
        
        assert repo.session == mock_session
        assert repo.model_class == TestModel
        assert repo.get_primary_key_field() == "id"
    
    async def test_create_success(self, repository, mock_session):
        """Test successful record creation."""
        # Mock the created instance
        mock_instance = MockTestModel(id=1, name="test")
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock()
        mock_session.refresh = AsyncMock()
        
        # Mock the model class constructor
        with patch.object(TestModel, '__new__', return_value=mock_instance):
            result = await repository.create(name="test")
        
        assert result == mock_instance
        mock_session.add.assert_called_once_with(mock_instance)
        mock_session.flush.assert_called_once()
        mock_session.refresh.assert_called_once_with(mock_instance)
    
    async def test_create_failure(self, repository, mock_session):
        """Test record creation failure."""
        mock_instance = MockTestModel(name="test")
        mock_session.add = MagicMock()
        mock_session.flush = AsyncMock(side_effect=SQLAlchemyError("Database error"))
        
        # Mock the model class constructor
        with patch.object(TestModel, '__new__', return_value=mock_instance):
            with pytest.raises(SQLAlchemyError):
                await repository.create(name="test")
    
    async def test_get_by_id_success(self, repository, mock_session):
        """Test successful record retrieval by ID."""
        mock_instance = MockTestModel(id=1, name="test")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_instance
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_by_id(1)
        
        assert result == mock_instance
        mock_session.execute.assert_called_once()
    
    async def test_get_by_id_not_found(self, repository, mock_session):
        """Test record retrieval by ID when record not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_by_id(999)
        
        assert result is None
        mock_session.execute.assert_called_once()
    
    async def test_get_by_id_failure(self, repository, mock_session):
        """Test record retrieval by ID failure."""
        mock_session.execute = AsyncMock(side_effect=SQLAlchemyError("Database error"))
        
        with pytest.raises(SQLAlchemyError):
            await repository.get_by_id(1)
    
    async def test_get_all_success(self, repository, mock_session):
        """Test successful retrieval of all records."""
        mock_instances = [MockTestModel(id=1, name="test1"), MockTestModel(id=2, name="test2")]
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = mock_instances
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_all()
        
        assert result == mock_instances
        mock_session.execute.assert_called_once()
    
    async def test_get_all_with_filters(self, repository, mock_session):
        """Test retrieval of all records with filters."""
        mock_instances = [MockTestModel(id=1, name="test1")]
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = mock_instances
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        filters = [FilterCondition("name", FilterOperator.EQ, "test1")]
        result = await repository.get_all(filters=filters)
        
        assert result == mock_instances
        mock_session.execute.assert_called_once()
    
    async def test_get_all_with_ordering(self, repository, mock_session):
        """Test retrieval of all records with ordering."""
        mock_instances = [MockTestModel(id=2, name="test2"), MockTestModel(id=1, name="test1")]
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = mock_instances
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.get_all(order_by="id", order_desc=True)
        
        assert result == mock_instances
        mock_session.execute.assert_called_once()
    
    async def test_get_paginated_success(self, repository, mock_session):
        """Test successful paginated retrieval."""
        mock_instances = [MockTestModel(id=1, name="test1"), MockTestModel(id=2, name="test2")]
        
        # Mock count query
        mock_count_result = MagicMock()
        mock_count_result.scalar.return_value = 10
        
        # Mock main query
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = mock_instances
        mock_result.scalars.return_value = mock_scalars
        
        # Configure session to return different results for different queries
        mock_session.execute = AsyncMock(side_effect=[mock_count_result, mock_result])
        
        pagination = PaginationParams(skip=0, limit=5)
        result = await repository.get_paginated(pagination)
        
        assert isinstance(result, PaginatedResult)
        assert result.items == mock_instances
        assert result.total_count == 10
        assert result.pagination == pagination
        assert mock_session.execute.call_count == 2
    
    async def test_update_success(self, repository, mock_session):
        """Test successful record update."""
        mock_instance = MockTestModel(id=1, name="updated")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_instance
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.refresh = AsyncMock()
        
        result = await repository.update(1, name="updated")
        
        assert result == mock_instance
        mock_session.execute.assert_called_once()
        mock_session.refresh.assert_called_once_with(mock_instance)
    
    async def test_update_not_found(self, repository, mock_session):
        """Test record update when record not found."""
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.update(999, name="updated")
        
        assert result is None
        mock_session.execute.assert_called_once()
    
    async def test_delete_success(self, repository, mock_session):
        """Test successful record deletion."""
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.delete(1)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    async def test_delete_not_found(self, repository, mock_session):
        """Test record deletion when record not found."""
        mock_result = MagicMock()
        mock_result.rowcount = 0
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.delete(999)
        
        assert result is False
        mock_session.execute.assert_called_once()
    
    async def test_exists_true(self, repository, mock_session):
        """Test exists method when record exists."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 1
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.exists(1)
        
        assert result is True
        mock_session.execute.assert_called_once()
    
    async def test_exists_false(self, repository, mock_session):
        """Test exists method when record does not exist."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 0
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.exists(999)
        
        assert result is False
        mock_session.execute.assert_called_once()
    
    async def test_count_success(self, repository, mock_session):
        """Test successful record count."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 5
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        result = await repository.count()
        
        assert result == 5
        mock_session.execute.assert_called_once()
    
    async def test_count_with_filters(self, repository, mock_session):
        """Test record count with filters."""
        mock_result = MagicMock()
        mock_result.scalar.return_value = 2
        mock_session.execute = AsyncMock(return_value=mock_result)
        
        filters = [FilterCondition("name", FilterOperator.LIKE, "test%")]
        result = await repository.count(filters=filters)
        
        assert result == 2
        mock_session.execute.assert_called_once()
    
    def test_create_filter(self, repository):
        """Test filter creation utility method."""
        filter_cond = repository.create_filter("name", FilterOperator.EQ, "test")
        
        assert isinstance(filter_cond, FilterCondition)
        assert filter_cond.field == "name"
        assert filter_cond.operator == FilterOperator.EQ
        assert filter_cond.value == "test"
    
    def test_create_date_range_filters(self, repository):
        """Test date range filter creation utility method."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        
        filters = repository.create_date_range_filters("created_at", start_date, end_date)
        
        assert len(filters) == 2
        assert filters[0].field == "created_at"
        assert filters[0].operator == FilterOperator.GTE
        assert filters[0].value == start_date
        assert filters[1].field == "created_at"
        assert filters[1].operator == FilterOperator.LTE
        assert filters[1].value == end_date
    
    def test_create_date_range_filters_partial(self, repository):
        """Test date range filter creation with only start or end date."""
        start_date = datetime(2023, 1, 1)
        
        # Only start date
        filters = repository.create_date_range_filters("created_at", start_date=start_date)
        assert len(filters) == 1
        assert filters[0].operator == FilterOperator.GTE
        
        # Only end date
        end_date = datetime(2023, 12, 31)
        filters = repository.create_date_range_filters("created_at", end_date=end_date)
        assert len(filters) == 1
        assert filters[0].operator == FilterOperator.LTE
        
        # Neither date
        filters = repository.create_date_range_filters("created_at")
        assert len(filters) == 0


class TestFilterOperators:
    """Test cases for filter operators."""
    
    def test_filter_operators_constants(self):
        """Test that all filter operator constants are defined."""
        assert FilterOperator.EQ == "eq"
        assert FilterOperator.NE == "ne"
        assert FilterOperator.GT == "gt"
        assert FilterOperator.GTE == "gte"
        assert FilterOperator.LT == "lt"
        assert FilterOperator.LTE == "lte"
        assert FilterOperator.LIKE == "like"
        assert FilterOperator.ILIKE == "ilike"
        assert FilterOperator.IN == "in"
        assert FilterOperator.NOT_IN == "not_in"
        assert FilterOperator.IS_NULL == "is_null"
        assert FilterOperator.IS_NOT_NULL == "is_not_null"