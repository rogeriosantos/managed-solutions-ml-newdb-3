"""
Application Settings Configuration

This module handles all application configuration including database connections,
API settings, and ML model parameters using environment variables.
"""

from functools import lru_cache
from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Application settings
    app_name: str = Field(default="CNC ML Monitoring API", env="APP_NAME")
    debug: bool = Field(default=False, env="DEBUG")
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")
    
    # CORS settings
    allowed_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        env="ALLOWED_ORIGINS"
    )
    
    # Database settings (Railway MySQL)
    database_url: str = Field(
        default="mysql+aiomysql://root:password@localhost:3306/railway",
        env="DATABASE_URL"
    )
    database_host: str = Field(default="gondola.proxy.rlwy.net", env="DB_HOST")
    database_port: int = Field(default=21632, env="DB_PORT")
    database_user: str = Field(default="root", env="DB_USER")
    database_password: str = Field(default="", env="DB_PASSWORD")
    database_name: str = Field(default="railway", env="DB_NAME")
    
    # Database connection pool settings
    db_pool_size: int = Field(default=10, env="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=20, env="DB_MAX_OVERFLOW")
    db_pool_timeout: int = Field(default=30, env="DB_POOL_TIMEOUT")
    db_pool_recycle: int = Field(default=3600, env="DB_POOL_RECYCLE")
    
    # ML Model settings
    ml_model_storage_path: str = Field(default="./models", env="ML_MODEL_STORAGE_PATH")
    ml_feature_cache_ttl: int = Field(default=3600, env="ML_FEATURE_CACHE_TTL")  # seconds
    ml_prediction_batch_size: int = Field(default=100, env="ML_PREDICTION_BATCH_SIZE")
    
    # API settings
    api_rate_limit: int = Field(default=100, env="API_RATE_LIMIT")  # requests per minute
    api_timeout: int = Field(default=30, env="API_TIMEOUT")  # seconds
    
    # Logging settings
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )
    
    # Development settings
    skip_db_init: bool = Field(default=False, env="SKIP_DB_INIT")
    
    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Allow extra fields to be ignored


@lru_cache()
def get_settings() -> Settings:
    """
    Get application settings with caching.
    
    Returns:
        Settings: Cached application settings instance
    """
    return Settings()