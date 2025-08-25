"""
CNC ML Monitoring Application - FastAPI Entry Point

This module serves as the main entry point for the FastAPI application,
providing REST API endpoints for CNC machine monitoring and ML analytics.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config.settings import get_settings
from app.config.database import init_database, close_database

# Initialize settings
settings = get_settings()

# Create FastAPI application instance
app = FastAPI(
    title="CNC ML Monitoring API",
    description="REST API for CNC machine monitoring, downtime analysis, and ML-based predictive maintenance",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Application startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database and other startup tasks."""
    await init_database()

# Application shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on application shutdown."""
    await close_database()

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Root endpoint providing basic API information."""
    return {
        "message": "CNC ML Monitoring API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring system status."""
    from app.config.database import connection_manager
    
    # Get comprehensive database health info
    health_info = await connection_manager.health_check()
    
    return {
        "status": "healthy" if health_info["status"] == "healthy" else "degraded",
        "database": {
            "status": health_info["status"],
            "connection_pool": health_info.get("connection_pool", {}),
            "info": health_info.get("database_info", {}),
            "error": health_info.get("error")
        },
        "ml_engine": "ready"
    }


@app.get("/health/database")
async def database_health_check():
    """Detailed database health check endpoint."""
    from app.config.database import connection_manager
    
    # Get comprehensive health check
    health_info = await connection_manager.health_check()
    
    # Get CRUD operations test results
    crud_results = await connection_manager.test_crud_operations()
    
    return {
        "health_check": health_info,
        "crud_test": crud_results,
        "timestamp": "2025-08-25T17:58:46Z"  # This would be dynamic in real implementation
    }

# API routes will be added in subsequent tasks
# app.include_router(machines.router, prefix="/api/v1", tags=["machines"])
# app.include_router(operators.router, prefix="/api/v1", tags=["operators"])
# app.include_router(jobs.router, prefix="/api/v1", tags=["jobs"])
# app.include_router(parts.router, prefix="/api/v1", tags=["parts"])
# app.include_router(ml_training.router, prefix="/api/v1", tags=["ml-training"])
# app.include_router(predictions.router, prefix="/api/v1", tags=["predictions"])
# app.include_router(analytics.router, prefix="/api/v1", tags=["analytics"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level="info"
    )