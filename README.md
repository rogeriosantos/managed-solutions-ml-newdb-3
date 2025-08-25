# CNC ML Monitoring API

A FastAPI-based backend system for Industry 4.0 CNC machine monitoring and machine learning analytics.

## Features

- REST API for CNC machine data access and management
- Machine learning models for predictive maintenance
- Downtime analysis and OEE (Overall Equipment Effectiveness) calculations
- Real-time analytics and reporting
- Integration with Railway MySQL database

## Project Structure

```
cnc-ml-monitoring/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry point
│   ├── config/
│   │   ├── __init__.py
│   │   ├── database.py         # Database configuration
│   │   └── settings.py         # Application settings
│   ├── models/
│   │   ├── __init__.py
│   │   ├── database_models.py  # SQLAlchemy models
│   │   └── pydantic_models.py  # Pydantic schemas
│   ├── repositories/
│   │   ├── __init__.py
│   │   ├── machine_repository.py
│   │   ├── operator_repository.py
│   │   ├── job_repository.py
│   │   └── part_repository.py
│   ├── services/
│   │   ├── __init__.py
│   │   ├── machine_service.py
│   │   ├── operator_service.py
│   │   ├── job_service.py
│   │   ├── part_service.py
│   │   ├── ml_service.py
│   │   └── analytics_service.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── dependencies.py
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── machines.py
│   │       ├── operators.py
│   │       ├── jobs.py
│   │       ├── parts.py
│   │       ├── ml_training.py
│   │       ├── predictions.py
│   │       └── analytics.py
│   └── ml/
│       ├── __init__.py
│       ├── feature_engineering.py
│       ├── models/
│       │   ├── __init__.py
│       │   ├── downtime_predictor.py
│       │   └── oee_optimizer.py
│       └── utils/
│           ├── __init__.py
│           ├── data_preprocessing.py
│           └── model_evaluation.py
├── tests/
│   ├── __init__.py
│   ├── test_api/
│   ├── test_services/
│   └── test_ml/
├── requirements.txt
├── .env.example
└── README.md
```

## Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cnc-ml-monitoring
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env file with your Railway database credentials
   # Replace 'your_railway_password_here' with your actual Railway database password
   ```

5. **Test database connection (optional)**
   ```bash
   python test_db_connection.py
   ```

6. **Run the application**
   ```bash
   python -m app.main
   # Or using uvicorn directly:
   uvicorn app.main:app --reload
   ```

## API Documentation

Once the application is running, you can access:
- Interactive API documentation: http://localhost:8000/docs
- Alternative documentation: http://localhost:8000/redoc
- Health check: http://localhost:8000/health

## Development

### Running Tests
```bash
pytest
```

### Code Formatting
```bash
black app/ tests/
```

### Type Checking
```bash
mypy app/
```

## Database

The application connects to a Railway MySQL database containing CNC machine operational data. The main table `joblog_ob` contains approximately 10,000 records with machine downtime metrics and operational data.

### Database Schema

The `joblog_ob` table contains the following key columns:
- `machine`: Machine identifier (e.g., Machine0001, Machine0002)
- `StartTime`: Job start timestamp
- `EndTime`: Job end timestamp  
- `JobNumber`: Unique job identifier
- `State`: Job state (OPENED, CLOSED)
- `PartNumber`: Part being manufactured
- `EmpID`: Employee ID
- `OperatorName`: Operator identifier
- `OpNumber`: Operation number
- `PartsProduced`: Number of parts produced
- `JobDuration`: Total job duration in seconds
- `RunningTime`: Actual machine running time in seconds
- `SetupTime`: Setup time in seconds
- `WaitingTime`: Waiting/idle time in seconds

This data enables analysis of:
- Machine utilization and efficiency
- Downtime patterns and causes
- Production rates and quality metrics
- Operator performance
- Overall Equipment Effectiveness (OEE)

## Machine Learning

The system includes ML capabilities for:
- Downtime prediction based on historical patterns
- OEE optimization recommendations
- Maintenance scheduling predictions
- Performance analytics and trend analysis

## License

[Add your license information here]