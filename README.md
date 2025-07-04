# Clinical Trials Metadata Management System

A production-ready data architecture solution for managing clinical trials metadata from ClinicalTrials.gov, featuring dimensional modeling, ETL pipelines, and analytics capabilities.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │    │   ETL Pipeline  │    │   Data Warehouse│    │   Analytics     │
│                 │    │                 │    │                 │    │                 │
│ ClinicalTrials  │───▶│ Extract & Load  │───▶│ Dimensional     │───▶│ Streamlit       │
│ .gov API        │    │ Transform       │    │ Schema          │    │ Dashboard       │
│                 │    │ Validate        │    │ (Star Schema)   │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 Data Model

### Star Schema Design
- **Fact Table**: `fact_trials` - Core trial metrics and measures
- **Dimension Tables**: 
  - `dim_sponsor` - Trial sponsors and organizations
  - `dim_location` - Geographic locations and sites
  - `dim_condition` - Medical conditions and diseases
  - `dim_dates` - Date dimension for temporal analysis
  - `dim_intervention` - Drug/treatment interventions

### Key Metrics
- Trial counts by sponsor, condition, location
- Trial duration and enrollment statistics
- Phase distribution and status tracking
- Geographic analysis and site performance

## 🚦 Usage

See [docs/USAGE.md](docs/USAGE.md) for full instructions.

### Quick Start

1. **Set up your environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. **(Optional) Configure the database:**
   - By default, the app uses `clinical_trials.db` in your project root.
   - To change the database location or settings, edit `config/database.yml`.
3. **(Optional) Extract and load data:**
   - If you want to fetch fresh data or the database is missing/corrupted:
     ```bash
     python data_ingestion/extract_trials.py
     python data_cleaning/transform_trials.py
     ```
4. **Start the dashboard:**
   ```bash
   streamlit run dashboard/app.py
   ```
5. **(Optional) Run tests:**
   ```bash
   pytest tests/
   ```

---

## 📁 Project Structure

```
clinical_trails/
├── data_ingestion/           # ETL extraction and loading
│   ├── extract_trials.py     # Extract from ClinicalTrials.gov API
│   └── api_client.py         # API client utilities
├── data_cleaning/            # Data transformation and validation
│   └── transform_trials.py   # Clean and normalize data
├── sql/                      # Database schema and transformations
│   ├── create_schema.sql     # Dimensional schema creation
│   ├── staging_transforms.sql # Raw to staging transformations
│   └── warehouse_transforms.sql # Staging to warehouse loads
├── dashboard/                # Analytics and visualization
│   ├── app.py               # Streamlit dashboard
│   └── queries.py           # Analytical queries
├── utils/                    # Shared utilities
│   ├── config.py            # Configuration management
│   ├── logging.py           # Logging setup
│   ├── database.py          # Database connection utilities
│   └── helpers.py           # Common helper functions
├── config/                   # Configuration files
│   ├── database.yml         # Database connection settings
│   └── database.example.yml # Example config
├── tests/                    # Unit and integration tests
│   └── test_basic.py        # Basic tests
├── logs/                     # ETL and app logs
│   └── clinical_trials.log  # Log file
├── docs/                     # Documentation (see USAGE.md for usage)
├── requirements.txt          # Python dependencies
├── clinical_trials.db        # SQLite database file (auto-created)
└── README.md                # This file
```

## 🛠️ Technology Stack

- **Data Storage**: SQLite (file-based, no server required)
- **ETL/ELT**: Python 3.8+ with pandas, requests
- **Analytics**: Streamlit, Plotly
- **Database**: SQL with dimensional modeling
- **Monitoring**: Structured logging with timestamps

## 📈 Sample Queries

### Top 10 Sponsors by Number of Trials (2023)
```sql
SELECT 
    s.sponsor_name,
    COUNT(*) as trial_count,
    AVG(t.enrollment_count) as avg_enrollment
FROM fact_trials t
JOIN dim_sponsor s ON t.sponsor_key = s.sponsor_key
JOIN dim_dates d ON t.start_date_key = d.date_key
WHERE d.year = 2023
GROUP BY s.sponsor_name
ORDER BY trial_count DESC
LIMIT 10;
```

### Most Common Trial Conditions and Average Duration
```sql
SELECT 
    c.condition_name,
    COUNT(*) as trial_count,
    AVG(t.duration_days) as avg_duration_days,
    AVG(t.enrollment_count) as avg_enrollment
FROM fact_trials t
JOIN dim_condition c ON t.condition_key = c.condition_key
GROUP BY c.condition_name
ORDER BY trial_count DESC
LIMIT 20;
```

### Geographic Distribution of Trials
```sql
SELECT 
    l.country,
    l.state,
    COUNT(*) as trial_count,
    SUM(t.enrollment_count) as total_enrollment
FROM fact_trials t
JOIN dim_location l ON t.location_key = l.location_key
GROUP BY l.country, l.state
ORDER BY trial_count DESC;
```

## 🔍 Data Quality & Monitoring

### Automated Checks
- Missing value detection and reporting
- Data type validation and conversion
- Duplicate record identification
- Referential integrity validation
- Business rule validation

### Logging & Monitoring
- ETL job execution logs with timestamps
- Row count tracking at each stage
- Data quality score calculation
- Performance metrics and execution times
- Error handling and alerting

## 🧪 Testing

Run the test suite:
```bash
pytest tests/
```

## 📊 Dashboard Features

- **Trial Overview**: Summary statistics and trends
- **Sponsor Analysis**: Top sponsors and performance metrics
- **Geographic Distribution**: Map-based trial location analysis
- **Condition Analysis**: Most studied conditions and outcomes
- **Temporal Trends**: Trial activity over time
- **Data Quality**: Quality metrics and issue tracking

## 🔧 Configuration

All configuration is managed through YAML files:
- `config/database.yml`: Database connection settings
- `config/database.example.yml`: Example config

## 📝 Documentation

- [Architecture Guide](docs/architecture.md)
- [Data Model Documentation](docs/data_model.md)
- [API Documentation](docs/api.md)
- [Deployment Guide](docs/deployment.md)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For questions or issues, please open a GitHub issue. # clinical_trails_ETL
