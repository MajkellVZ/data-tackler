## Prerequisites

- Docker & Docker Compose installed
- Football-data API key

---

## Setup Instructions

1. **Clone the repository**:

```bash
git clone <repository_url>
cd data-tackler/airflow
```

2. **Set up .env file**:

```bash
AIRFLOW_UID=501
```

3. **Place API KEY in the docker-compose.yaml**:

```bash
FOOTBALL_API_KEY: 'api-key'
```

4. **Set up .env file**:

```bash
docker-compose up --build
```

5. **Go to http://localhost:8501**