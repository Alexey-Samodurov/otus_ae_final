# Otus analytics engineer final project
Theme: Автоматизация аналитики и визуализации данных на примере медийных кабинетов, с помощью Airflow, DBT, ClickHouse и Superset. 
## How to run:

### Note: 
- All commands should be run from the root of the project
- Supported only Unix-like systems

1. Add environment variables in `.env.airflow` file:
   - YANDEX_DIRECT_TOKEN
   - MY_TARGET_CLIENT_ID
   - MY_TARGET_CLIENT_SECRET
   - S3_KEY
   - S3_SECRET

2. Run `run.sh`:
    - example: `./run.sh`

3. Airflow available url: `http://localhost:8080`

4. Superset available url: `http://localhost:8088`
