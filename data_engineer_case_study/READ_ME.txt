Create and activate Python virtual environment
```bash
python3 venv -m my_venv_name
source my_venv_name/bin/activate

### Local PostgreSQL Setup

# To run the pipeline locally, start a Postgres 15 container:
# Pull the Postgres 15 Docker image from Docker Hub.
```bash
docker pull postgres:15

# Launch a new container named "local-postgres"
# Run a container named local-postgres in detached mode.
# Set up a database user YOUR_USERNAME with password YOUR_PASSWORD
# Create a default database named YOUR_DATABASE_NAME
# Maps port 5432 inside the container to port 5432 on your local machine, so you can connect with postgresql://YOUR_USERNAME:YOUR_PASSWORD@localhost:5432/YOUR_DATABASE_NAME

```bash
docker run -d \
  --name local-postgres \
  -e POSTGRES_USER=YOUR_USERNAME \
  -e POSTGRES_PASSWORD=YOUR_PASSWORD \
  -e POSTGRES_DB=YOUR_DATABASE_NAME \
  -p 5432:5432 \
  postgres:15

# create schemas and tables
# run the postgresql interface
```bash
psql postgresql://YOUR_USERNAME:YOUR_PASSWORD@localhost:5432/YOUR_DATABASE_NAME  
# create schemas: 
# 1. raw data for storing all data from different sources
CREATE SCHEMA raw_data AUTHORIZATION octopus_challenge;
# 2. analytics for final usage
CREATE SCHEMA analytics AUTHORIZATION octopus_challenge;

# create tables in raw_data
# 1. network_costs for fetching data from network_costs.csv
CREATE TABLE raw_data.network_costs (
	postcode varchar NULL,
	town varchar NULL,
	network_operator varchar NULL,
	network_cost_per_year numeric NULL,
	"network_cost_per_kWh" numeric NULL,
	currency varchar NULL
);

# 2. price_comparison to fetch data from all price_comparison_{date}.csv files
CREATE TABLE raw_data.price_comparison (
	report_date date NULL,
	postcode varchar NULL,
	location_name varchar NULL,
	"annual_consumption_in_kWh" float8 NULL,
	market varchar NULL,
	ranking int4 NULL,
	gross_price_per_month numeric NULL,
	currency varchar NULL
);

# 3. products to fetch product data from backend.db
CREATE TABLE raw_data.products (
	product_id int4 NOT NULL,
	code text NOT NULL,
	description text NULL,
	CONSTRAINT products_pkey PRIMARY KEY (product_id)
);

# 4. products_rates to fetch product_rates data from backend.db
CREATE TABLE raw_data.product_rates (
	product_id int4 NOT NULL,
	valid_from date NOT NULL,
	valid_to date NULL,
	band varchar NULL,
	unit_type varchar NULL,
	currency varchar NULL,
	price_per_unit float8 NULL,
	CONSTRAINT product_rates_pkey PRIMARY KEY (product_id, valid_from)
);
#(Optional for performance)
CREATE INDEX idx_product_rates_valid_to ON raw_data.product_rates USING btree (valid_to);


### dbt Setup

This project uses dbt to manage and run SQL transformations against a local Postgres instance.

# 1. Install dbt

```bash
pip install dbt-core dbt-postgres

#Initialize the dbt project
```bash
dbt init octopus_pricing

#When the prompt asks you for:
a number: 1 
Host: localhost
Port: 5432
User: YOUR_USERNAME
Password: YOUR_PASSWORD
Database: YOUR_DATABASE_NAME
Schema: raw
threading: 1

# verifiy connection
```bash
cd octopus_pricing
dbt debug
-> should show "All checks passed"

# add sources.yml file to your models with:
version: 1

sources:
  - name: raw_data
    database: YOUR_DATABASE_NAME
    schema: raw_data
    tables:
      - name: network_costs
      - name: price_comparison
      - name: products
      - name: product_rates
  - name: analytics
    database: YOUR_DATABASE_NAME
    schema: analytics
    tables:
      - name: final_analytics_table

# edit your dbt_project.yml

name: 'octopus_pricing'
version: '1.0.0'

# This setting configures which "profile" dbt uses for this project.
profile: 'octopus_pricing'

# These configurations specify where dbt should look for different types of files.
# The `model-paths` config, for example, states that models in this project can be
# found in the "models/" directory. You probably won't need to change these!
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_packages"

models:
  octopus_pricing:
    final_analytics_table:
      materialized: table


### Airflow Setup

```bash
pip install apache-airflow

# by default Airflow uses ~/airflow, to point it into you project:
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize the metadata database
```bash
airflow db reset --yes
# This will create a fresh SQLite metadata DB under $AIRFLOW_HOME/airflow.db

# Configure your Postgres connection
# Register the staging database so Airflow tasks can write to it:
```bash
airflow connections add staging_postgres --conn-uri postgresql+psycopg2://YOUR_USERNAME:YOUR_PASSWORD@localhost:5432/YOUR_DATABASE_NAME

# Start Scheduler & UI/API server
# In one terminal:
```bash
airflow scheduler

# In a second terminal:
```bash
airflow api-server 

# In a third terminal:
```bash
airflow dag-processor

# OR run the ./start_airflow.sh script (when in the main folder which is running all three commands) -> to stop: run ./stop_airflow.sh

# Open the Airflow UI
# Browse to http://localhost:8080
# log in with the admin credentials found in $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated.
# drop your dags/ folder into $AIRFLOW_HOME/dags/

# set VARIABLES
```bash 
airflow variables set BASE_PATH /path/to/your/working_directory
airflow variables set BASE_DATA_PATH /path/to/your/data_folder
airflow variables set NETWORK_COSTS_FILE network_costs.csv
airflow variables set BACKEND_DB_FILE /path/to/your/backend.db
airflow variables set PRODUCT_ID 224
airflow variables set STAGING_POSTGRES_CONN postgresql+psycopg2://YOUR_USERNAME:YOUR_PASSWORD@localhost:5432/YOUR_DATABASE_NAME

