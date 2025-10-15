import os
import sqlite3
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

default_args = {
    "owner": "ilies",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

base_path = Variable.get("BASE_PATH")
base_data_path = Variable.get("BASE_DATA_PATH")
hook = PostgresHook(postgres_conn_id="staging_postgres")
engine = hook.get_sqlalchemy_engine()

def _insert_to_db(table: Table, df: pd.DataFrame):
        records = df.to_dict(orient="records")
        metadata = MetaData()
        with engine.begin() as conn:
            if table.name == "network_costs":
                query = f"SELECT * FROM {table.schema}.{table.name}"
                raw_conn = engine.raw_connection()
                existing_data = pd.read_sql(query, raw_conn)
                raw_conn.close()
                if existing_data.empty:
                    target_tbl = Table(
                        "network_costs",
                        metadata,
                        autoload_with=engine,
                        schema="raw_data",
                    )
                    conn.execute(target_tbl.insert(), records)
                else:
                    existing_data["postcode"] = existing_data["postcode"].astype(int)
                    if existing_data.empty:
                        target_tbl = Table(
                            "network_costs",
                            metadata,
                            autoload_with=engine,
                            schema="raw_data",
                        )
                        conn.execute(target_tbl.insert(), records)
                    else:
                        df["postcode"] = df["postcode"].astype(int)
                        existing_keys = set(zip(existing_data["postcode"], existing_data["network_operator"]))
                        mask = df.apply(lambda r: (r["postcode"], r["network_operator"]) not in existing_keys, axis=1)
                        new_data = df[mask]

                        if not new_data.empty:
                            new_records = new_data.to_dict(orient="records")
                            target_tbl = Table(
                                "network_costs",
                                metadata,
                                autoload_with=engine,
                                schema="raw_data",
                            )
                            conn.execute(target_tbl.insert(), new_records)

            elif table.name == "price_comparison":
                query = f"SELECT * FROM {table.schema}.{table.name}"
                raw_conn = engine.raw_connection()
                existing_data = pd.read_sql(query, raw_conn)
                raw_conn.close()
                existing_data["report_date"] = pd.to_datetime(existing_data["report_date"])
                df["report_date"] = pd.to_datetime(df["report_date"])
                existing_data["postcode"] = existing_data["postcode"].astype(int)
                df["postcode"] = df["postcode"].astype(int)
                existing_keys = set(zip(existing_data["report_date"], existing_data["postcode"]))
                mask = df.apply(lambda r: (r["report_date"], r["postcode"]) not in existing_keys, axis=1)
                new_data = df[mask]
                
                if not new_data.empty:
                    new_records = new_data.to_dict(orient="records")
                    target_tbl = Table(
                        "price_comparison",
                        metadata,
                        autoload_with=engine,
                        schema="raw_data",
                    )
                    conn.execute(target_tbl.insert(), new_records)

            elif table.name == "products":
                stmt = insert(table).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["product_id"], 
                    set_={
                        "product_id": stmt.excluded.product_id,
                        "code": stmt.excluded.code, 
                        "description": stmt.excluded.description, 
                    }
                )
                conn.execute(stmt)

            elif table.name == "product_rates":
                stmt = insert(table).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["product_id", "valid_from"],  
                    set_={
                        "price_per_unit": stmt.excluded.price_per_unit,
                        "valid_to": stmt.excluded.valid_to,
                        "band": stmt.excluded.band,
                        "unit_type": stmt.excluded.unit_type
                    }
                )
                conn.execute(stmt)

with DAG(
    dag_id="octopus_pricing_midnight",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule="0 0 * * *",
    catchup=False,
) as dag_midnight:

    def ingest_backend_db():
        src_conn = sqlite3.connect(Variable.get("BACKEND_DB_FILE"))
        meta = MetaData()
        for tbl_name in ["products", "product_rates"]:
            df = pd.read_sql(f"SELECT * FROM {tbl_name}", src_conn)
            target_tbl = Table(
                tbl_name,
                meta,
                autoload_with=engine,
                schema="raw_data",
            )
            _insert_to_db(target_tbl, df)
        src_conn.close()

    ingest_bb = PythonOperator(
        task_id="ingest_backend_db",
        python_callable=ingest_backend_db,
    )

    dbt_run_mid = BashOperator(
        task_id="dbt_run",
        bash_command="cd {{ var.value.BASE_PATH }}/octopus_pricing && dbt run --models final_analytics_table",
    )

    ingest_bb >> dbt_run_mid

with DAG(
    dag_id="octopus_pricing_8am",
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    schedule="0 8 * * *",
    catchup=False,
) as dag_8am:

    def ingest_price_comparison():
        base_data_path = Variable.get("BASE_DATA_PATH")
        meta = MetaData()
        price_tbl = Table("price_comparison", meta, autoload_with=engine, schema="raw_data")
        start = datetime(2024, 1, 1)
        end = datetime(2024, 2, 12)
        current = start
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            path = f"{base_data_path}/price_comparison_{date_str}.csv"
            try:
                df = pd.read_csv(path, parse_dates=["report_date"])
                _insert_to_db(price_tbl, df)
            except FileNotFoundError:
                pass
            current += timedelta(days=7)

    ingest_pc2 = PythonOperator(
        task_id="ingest_price_comparison",
        python_callable=ingest_price_comparison,
    )

    def ingest_network_costs():
        base_data_path = Variable.get("BASE_DATA_PATH")
        filename = Variable.get("NETWORK_COSTS_FILE")
        df = pd.read_csv(f"{base_data_path}/{filename}")
        meta = MetaData()
        net_tbl = Table("network_costs", meta, autoload_with=engine, schema="raw_data")
        _insert_to_db(net_tbl, df)

    ingest_nc2 = PythonOperator(
        task_id="ingest_network_costs",
        python_callable=ingest_network_costs,
    )

    dbt_run_morn = BashOperator(
        task_id="dbt_run",
        bash_command="cd {{ var.value.BASE_PATH }}/octopus_pricing && dbt run --models final_analytics_table",
    )

    [ingest_pc2, ingest_nc2] >> dbt_run_morn