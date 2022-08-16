from google.cloud import bigquery
import pandas as pd
import logging


def run_job(df: pd.DataFrame, table_info: dict) -> None:
    """
    Função para rodar um job no BigQuery

    Args:
        df (pd.DataFrame): Dataframe de dados para inserção no BQ
        table_info (dict): Informações do nome completo e do schema da tabela no BQ
    """
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = table_info["name"]
    schema = format_schema(table_info["schema"])

    job_config = bigquery.LoadJobConfig(
        schema=schema, write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    logging.info(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def format_schema(schema_dict: dict) -> list:
    """
    Função para linkar as colunas do dict com as coluna no BQ

    Args:
        schema_dict (dict): Schema das colunas no BQ, com nome e tipo do dado

    Returns:
        list: Lista de colunas dos dados no BQ
    """
    return [bigquery.SchemaField(s["name"], s["type"]) for s in schema_dict]
