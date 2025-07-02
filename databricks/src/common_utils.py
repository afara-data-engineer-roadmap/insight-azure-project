# ======================================================================================
# BIBLIOTHÈQUE DE FONCTIONS PARTAGÉES
# Fichier: databricks/src/common_utils.py
# ======================================================================================

import logging
from pyspark.sql import DataFrame, SparkSession

def setup_adls_access(spark: SparkSession, dbutils, storage_account: str, scope: str, key_name: str) -> None:
    """
    Configure l'accès au compte de stockage ADLS Gen2 pour la session Spark en cours.
    """
    logging.info(f"Configuration de l'accès pour le compte de stockage : {storage_account}")
    access_key = dbutils.secrets.get(scope=scope, key=key_name)
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        access_key
    )
    logging.info("Accès ADLS configuré avec succès pour cette session.")

def get_jdbc_connection_properties(dbutils, hostname: str, db_name: str, scope: str, user_key: str, pwd_key: str) -> tuple[str, dict]:
    """
    Construit l'URL JDBC et le dictionnaire de propriétés pour la connexion à Azure SQL DB.
    """
    logging.info("Configuration de la connexion JDBC.")
    jdbc_port = 1433
    sql_user = dbutils.secrets.get(scope=scope, key=user_key)
    sql_password = dbutils.secrets.get(scope=scope, key=pwd_key)
    jdbc_url = f"jdbc:sqlserver://{hostname}:{jdbc_port};database={db_name}"
    properties = {
      "user": sql_user,
      "password": sql_password,
      "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return jdbc_url, properties

def read_silver_data(spark: SparkSession, source_path: str) -> DataFrame:
    """
    Lit la table Delta depuis la couche Silver.
    """
    logging.info(f"Lecture des données Silver depuis : {source_path}")
    try:
        df = spark.read.format("delta").load(source_path)
        logging.info(f"Lecture réussie depuis la couche Silver.")
        return df
    except Exception as e:
        logging.error(f"❌ ERREUR lors de la lecture des données depuis {source_path}", exc_info=True)
        raise e

def write_dimension_to_gold(dim_df: DataFrame, table_name: str, jdbc_url: str, properties: dict) -> None:
    """
    Écrit un DataFrame de dimension dans la couche Gold (Azure SQL DB).
    """
    logging.info(f"Début de l'écriture de la dimension '{table_name}' vers la couche Gold.")
    try:
        dim_df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=properties
        )
        logging.info(f"✅ La dimension '{table_name}' a été écrite avec succès dans la couche Gold.")
    except Exception as e:
        logging.error(f"❌ ERREUR lors de l'écriture de la dimension '{table_name}'.", exc_info=True)
        raise e
