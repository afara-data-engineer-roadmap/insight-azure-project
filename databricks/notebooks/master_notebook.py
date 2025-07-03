# ======================================================================================

# NOTEBOOK ORCHESTRATEUR

# Version finale utilisant des chemins absolus pour garantir la fiabilité de l'exécution.

# ======================================================================================

import logging

import json



# ======================================================================================

# 1. DÉCLARATION DES PARAMÈTRES (WIDGETS)

# ======================================================================================

logging.info("DÉMARRAGE DU PIPELINE ORCHESTRATEUR)")



dbutils.widgets.text("child_notebook_path", "", "Chemin du notebook à exécuter")

dbutils.widgets.text("storage_account", "stsalesinsightcuxm0611", "Nom du compte de stockage")

dbutils.widgets.text("container", "data", "Nom du conteneur")

dbutils.widgets.text("silver_folder", "silver/sales_orders/", "Dossier source dans la couche Silver")

dbutils.widgets.text("secret_scope", "dbricks-scope-projet", "Scope unique pour les secrets du projet")

dbutils.widgets.text("adls_secret_key", "adls-access-key", "Clé du secret pour l'accès ADLS")

dbutils.widgets.text("sql_user_key", "sql-admin-user", "Clé du secret pour l'utilisateur SQL")

dbutils.widgets.text("sql_password_key", "sql-admin-password", "Clé du secret pour le mot de passe SQL")

dbutils.widgets.text("jdbc_hostname", "sqlsvr-salesinsightcuxm0611.database.windows.net", "Serveur Azure SQL DB")

dbutils.widgets.text("jdbc_database", "sqldb-salesinsight-gold", "Base de données Gold")

logging.info("Widgets déclarés.")



# ======================================================================================

# 2. POINT D'ENTRÉE PRINCIPAL (MAIN)

# ======================================================================================

if __name__ == "__main__":
    logging.info("Entrée dans le bloc __main__.")

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



    child_notebook = dbutils.widgets.get("child_notebook_path").strip()


    params_to_pass = {

    "storage_account": dbutils.widgets.get("storage_account"),

    "container": dbutils.widgets.get("container"),

    "silver_folder": dbutils.widgets.get("silver_folder"),

"secret_scope": dbutils.widgets.get("secret_scope"),

"adls_secret_key": dbutils.widgets.get("adls_secret_key"),

"sql_user_key": dbutils.widgets.get("sql_user_key"),

"sql_password_key": dbutils.widgets.get("sql_password_key"),
"jdbc_hostname": dbutils.widgets.get("jdbc_hostname"),

    "jdbc_database": dbutils.widgets.get("jdbc_database"),

}



if child_notebook:

# MODE PRODUCTION (appelé par ADF)

    logging.info(f"Mode Production (ADF) : Exécution du notebook enfant : {child_notebook}")

    try:

        dbutils.notebook.run(path=child_notebook, timeout_seconds=3600, arguments=params_to_pass)

        logging.info(f"Le notebook enfant {child_notebook} s'est terminé avec succès.")

    except Exception as e:

        logging.error(f"❌ L'exécution du notebook enfant {child_notebook} a échoué.", exc_info=True)

        raise e

else:

# MODE TEST (exécuté manuellement)

    logging.warning("Mode Test d'Intégration : Exécution de la séquence complète des dimensions.")


# --- CORRECTION : Utilisation des chemins absolus du Repo ---

# C'est la méthode la plus robuste pour éviter les problèmes de contexte d'exécution.

# NOTE : Le chemin /Workspace/ est la racine pour les appels dbutils.notebook.run
# 
    repo_base_path = "/Workspace/Repos/fara.niang@lightberg.com/insight-azure-project"



    dimension_notebooks = [

          f"{repo_base_path}/databricks/notebooks/silver_to_gold_dim_product",

 ]

for notebook_path in dimension_notebooks:

    logging.info(f"===================================================")

    logging.info(f"Début de l'exécution du notebook de test : {notebook_path}")


    try:

        dbutils.notebook.run(

                            path=notebook_path,

                            timeout_seconds=3600,
                            arguments=params_to_pass

                        )
        logging.info(f"✅ Le notebook {notebook_path} s'est terminé avec succès.")

    except Exception as e:

        error_message = str(e)

        logging.error(f"❌ L'exécution du notebook {notebook_path} a ÉCHOUÉ.")

        logging.error(f"MESSAGE D'ERREUR BRUT : {error_message}")

        dbutils.notebook.exit(f"Échec sur le notebook {notebook_path}. Erreur : {error_message}")

        raise e



    logging.info("===================================================")

    logging.info("✅ Test d'intégration complet terminé avec succès.")

    logging.info("===================================================")ableau adf