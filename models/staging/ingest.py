def model(dbt, session):
    dbt.config(materialized="table")
    
    # Lire tous les fichiers Parquet depuis un dossier
    df = session.execute(r"""SELECT * FROM read_parquet('C:\Users\eupho\OneDrive\Documents\perso\projets\web_crawlers_public\data_input\logs_train_test\*.parquet')""").fetchdf()
    
    # Retourner le DataFrame final pour dbt
    return df
