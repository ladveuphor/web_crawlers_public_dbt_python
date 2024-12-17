{% macro clean_models(schema_name='NRJBI_SI') %}
    {% set tables_to_drop = [] %}

    -- Requête pour obtenir la liste des tables dans le schéma spécifié
    {% set query %}
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{{ schema_name }}' AND TABLE_TYPE = 'BASE TABLE'
    {% endset %}

    {% set results = run_query(query) %}

    {% if results and results.rows %}
        {% for row in results %}
            {% set table_name = row[0] %}
            {% do tables_to_drop.append(table_name) %}
        {% endfor %}
    {% else %}
        {{ log("Aucune table trouvée dans le schéma " ~ schema_name, info=True) }}
    {% endif %}

    -- Suppression des tables listées
    {% for table in tables_to_drop %}
        {{ log("Suppression de la table : " ~ schema_name ~ "." ~ table, info=True) }}
        {{ run_query("DROP TABLE IF EXISTS " ~ schema_name ~ "." ~ table) }}
    {% endfor %}

    {{ log("Suppression des tables du schéma " ~ schema_name ~ " terminée.", info=True) }}
{% endmacro %}
