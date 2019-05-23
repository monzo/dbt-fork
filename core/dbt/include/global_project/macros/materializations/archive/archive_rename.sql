{% macro archive_migration_rename_columns(target_table) %}
    {{ adapter_macro('archive_migration_rename_columns', target_table) }}
{% endmacro %}

{% macro default__archive_migration_rename_columns(target_table) %}
    {% set oldnames = ['valid_from', 'valid_to', 'updated_at', 'scd_id'] %}
    {% for old in oldnames %}
        {% call statement('_') %}
            alter table {{ target_table }} rename column {{ adapter.quote(old) }} to {{ 'dbt_' ~ old }}
        {% endcall %}
    {% endfor %}
{% endmacro %}


{% macro bigquery__archive_migration_rename_columns(target_table) %}
    {% if target_table.endswith('`') %}
        {% set tmp_table = target_table.rstrip('`') ~ '_dbt_archive_migrate_backup' ~ '~' %}
    {% else %}
        {% set tmp_table = target_table ~ '_dbt_archive_migrate_backup' %}
    {% endif %}
    {% set sql %}
    select * EXCEPT({% for old in oldnames %}{{adapter.quote(old)}}{% if not loop.last %},{% endif %}{% endfor %}),
        {% for old in oldnames %}
            {{ adapter.quote(old) }} as {{ 'dbt_' ~ old }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ tmp_table }}
    {% endset %}
    {# if we had support for destination tables and stuff I think we could execute the select statement,
       set the destination to the target table itself, and tell bq to truncate the target if it exists
    #}
    {% call statement('_') %}
        {{ create_table_as(False, tmp_table, 'select * from ' ~ target_table)}}
    {% endcall %}

    {% call statement('_') %}
        {{ create_table_as(False, target_table, sql) }}
    {% endcall %}
{% endmacro %}
