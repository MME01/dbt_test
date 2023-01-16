{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_M4CLI_REGIONES_ab1') }}
select
    cast(ID_APPROLE as {{ dbt_utils.type_string() }}) as ID_APPROLE,
    cast(ID_SECUSER as {{ dbt_utils.type_string() }}) as ID_SECUSER,
    cast(CLI_ID_REGION as {{ dbt_utils.type_float() }}) as CLI_ID_REGION,
    cast(CLI_NM_REGION as {{ dbt_utils.type_string() }}) as CLI_NM_REGION,
    cast(DT_LAST_UPDATE as {{ dbt_utils.type_string() }}) as DT_LAST_UPDATE,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_M4CLI_REGIONES_ab1') }}
-- LND_TBL_M4CLI_REGIONES
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

