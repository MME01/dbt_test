{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CLI_REGIONES') }}
select
    {{ json_extract_scalar('_airbyte_data', ['ID_APPROLE'], ['ID_APPROLE']) }} as ID_APPROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_SECUSER'], ['ID_SECUSER']) }} as ID_SECUSER,
    {{ json_extract_scalar('_airbyte_data', ['CLI_ID_REGION'], ['CLI_ID_REGION']) }} as CLI_ID_REGION,
    {{ json_extract_scalar('_airbyte_data', ['CLI_NM_REGION'], ['CLI_NM_REGION']) }} as CLI_NM_REGION,
    {{ json_extract_scalar('_airbyte_data', ['DT_LAST_UPDATE'], ['DT_LAST_UPDATE']) }} as DT_LAST_UPDATE,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CLI_REGIONES') }} as table_alias
-- LND_TBL_M4CLI_REGIONES
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

