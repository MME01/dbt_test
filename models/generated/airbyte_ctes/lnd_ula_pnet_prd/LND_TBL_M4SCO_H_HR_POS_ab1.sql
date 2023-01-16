{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4SCO_H_HR_POS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_HR'], ['SCO_ID_HR']) }} as SCO_ID_HR,
    {{ json_extract_scalar('_airbyte_data', ['ID_APPROLE'], ['ID_APPROLE']) }} as ID_APPROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_SECUSER'], ['ID_SECUSER']) }} as ID_SECUSER,
    {{ json_extract_scalar('_airbyte_data', ['SCO_DT_END'], ['SCO_DT_END']) }} as SCO_DT_END,
    {{ json_extract_scalar('_airbyte_data', ['SCO_DT_START'], ['SCO_DT_START']) }} as SCO_DT_START,
    {{ json_extract_scalar('_airbyte_data', ['DT_LAST_UPDATE'], ['DT_LAST_UPDATE']) }} as DT_LAST_UPDATE,
    {{ json_extract_scalar('_airbyte_data', ['SCO_OR_HR_ROLE'], ['SCO_OR_HR_ROLE']) }} as SCO_OR_HR_ROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_POSITION'], ['SCO_ID_POSITION']) }} as SCO_ID_POSITION,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4SCO_H_HR_POS') }} as table_alias
-- LND_TBL_M4SCO_H_HR_POS
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

