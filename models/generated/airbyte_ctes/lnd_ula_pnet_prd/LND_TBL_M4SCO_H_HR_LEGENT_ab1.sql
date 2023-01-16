{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4SCO_H_HR_LEGENT') }}
select
    {{ json_extract_scalar('_airbyte_data', ['DT_END'], ['DT_END']) }} as DT_END,
    {{ json_extract_scalar('_airbyte_data', ['DT_START'], ['DT_START']) }} as DT_START,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_HR'], ['STD_ID_HR']) }} as STD_ID_HR,
    {{ json_extract_scalar('_airbyte_data', ['ID_APPROLE'], ['ID_APPROLE']) }} as ID_APPROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_SECUSER'], ['ID_SECUSER']) }} as ID_SECUSER,
    {{ json_extract_scalar('_airbyte_data', ['SCO_COMMENT'], ['SCO_COMMENT']) }} as SCO_COMMENT,
    {{ json_extract_scalar('_airbyte_data', ['DT_LAST_UPDATE'], ['DT_LAST_UPDATE']) }} as DT_LAST_UPDATE,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_LEG_ENT'], ['STD_ID_LEG_ENT']) }} as STD_ID_LEG_ENT,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_REA_CHANG'], ['SCO_ID_REA_CHANG']) }} as SCO_ID_REA_CHANG,
    {{ json_extract_scalar('_airbyte_data', ['STD_OR_HR_PERIOD'], ['STD_OR_HR_PERIOD']) }} as STD_OR_HR_PERIOD,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_LEG_ENT_TEMP'], ['STD_ID_LEG_ENT_TEMP']) }} as STD_ID_LEG_ENT_TEMP,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4SCO_H_HR_LEGENT') }} as table_alias
-- LND_TBL_M4SCO_H_HR_LEGENT
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

