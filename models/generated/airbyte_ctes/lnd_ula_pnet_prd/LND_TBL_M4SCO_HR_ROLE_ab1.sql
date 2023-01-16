{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4SCO_HR_ROLE') }}
select
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_HR'], ['SCO_ID_HR']) }} as SCO_ID_HR,
    {{ json_extract_scalar('_airbyte_data', ['ID_APPROLE'], ['ID_APPROLE']) }} as ID_APPROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_SECUSER'], ['ID_SECUSER']) }} as ID_SECUSER,
    {{ json_extract_scalar('_airbyte_data', ['SCO_DT_END'], ['SCO_DT_END']) }} as SCO_DT_END,
    {{ json_extract_scalar('_airbyte_data', ['SCO_DT_START'], ['SCO_DT_START']) }} as SCO_DT_START,
    {{ json_extract_scalar('_airbyte_data', ['CCL_PERC_PART'], ['CCL_PERC_PART']) }} as CCL_PERC_PART,
    {{ json_extract_scalar('_airbyte_data', ['SCO_MAIN_ROLE'], ['SCO_MAIN_ROLE']) }} as SCO_MAIN_ROLE,
    {{ json_extract_scalar('_airbyte_data', ['SCO_N_ROLEBRA'], ['SCO_N_ROLEBRA']) }} as SCO_N_ROLEBRA,
    {{ json_extract_scalar('_airbyte_data', ['SCO_N_ROLEENG'], ['SCO_N_ROLEENG']) }} as SCO_N_ROLEENG,
    {{ json_extract_scalar('_airbyte_data', ['SCO_N_ROLEESP'], ['SCO_N_ROLEESP']) }} as SCO_N_ROLEESP,
    {{ json_extract_scalar('_airbyte_data', ['SCO_N_ROLEFRA'], ['SCO_N_ROLEFRA']) }} as SCO_N_ROLEFRA,
    {{ json_extract_scalar('_airbyte_data', ['SCO_N_ROLEGEN'], ['SCO_N_ROLEGEN']) }} as SCO_N_ROLEGEN,
    {{ json_extract_scalar('_airbyte_data', ['SCO_N_ROLEGER'], ['SCO_N_ROLEGER']) }} as SCO_N_ROLEGER,
    {{ json_extract_scalar('_airbyte_data', ['SCO_N_ROLEITA'], ['SCO_N_ROLEITA']) }} as SCO_N_ROLEITA,
    {{ json_extract_scalar('_airbyte_data', ['SCO_OR_HR_PER'], ['SCO_OR_HR_PER']) }} as SCO_OR_HR_PER,
    {{ json_extract_scalar('_airbyte_data', ['DT_LAST_UPDATE'], ['DT_LAST_UPDATE']) }} as DT_LAST_UPDATE,
    {{ json_extract_scalar('_airbyte_data', ['SCO_DT_LAST_WK'], ['SCO_DT_LAST_WK']) }} as SCO_DT_LAST_WK,
    {{ json_extract_scalar('_airbyte_data', ['SCO_OR_HR_ROLE'], ['SCO_OR_HR_ROLE']) }} as SCO_OR_HR_ROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_DURATION'], ['SCO_ID_DURATION']) }} as SCO_ID_DURATION,
    {{ json_extract_scalar('_airbyte_data', ['SUK_NMW_TRAINEE'], ['SUK_NMW_TRAINEE']) }} as SUK_NMW_TRAINEE,
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_INT_RO_TY'], ['SCO_ID_INT_RO_TY']) }} as SCO_ID_INT_RO_TY,
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_REA_CHANG'], ['SCO_ID_REA_CHANG']) }} as SCO_ID_REA_CHANG,
    {{ json_extract_scalar('_airbyte_data', ['SUK_DT_END_TRAINEE'], ['SUK_DT_END_TRAINEE']) }} as SUK_DT_END_TRAINEE,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4SCO_HR_ROLE') }} as table_alias
-- LND_TBL_M4SCO_HR_ROLE
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

