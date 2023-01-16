{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4RCH_ORGANIZATION') }}
select
    {{ json_extract_scalar('_airbyte_data', ['ID_LEVEL'], ['ID_LEVEL']) }} as ID_LEVEL,
    {{ json_extract_scalar('_airbyte_data', ['ID_APPROLE'], ['ID_APPROLE']) }} as ID_APPROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_SECUSER'], ['ID_SECUSER']) }} as ID_SECUSER,
    {{ json_extract_scalar('_airbyte_data', ['CLI_ID_REGION'], ['CLI_ID_REGION']) }} as CLI_ID_REGION,
    {{ json_extract_scalar('_airbyte_data', ['DT_LAST_UPDATE'], ['DT_LAST_UPDATE']) }} as DT_LAST_UPDATE,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    {{ json_extract_scalar('_airbyte_data', ['NM_ORGANIZATIONBRA'], ['NM_ORGANIZATIONBRA']) }} as NM_ORGANIZATIONBRA,
    {{ json_extract_scalar('_airbyte_data', ['NM_ORGANIZATIONENG'], ['NM_ORGANIZATIONENG']) }} as NM_ORGANIZATIONENG,
    {{ json_extract_scalar('_airbyte_data', ['NM_ORGANIZATIONESP'], ['NM_ORGANIZATIONESP']) }} as NM_ORGANIZATIONESP,
    {{ json_extract_scalar('_airbyte_data', ['NM_ORGANIZATIONFRA'], ['NM_ORGANIZATIONFRA']) }} as NM_ORGANIZATIONFRA,
    {{ json_extract_scalar('_airbyte_data', ['NM_ORGANIZATIONGEN'], ['NM_ORGANIZATIONGEN']) }} as NM_ORGANIZATIONGEN,
    {{ json_extract_scalar('_airbyte_data', ['NM_ORGANIZATIONGER'], ['NM_ORGANIZATIONGER']) }} as NM_ORGANIZATIONGER,
    {{ json_extract_scalar('_airbyte_data', ['NM_ORGANIZATIONITA'], ['NM_ORGANIZATIONITA']) }} as NM_ORGANIZATIONITA,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4RCH_ORGANIZATION') }} as table_alias
-- LND_TBL_M4RCH_ORGANIZATION
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

