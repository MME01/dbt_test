{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_STD_LU_GENDER') }}
select
    {{ json_extract_scalar('_airbyte_data', ['ID_APPROLE'], ['ID_APPROLE']) }} as ID_APPROLE,
    {{ json_extract_scalar('_airbyte_data', ['ID_SECUSER'], ['ID_SECUSER']) }} as ID_SECUSER,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_GENDER'], ['STD_ID_GENDER']) }} as STD_ID_GENDER,
    {{ json_extract_scalar('_airbyte_data', ['DT_LAST_UPDATE'], ['DT_LAST_UPDATE']) }} as DT_LAST_UPDATE,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    {{ json_extract_scalar('_airbyte_data', ['STD_N_GENDERBRA'], ['STD_N_GENDERBRA']) }} as STD_N_GENDERBRA,
    {{ json_extract_scalar('_airbyte_data', ['STD_N_GENDERENG'], ['STD_N_GENDERENG']) }} as STD_N_GENDERENG,
    {{ json_extract_scalar('_airbyte_data', ['STD_N_GENDERESP'], ['STD_N_GENDERESP']) }} as STD_N_GENDERESP,
    {{ json_extract_scalar('_airbyte_data', ['STD_N_GENDERFRA'], ['STD_N_GENDERFRA']) }} as STD_N_GENDERFRA,
    {{ json_extract_scalar('_airbyte_data', ['STD_N_GENDERGEN'], ['STD_N_GENDERGEN']) }} as STD_N_GENDERGEN,
    {{ json_extract_scalar('_airbyte_data', ['STD_N_GENDERGER'], ['STD_N_GENDERGER']) }} as STD_N_GENDERGER,
    {{ json_extract_scalar('_airbyte_data', ['STD_N_GENDERITA'], ['STD_N_GENDERITA']) }} as STD_N_GENDERITA,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_STD_LU_GENDER') }} as table_alias
-- LND_TBL_STD_LU_GENDER
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

