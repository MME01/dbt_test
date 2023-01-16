{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CCL_POSITION') }}
select
    {{ json_extract_scalar('_airbyte_data', ['ID_TURNO'], ['ID_TURNO']) }} as ID_TURNO,
    {{ json_extract_scalar('_airbyte_data', ['M4CCL_CADENA'], ['M4CCL_CADENA']) }} as M4CCL_CADENA,
    {{ json_extract_scalar('_airbyte_data', ['M4CCL_FUTURO'], ['M4CCL_FUTURO']) }} as M4CCL_FUTURO,
    {{ json_extract_scalar('_airbyte_data', ['CCL_CHK_HABIL'], ['CCL_CHK_HABIL']) }} as CCL_CHK_HABIL,
    {{ json_extract_scalar('_airbyte_data', ['CCL_DATE_HIRE'], ['CCL_DATE_HIRE']) }} as CCL_DATE_HIRE,
    {{ json_extract_scalar('_airbyte_data', ['CCL_ID_HORARIO'], ['CCL_ID_HORARIO']) }} as CCL_ID_HORARIO,
    {{ json_extract_scalar('_airbyte_data', ['M4CCL_PROYECTO'], ['M4CCL_PROYECTO']) }} as M4CCL_PROYECTO,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_LEG_ENT'], ['STD_ID_LEG_ENT']) }} as STD_ID_LEG_ENT,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    {{ json_extract_scalar('_airbyte_data', ['SCO_ID_POSITION'], ['SCO_ID_POSITION']) }} as SCO_ID_POSITION,
    {{ json_extract_scalar('_airbyte_data', ['CCL_EMP_OTRO_PAIS'], ['CCL_EMP_OTRO_PAIS']) }} as CCL_EMP_OTRO_PAIS,
    {{ json_extract_scalar('_airbyte_data', ['CCL_CHK_PUESTO_ADM'], ['CCL_CHK_PUESTO_ADM']) }} as CCL_CHK_PUESTO_ADM,
    {{ json_extract_scalar('_airbyte_data', ['CCL_ID_OTR_LEG_ENT'], ['CCL_ID_OTR_LEG_ENT']) }} as CCL_ID_OTR_LEG_ENT,
    {{ json_extract_scalar('_airbyte_data', ['M4CCL_ID_FLEXFIELD'], ['M4CCL_ID_FLEXFIELD']) }} as M4CCL_ID_FLEXFIELD,
    {{ json_extract_scalar('_airbyte_data', ['M4CCL_INTERCOMPANY'], ['M4CCL_INTERCOMPANY']) }} as M4CCL_INTERCOMPANY,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_JOB_INT_CLA'], ['STD_ID_JOB_INT_CLA']) }} as STD_ID_JOB_INT_CLA,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_JOB_INT_FAM'], ['STD_ID_JOB_INT_FAM']) }} as STD_ID_JOB_INT_FAM,
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_JOB_CATEGORY'], ['STD_ID_JOB_CATEGORY']) }} as STD_ID_JOB_CATEGORY,
    {{ json_extract_scalar('_airbyte_data', ['CCL_JEFE_CENTRO_ANALISIS'], ['CCL_JEFE_CENTRO_ANALISIS']) }} as CCL_JEFE_CENTRO_ANALISIS,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CCL_POSITION') }} as table_alias
-- LND_TBL_M4CCL_POSITION
where 1 = 1

