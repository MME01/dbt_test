{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CCL_HR_PERIOD') }}
select
    {{ json_extract_scalar('_airbyte_data', ['STD_ID_HR'], ['STD_ID_HR']) }} as STD_ID_HR,
    {{ json_extract_scalar('_airbyte_data', ['CCL_ID_GAFETE'], ['CCL_ID_GAFETE']) }} as CCL_ID_GAFETE,
    {{ json_extract_scalar('_airbyte_data', ['CCL_ETAPA_EVAL'], ['CCL_ETAPA_EVAL']) }} as CCL_ETAPA_EVAL,
    {{ json_extract_scalar('_airbyte_data', ['CCL_PENSIONADO'], ['CCL_PENSIONADO']) }} as CCL_PENSIONADO,
    {{ json_extract_scalar('_airbyte_data', ['ID_ORGANIZATION'], ['ID_ORGANIZATION']) }} as ID_ORGANIZATION,
    {{ json_extract_scalar('_airbyte_data', ['CCL_FECHA_SALIDA'], ['CCL_FECHA_SALIDA']) }} as CCL_FECHA_SALIDA,
    {{ json_extract_scalar('_airbyte_data', ['STD_OR_HR_PERIOD'], ['STD_OR_HR_PERIOD']) }} as STD_OR_HR_PERIOD,
    {{ json_extract_scalar('_airbyte_data', ['M4CCL_COD_JORNADA'], ['M4CCL_COD_JORNADA']) }} as M4CCL_COD_JORNADA,
    {{ json_extract_scalar('_airbyte_data', ['CCL_CONTROL_ASISTE'], ['CCL_CONTROL_ASISTE']) }} as CCL_CONTROL_ASISTE,
    {{ json_extract_scalar('_airbyte_data', ['CCL_FECHA_CREACION'], ['CCL_FECHA_CREACION']) }} as CCL_FECHA_CREACION,
    {{ json_extract_scalar('_airbyte_data', ['CCL_FECHA_PREAVISO'], ['CCL_FECHA_PREAVISO']) }} as CCL_FECHA_PREAVISO,
    {{ json_extract_scalar('_airbyte_data', ['CCL_ID_RELOJ_MARCA'], ['CCL_ID_RELOJ_MARCA']) }} as CCL_ID_RELOJ_MARCA,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CCL_HR_PERIOD') }} as table_alias
-- LND_TBL_M4CCL_HR_PERIOD
where 1 = 1

