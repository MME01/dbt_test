{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_M4CCL_HR_PERIOD_ab1') }}
select
    cast(STD_ID_HR as {{ dbt_utils.type_string() }}) as STD_ID_HR,
    cast(CCL_ID_GAFETE as {{ dbt_utils.type_string() }}) as CCL_ID_GAFETE,
    cast(CCL_ETAPA_EVAL as {{ dbt_utils.type_string() }}) as CCL_ETAPA_EVAL,
    cast(CCL_PENSIONADO as {{ dbt_utils.type_string() }}) as CCL_PENSIONADO,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(CCL_FECHA_SALIDA as {{ dbt_utils.type_string() }}) as CCL_FECHA_SALIDA,
    cast(STD_OR_HR_PERIOD as {{ dbt_utils.type_float() }}) as STD_OR_HR_PERIOD,
    cast(M4CCL_COD_JORNADA as {{ dbt_utils.type_string() }}) as M4CCL_COD_JORNADA,
    cast(CCL_CONTROL_ASISTE as {{ dbt_utils.type_string() }}) as CCL_CONTROL_ASISTE,
    cast(CCL_FECHA_CREACION as {{ dbt_utils.type_string() }}) as CCL_FECHA_CREACION,
    cast(CCL_FECHA_PREAVISO as {{ dbt_utils.type_string() }}) as CCL_FECHA_PREAVISO,
    cast(CCL_ID_RELOJ_MARCA as {{ dbt_utils.type_string() }}) as CCL_ID_RELOJ_MARCA,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_M4CCL_HR_PERIOD_ab1') }}
-- LND_TBL_M4CCL_HR_PERIOD
where 1 = 1

