{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_M4CCL_POSITION_ab1') }}
select
    cast(ID_TURNO as {{ dbt_utils.type_string() }}) as ID_TURNO,
    cast(M4CCL_CADENA as {{ dbt_utils.type_float() }}) as M4CCL_CADENA,
    cast(M4CCL_FUTURO as {{ dbt_utils.type_string() }}) as M4CCL_FUTURO,
    cast(CCL_CHK_HABIL as {{ dbt_utils.type_string() }}) as CCL_CHK_HABIL,
    cast(CCL_DATE_HIRE as {{ dbt_utils.type_string() }}) as CCL_DATE_HIRE,
    cast(CCL_ID_HORARIO as {{ dbt_utils.type_float() }}) as CCL_ID_HORARIO,
    cast(M4CCL_PROYECTO as {{ dbt_utils.type_string() }}) as M4CCL_PROYECTO,
    cast(STD_ID_LEG_ENT as {{ dbt_utils.type_string() }}) as STD_ID_LEG_ENT,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(SCO_ID_POSITION as {{ dbt_utils.type_string() }}) as SCO_ID_POSITION,
    cast(CCL_EMP_OTRO_PAIS as {{ dbt_utils.type_string() }}) as CCL_EMP_OTRO_PAIS,
    cast(CCL_CHK_PUESTO_ADM as {{ dbt_utils.type_string() }}) as CCL_CHK_PUESTO_ADM,
    cast(CCL_ID_OTR_LEG_ENT as {{ dbt_utils.type_string() }}) as CCL_ID_OTR_LEG_ENT,
    cast(M4CCL_ID_FLEXFIELD as {{ dbt_utils.type_string() }}) as M4CCL_ID_FLEXFIELD,
    cast(M4CCL_INTERCOMPANY as {{ dbt_utils.type_string() }}) as M4CCL_INTERCOMPANY,
    cast(STD_ID_JOB_INT_CLA as {{ dbt_utils.type_float() }}) as STD_ID_JOB_INT_CLA,
    cast(STD_ID_JOB_INT_FAM as {{ dbt_utils.type_float() }}) as STD_ID_JOB_INT_FAM,
    cast(STD_ID_JOB_CATEGORY as {{ dbt_utils.type_float() }}) as STD_ID_JOB_CATEGORY,
    cast(CCL_JEFE_CENTRO_ANALISIS as {{ dbt_utils.type_string() }}) as CCL_JEFE_CENTRO_ANALISIS,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_M4CCL_POSITION_ab1') }}
-- LND_TBL_M4CCL_POSITION
where 1 = 1

