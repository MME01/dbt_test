{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_M4SCO_HR_ROLE_ab1') }}
select
    cast(SCO_ID_HR as {{ dbt_utils.type_string() }}) as SCO_ID_HR,
    cast(ID_APPROLE as {{ dbt_utils.type_string() }}) as ID_APPROLE,
    cast(ID_SECUSER as {{ dbt_utils.type_string() }}) as ID_SECUSER,
    cast(SCO_DT_END as {{ dbt_utils.type_string() }}) as SCO_DT_END,
    cast(SCO_DT_START as {{ dbt_utils.type_string() }}) as SCO_DT_START,
    cast(CCL_PERC_PART as {{ dbt_utils.type_float() }}) as CCL_PERC_PART,
    cast(SCO_MAIN_ROLE as {{ dbt_utils.type_string() }}) as SCO_MAIN_ROLE,
    cast(SCO_N_ROLEBRA as {{ dbt_utils.type_string() }}) as SCO_N_ROLEBRA,
    cast(SCO_N_ROLEENG as {{ dbt_utils.type_string() }}) as SCO_N_ROLEENG,
    cast(SCO_N_ROLEESP as {{ dbt_utils.type_string() }}) as SCO_N_ROLEESP,
    cast(SCO_N_ROLEFRA as {{ dbt_utils.type_string() }}) as SCO_N_ROLEFRA,
    cast(SCO_N_ROLEGEN as {{ dbt_utils.type_string() }}) as SCO_N_ROLEGEN,
    cast(SCO_N_ROLEGER as {{ dbt_utils.type_string() }}) as SCO_N_ROLEGER,
    cast(SCO_N_ROLEITA as {{ dbt_utils.type_string() }}) as SCO_N_ROLEITA,
    cast(SCO_OR_HR_PER as {{ dbt_utils.type_float() }}) as SCO_OR_HR_PER,
    cast(DT_LAST_UPDATE as {{ dbt_utils.type_string() }}) as DT_LAST_UPDATE,
    cast(SCO_DT_LAST_WK as {{ dbt_utils.type_string() }}) as SCO_DT_LAST_WK,
    cast(SCO_OR_HR_ROLE as {{ dbt_utils.type_float() }}) as SCO_OR_HR_ROLE,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(SCO_ID_DURATION as {{ dbt_utils.type_string() }}) as SCO_ID_DURATION,
    cast(SUK_NMW_TRAINEE as {{ dbt_utils.type_float() }}) as SUK_NMW_TRAINEE,
    cast(SCO_ID_INT_RO_TY as {{ dbt_utils.type_string() }}) as SCO_ID_INT_RO_TY,
    cast(SCO_ID_REA_CHANG as {{ dbt_utils.type_string() }}) as SCO_ID_REA_CHANG,
    cast(SUK_DT_END_TRAINEE as {{ dbt_utils.type_string() }}) as SUK_DT_END_TRAINEE,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_M4SCO_HR_ROLE_ab1') }}
-- LND_TBL_M4SCO_HR_ROLE
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

