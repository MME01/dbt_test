{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_M4SCO_H_HR_LEGENT_ab1') }}
select
    cast(DT_END as {{ dbt_utils.type_string() }}) as DT_END,
    cast(DT_START as {{ dbt_utils.type_string() }}) as DT_START,
    cast(STD_ID_HR as {{ dbt_utils.type_string() }}) as STD_ID_HR,
    cast(ID_APPROLE as {{ dbt_utils.type_string() }}) as ID_APPROLE,
    cast(ID_SECUSER as {{ dbt_utils.type_string() }}) as ID_SECUSER,
    cast(SCO_COMMENT as {{ dbt_utils.type_string() }}) as SCO_COMMENT,
    cast(DT_LAST_UPDATE as {{ dbt_utils.type_string() }}) as DT_LAST_UPDATE,
    cast(STD_ID_LEG_ENT as {{ dbt_utils.type_string() }}) as STD_ID_LEG_ENT,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(SCO_ID_REA_CHANG as {{ dbt_utils.type_string() }}) as SCO_ID_REA_CHANG,
    cast(STD_OR_HR_PERIOD as {{ dbt_utils.type_float() }}) as STD_OR_HR_PERIOD,
    cast(STD_ID_LEG_ENT_TEMP as {{ dbt_utils.type_string() }}) as STD_ID_LEG_ENT_TEMP,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_M4SCO_H_HR_LEGENT_ab1') }}
-- LND_TBL_M4SCO_H_HR_LEGENT
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

