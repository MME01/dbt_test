{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_M4SCO_H_HR_POS_ab1') }}
select
    cast(SCO_ID_HR as {{ dbt_utils.type_string() }}) as SCO_ID_HR,
    cast(ID_APPROLE as {{ dbt_utils.type_string() }}) as ID_APPROLE,
    cast(ID_SECUSER as {{ dbt_utils.type_string() }}) as ID_SECUSER,
    cast(SCO_DT_END as {{ dbt_utils.type_string() }}) as SCO_DT_END,
    cast(SCO_DT_START as {{ dbt_utils.type_string() }}) as SCO_DT_START,
    cast(DT_LAST_UPDATE as {{ dbt_utils.type_string() }}) as DT_LAST_UPDATE,
    cast(SCO_OR_HR_ROLE as {{ dbt_utils.type_float() }}) as SCO_OR_HR_ROLE,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(SCO_ID_POSITION as {{ dbt_utils.type_string() }}) as SCO_ID_POSITION,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_M4SCO_H_HR_POS_ab1') }}
-- LND_TBL_M4SCO_H_HR_POS
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

