{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_M4RCH_ORGANIZATION_ab1') }}
select
    cast(ID_LEVEL as {{ dbt_utils.type_float() }}) as ID_LEVEL,
    cast(ID_APPROLE as {{ dbt_utils.type_string() }}) as ID_APPROLE,
    cast(ID_SECUSER as {{ dbt_utils.type_string() }}) as ID_SECUSER,
    cast(CLI_ID_REGION as {{ dbt_utils.type_float() }}) as CLI_ID_REGION,
    cast(DT_LAST_UPDATE as {{ dbt_utils.type_string() }}) as DT_LAST_UPDATE,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(NM_ORGANIZATIONBRA as {{ dbt_utils.type_string() }}) as NM_ORGANIZATIONBRA,
    cast(NM_ORGANIZATIONENG as {{ dbt_utils.type_string() }}) as NM_ORGANIZATIONENG,
    cast(NM_ORGANIZATIONESP as {{ dbt_utils.type_string() }}) as NM_ORGANIZATIONESP,
    cast(NM_ORGANIZATIONFRA as {{ dbt_utils.type_string() }}) as NM_ORGANIZATIONFRA,
    cast(NM_ORGANIZATIONGEN as {{ dbt_utils.type_string() }}) as NM_ORGANIZATIONGEN,
    cast(NM_ORGANIZATIONGER as {{ dbt_utils.type_string() }}) as NM_ORGANIZATIONGER,
    cast(NM_ORGANIZATIONITA as {{ dbt_utils.type_string() }}) as NM_ORGANIZATIONITA,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_M4RCH_ORGANIZATION_ab1') }}
-- LND_TBL_M4RCH_ORGANIZATION
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

