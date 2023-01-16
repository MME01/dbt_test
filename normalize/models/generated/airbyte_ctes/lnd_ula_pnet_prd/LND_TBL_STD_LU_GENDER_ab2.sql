{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_STD_LU_GENDER_ab1') }}
select
    cast(ID_APPROLE as {{ dbt_utils.type_string() }}) as ID_APPROLE,
    cast(ID_SECUSER as {{ dbt_utils.type_string() }}) as ID_SECUSER,
    cast(STD_ID_GENDER as {{ dbt_utils.type_string() }}) as STD_ID_GENDER,
    cast(DT_LAST_UPDATE as {{ dbt_utils.type_string() }}) as DT_LAST_UPDATE,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(STD_N_GENDERBRA as {{ dbt_utils.type_string() }}) as STD_N_GENDERBRA,
    cast(STD_N_GENDERENG as {{ dbt_utils.type_string() }}) as STD_N_GENDERENG,
    cast(STD_N_GENDERESP as {{ dbt_utils.type_string() }}) as STD_N_GENDERESP,
    cast(STD_N_GENDERFRA as {{ dbt_utils.type_string() }}) as STD_N_GENDERFRA,
    cast(STD_N_GENDERGEN as {{ dbt_utils.type_string() }}) as STD_N_GENDERGEN,
    cast(STD_N_GENDERGER as {{ dbt_utils.type_string() }}) as STD_N_GENDERGER,
    cast(STD_N_GENDERITA as {{ dbt_utils.type_string() }}) as STD_N_GENDERITA,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_STD_LU_GENDER_ab1') }}
-- LND_TBL_STD_LU_GENDER
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

