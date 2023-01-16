{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LND_TBL_STD_LU_GENDER_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'ID_APPROLE',
        'ID_SECUSER',
        'STD_ID_GENDER',
        'DT_LAST_UPDATE',
        'ID_ORGANIZATION',
        'STD_N_GENDERBRA',
        'STD_N_GENDERENG',
        'STD_N_GENDERESP',
        'STD_N_GENDERFRA',
        'STD_N_GENDERGEN',
        'STD_N_GENDERGER',
        'STD_N_GENDERITA',
    ]) }} as _airbyte_LND_TBL_STD_LU_GENDER_hashid,
    tmp.*
from {{ ref('LND_TBL_STD_LU_GENDER_ab2') }} tmp
-- LND_TBL_STD_LU_GENDER
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

