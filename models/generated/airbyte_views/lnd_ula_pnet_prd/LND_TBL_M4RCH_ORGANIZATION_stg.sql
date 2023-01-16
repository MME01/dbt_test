{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LND_TBL_M4RCH_ORGANIZATION_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'ID_LEVEL',
        'ID_APPROLE',
        'ID_SECUSER',
        'CLI_ID_REGION',
        'DT_LAST_UPDATE',
        'ID_ORGANIZATION',
        'NM_ORGANIZATIONBRA',
        'NM_ORGANIZATIONENG',
        'NM_ORGANIZATIONESP',
        'NM_ORGANIZATIONFRA',
        'NM_ORGANIZATIONGEN',
        'NM_ORGANIZATIONGER',
        'NM_ORGANIZATIONITA',
    ]) }} as _airbyte_LND_TBL_M4RCH_ORGANIZATION_hashid,
    tmp.*
from {{ ref('LND_TBL_M4RCH_ORGANIZATION_ab2') }} tmp
-- LND_TBL_M4RCH_ORGANIZATION
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

