{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LND_TBL_M4SCO_H_HR_POS_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'SCO_ID_HR',
        'ID_APPROLE',
        'ID_SECUSER',
        'SCO_DT_END',
        'SCO_DT_START',
        'DT_LAST_UPDATE',
        'SCO_OR_HR_ROLE',
        'ID_ORGANIZATION',
        'SCO_ID_POSITION',
    ]) }} as _airbyte_LND_TBL_M4SCO_H_HR_POS_hashid,
    tmp.*
from {{ ref('LND_TBL_M4SCO_H_HR_POS_ab2') }} tmp
-- LND_TBL_M4SCO_H_HR_POS
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

