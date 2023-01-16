{{ config(
    cluster_by = ["_airbyte_unique_key","_airbyte_emitted_at"],
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = "_airbyte_unique_key",
    schema = "lnd_ula_pnet_prd",
    tags = [ "top-level" ],
    post_hook='{{ 
        apply_default_policy_tag(this, "projects/df-central-command-prd/locations/us/taxonomies/3753529189283377636/policyTags/1570780607584273335")
    }}'
) }}
-- Final base SQL model
-- depends_on: {{ ref('LND_TBL_M4SCO_H_HR_POS_scd') }}
select
    _airbyte_unique_key,
    SCO_ID_HR,
    ID_APPROLE,
    ID_SECUSER,
    SCO_DT_END,
    SCO_DT_START,
    DT_LAST_UPDATE,
    SCO_OR_HR_ROLE,
    ID_ORGANIZATION,
    SCO_ID_POSITION,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_LND_TBL_M4SCO_H_HR_POS_hashid
from {{ ref('LND_TBL_M4SCO_H_HR_POS_scd') }}
-- LND_TBL_M4SCO_H_HR_POS from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4SCO_H_HR_POS') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

