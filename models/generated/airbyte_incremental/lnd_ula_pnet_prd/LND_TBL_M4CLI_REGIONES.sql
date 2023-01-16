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
-- depends_on: {{ ref('LND_TBL_M4CLI_REGIONES_scd') }}
select
    _airbyte_unique_key,
    ID_APPROLE,
    ID_SECUSER,
    CLI_ID_REGION,
    CLI_NM_REGION,
    DT_LAST_UPDATE,
    ID_ORGANIZATION,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_LND_TBL_M4CLI_REGIONES_hashid
from {{ ref('LND_TBL_M4CLI_REGIONES_scd') }}
-- LND_TBL_M4CLI_REGIONES from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CLI_REGIONES') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

