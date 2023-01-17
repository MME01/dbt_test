{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "lnd_ula_pnet_prd",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='LND_TBL_M4CCL_POSITION_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        ",'{{ 
        apply_default_policy_tag(this, "projects/df-central-command-prd/locations/us/taxonomies/3753529189283377636/policyTags/1570780607584273335")
    }}'],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('LND_TBL_M4CCL_POSITION_ab3') }}
select
    ID_TURNO,
    M4CCL_CADENA,
    M4CCL_FUTURO,
    CCL_CHK_HABIL,
    CCL_DATE_HIRE,
    CCL_ID_HORARIO,
    M4CCL_PROYECTO,
    STD_ID_LEG_ENT,
    ID_ORGANIZATION,
    SCO_ID_POSITION,
    CCL_EMP_OTRO_PAIS,
    CCL_CHK_PUESTO_ADM,
    CCL_ID_OTR_LEG_ENT,
    M4CCL_ID_FLEXFIELD,
    M4CCL_INTERCOMPANY,
    STD_ID_JOB_INT_CLA,
    STD_ID_JOB_INT_FAM,
    STD_ID_JOB_CATEGORY,
    CCL_JEFE_CENTRO_ANALISIS,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_LND_TBL_M4CCL_POSITION_hashid
from {{ ref('LND_TBL_M4CCL_POSITION_ab3') }}
-- LND_TBL_M4CCL_POSITION from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CCL_POSITION') }}
where 1 = 1

