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
                            identifier='LND_TBL_M4CCL_HR_PERIOD_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        ", '{{ 
        apply_default_policy_tag(this, "projects/df-central-command-prd/locations/us/taxonomies/3753529189283377636/policyTags/1570780607584273335")
    }}'],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('LND_TBL_M4CCL_HR_PERIOD_ab3') }}
select
    STD_ID_HR,
    CCL_ID_GAFETE,
    CCL_ETAPA_EVAL,
    CCL_PENSIONADO,
    ID_ORGANIZATION,
    CCL_FECHA_SALIDA,
    STD_OR_HR_PERIOD,
    M4CCL_COD_JORNADA,
    CCL_CONTROL_ASISTE,
    CCL_FECHA_CREACION,
    CCL_FECHA_PREAVISO,
    CCL_ID_RELOJ_MARCA,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_LND_TBL_M4CCL_HR_PERIOD_hashid
from {{ ref('LND_TBL_M4CCL_HR_PERIOD_ab3') }}
-- LND_TBL_M4CCL_HR_PERIOD from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_M4CCL_HR_PERIOD') }}
where 1 = 1

