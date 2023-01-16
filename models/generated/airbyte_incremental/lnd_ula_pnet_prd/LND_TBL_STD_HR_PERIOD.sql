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
-- depends_on: {{ ref('LND_TBL_STD_HR_PERIOD_scd') }}
select
    _airbyte_unique_key,
    STD_ID_HR,
    ID_APPROLE,
    ID_SECUSER,
    SAR_CODIGO,
    SME_SALARY,
    STD_DT_END,
    SFR_SEN_SUP,
    SSP_PAGO_TA,
    STD_COMMENT,
    STD_DT_START,
    SPO_EXT_TELEF,
    DT_LAST_UPDATE,
    SSP_FEC_EXTRAS,
    STD_ID_HR_TYPE,
    SUK_LAST_WK_DT,
    CCL_BAJAENVIADA,
    ID_ORGANIZATION,
    SAR_CERTIF_ORIG,
    STD_DT_SEN_PROF,
    SUK_ACT_LEAV_DT,
    SFR_DT_SENIORITY,
    SME_ID_HR_STATUS,
    SPO_COMENT_SAIDA,
    STD_KEY_EMPLOYEE,
    STD_OR_HR_PERIOD,
    SUK_PROB_RESULTS,
    SME_OR_HR_PER_ANT,
    SPO_ID_EST_QUADRO,
    SSP_NUM_MATRICULA,
    SSP_NUM_PLURIEMPL,
    STD_ID_EXTERN_ORG,
    STD_STRATEGIC_EMP,
    SUK_PROB_END_DATE,
    SUK_TERM_RESIG_DT,
    SAR_ID_SIT_REVISTA,
    SFR_DT_SENIORITY_G,
    SFR_DT_SENIORITY_L,
    SME_ID_MOT_BAJA_IM,
    SPO_ID_EST_SALARIO,
    SSP_CHK_IRPF_MOVIL,
    SSP_CHK_IRPF_PROLO,
    SSP_FEC_ANTIGUEDAD,
    STD_ID_HRP_END_REA,
    STD_ID_HRP_SRT_REA,
    SUK_CONT_SERV_DATE,
    SUK_CONT_SERV_REAS,
    SUK_EXT_PROB_END_D,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_LND_TBL_STD_HR_PERIOD_hashid
from {{ ref('LND_TBL_STD_HR_PERIOD_scd') }}
-- LND_TBL_STD_HR_PERIOD from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_STD_HR_PERIOD') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

