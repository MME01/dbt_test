{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LND_TBL_STD_HR_PERIOD_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'STD_ID_HR',
        'ID_APPROLE',
        'ID_SECUSER',
        'SAR_CODIGO',
        'SME_SALARY',
        'STD_DT_END',
        'SFR_SEN_SUP',
        'SSP_PAGO_TA',
        'STD_COMMENT',
        'STD_DT_START',
        'SPO_EXT_TELEF',
        'DT_LAST_UPDATE',
        'SSP_FEC_EXTRAS',
        'STD_ID_HR_TYPE',
        'SUK_LAST_WK_DT',
        'CCL_BAJAENVIADA',
        'ID_ORGANIZATION',
        'SAR_CERTIF_ORIG',
        'STD_DT_SEN_PROF',
        'SUK_ACT_LEAV_DT',
        'SFR_DT_SENIORITY',
        'SME_ID_HR_STATUS',
        'SPO_COMENT_SAIDA',
        'STD_KEY_EMPLOYEE',
        'STD_OR_HR_PERIOD',
        'SUK_PROB_RESULTS',
        'SME_OR_HR_PER_ANT',
        'SPO_ID_EST_QUADRO',
        'SSP_NUM_MATRICULA',
        'SSP_NUM_PLURIEMPL',
        'STD_ID_EXTERN_ORG',
        'STD_STRATEGIC_EMP',
        'SUK_PROB_END_DATE',
        'SUK_TERM_RESIG_DT',
        'SAR_ID_SIT_REVISTA',
        'SFR_DT_SENIORITY_G',
        'SFR_DT_SENIORITY_L',
        'SME_ID_MOT_BAJA_IM',
        'SPO_ID_EST_SALARIO',
        'SSP_CHK_IRPF_MOVIL',
        'SSP_CHK_IRPF_PROLO',
        'SSP_FEC_ANTIGUEDAD',
        'STD_ID_HRP_END_REA',
        'STD_ID_HRP_SRT_REA',
        'SUK_CONT_SERV_DATE',
        'SUK_CONT_SERV_REAS',
        'SUK_EXT_PROB_END_D',
    ]) }} as _airbyte_LND_TBL_STD_HR_PERIOD_hashid,
    tmp.*
from {{ ref('LND_TBL_STD_HR_PERIOD_ab2') }} tmp
-- LND_TBL_STD_HR_PERIOD
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

