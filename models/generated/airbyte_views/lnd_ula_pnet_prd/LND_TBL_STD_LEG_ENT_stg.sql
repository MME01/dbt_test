{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LND_TBL_STD_LEG_ENT_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'ID_APPROLE',
        'ID_SECUSER',
        'SAR_ID_ART',
        'SSP_SUBCIF',
        'STD_DT_END',
        'STD_NID_ACT',
        'STD_NID_ENT',
        'SSP_ID_LEGAL',
        'STD_DT_START',
        'STD_N_LEG_ENT',
        'DT_LAST_UPDATE',
        'SFR_REG_LEGENT',
        'STD_ID_LEG_ENT',
        'CCB_SOC_SEC_NUM',
        'ID_ORGANIZATION',
        'SPO_ANO_LEG_ENT',
        'SPO_ID_NATUREZA',
        'STD_DESCRIPTION',
        'SME_FEC_PRES_DIM',
        'SME_FOLIO_OPERAC',
        'SPO_COD_CONCELHO',
        'SPO_DT_PUBLIC_BT',
        'SPO_REP_PERC_PUB',
        'SSP_COD_LOC_EMPR',
        'SPO_ID_ACTIVIDADE',
        'SPO_INST_REG_TRAB',
        'SPO_VOLUME_VENDAS',
        'SSP_CARGO_CERTBRA',
        'SSP_CARGO_CERTENG',
        'SSP_CARGO_CERTESP',
        'SSP_CARGO_CERTFRA',
        'SSP_CARGO_CERTGEN',
        'SSP_CARGO_CERTGER',
        'SSP_CARGO_CERTITA',
        'SSP_ID_TP_ALF_EMP',
        'SSP_ID_TP_ID_LGAL',
        'SSP_NUM_TRAB_EMPR',
        'SFR_ID_ACTIV_SECTO',
        'SFR_ID_LEGAL_HOLDI',
        'SFR_LEGAL_ENTITY_L',
        'SFR_REG_SUB_LEGENT',
        'SPO_CAPITAL_SOCIAL',
        'SPO_DT_ULT_TAB_SAL',
        'SPO_REPARTICAO_PER',
        'SPO_REP_PERC_ESTRA',
        'SSP_CARGO_CERT_HAB',
        'SSP_FIRMT_CERT_HAB',
        'SSP_ID_LEG_CER_HAB',
        'SSP_ID_PAIS_EMISOR',
        'SSP_ID_RESPONSABLE',
        'SSP_LIT_PLURIEMPLE',
    ]) }} as _airbyte_LND_TBL_STD_LEG_ENT_hashid,
    tmp.*
from {{ ref('LND_TBL_STD_LEG_ENT_ab2') }} tmp
-- LND_TBL_STD_LEG_ENT
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

