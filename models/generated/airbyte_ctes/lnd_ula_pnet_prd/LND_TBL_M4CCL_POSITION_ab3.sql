{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LND_TBL_M4CCL_POSITION_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'ID_TURNO',
        'M4CCL_CADENA',
        'M4CCL_FUTURO',
        'CCL_CHK_HABIL',
        'CCL_DATE_HIRE',
        'CCL_ID_HORARIO',
        'M4CCL_PROYECTO',
        'STD_ID_LEG_ENT',
        'ID_ORGANIZATION',
        'SCO_ID_POSITION',
        'CCL_EMP_OTRO_PAIS',
        'CCL_CHK_PUESTO_ADM',
        'CCL_ID_OTR_LEG_ENT',
        'M4CCL_ID_FLEXFIELD',
        'M4CCL_INTERCOMPANY',
        'STD_ID_JOB_INT_CLA',
        'STD_ID_JOB_INT_FAM',
        'STD_ID_JOB_CATEGORY',
        'CCL_JEFE_CENTRO_ANALISIS',
    ]) }} as _airbyte_LND_TBL_M4CCL_POSITION_hashid,
    tmp.*
from {{ ref('LND_TBL_M4CCL_POSITION_ab2') }} tmp
-- LND_TBL_M4CCL_POSITION
where 1 = 1

