{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LND_TBL_M4CCL_HR_PERIOD_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'STD_ID_HR',
        'CCL_ID_GAFETE',
        'CCL_ETAPA_EVAL',
        'CCL_PENSIONADO',
        'ID_ORGANIZATION',
        'CCL_FECHA_SALIDA',
        'STD_OR_HR_PERIOD',
        'M4CCL_COD_JORNADA',
        'CCL_CONTROL_ASISTE',
        'CCL_FECHA_CREACION',
        'CCL_FECHA_PREAVISO',
        'CCL_ID_RELOJ_MARCA',
    ]) }} as _airbyte_LND_TBL_M4CCL_HR_PERIOD_hashid,
    tmp.*
from {{ ref('LND_TBL_M4CCL_HR_PERIOD_ab2') }} tmp
-- LND_TBL_M4CCL_HR_PERIOD
where 1 = 1

