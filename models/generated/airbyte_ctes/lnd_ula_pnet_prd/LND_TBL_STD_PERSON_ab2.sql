{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_lnd_ula_pnet_prd",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('LND_TBL_STD_PERSON_ab1') }}
select
    cast(STD_SSN as {{ dbt_utils.type_string() }}) as STD_SSN,
    cast(SCO_PHOTO as {{ dbt_utils.type_string() }}) as SCO_PHOTO,
    cast(ID_APPROLE as {{ dbt_utils.type_string() }}) as ID_APPROLE,
    cast(ID_SECUSER as {{ dbt_utils.type_string() }}) as ID_SECUSER,
    cast(SCO_SMOKER as {{ dbt_utils.type_float() }}) as SCO_SMOKER,
    cast(SGE_ID_VSW as {{ dbt_utils.type_string() }}) as SGE_ID_VSW,
    cast(STD_SS_KEY as {{ dbt_utils.type_string() }}) as STD_SS_KEY,
    cast(SBR_ID_RACA as {{ dbt_utils.type_string() }}) as SBR_ID_RACA,
    cast(SCB_DIG_VER as {{ dbt_utils.type_string() }}) as SCB_DIG_VER,
    cast(SCO_GB_NAME as {{ dbt_utils.type_string() }}) as SCO_GB_NAME,
    cast(STD_COMMENT as {{ dbt_utils.type_string() }}) as STD_COMMENT,
    cast(SUS_VETERAN as {{ dbt_utils.type_float() }}) as SUS_VETERAN,
    cast(CCL_CK_BENEF as {{ dbt_utils.type_float() }}) as CCL_CK_BENEF,
    cast(SAR_EXPEDIDO as {{ dbt_utils.type_string() }}) as SAR_EXPEDIDO,
    cast(SGE_ID_TITEL as {{ dbt_utils.type_string() }}) as SGE_ID_TITEL,
    cast(STD_AFILIADO as {{ dbt_utils.type_float() }}) as STD_AFILIADO,
    cast(STD_DT_BIRTH as {{ dbt_utils.type_string() }}) as STD_DT_BIRTH,
    cast(CCL_LACTANCIA as {{ dbt_utils.type_float() }}) as CCL_LACTANCIA,
    cast(CCL_OCUPACION as {{ dbt_utils.type_string() }}) as CCL_OCUPACION,
    cast(SCO_HOME_PAGE as {{ dbt_utils.type_string() }}) as SCO_HOME_PAGE,
    cast(SCO_ID_REGION as {{ dbt_utils.type_string() }}) as SCO_ID_REGION,
    cast(SSP_ID_TP_DOC as {{ dbt_utils.type_string() }}) as SSP_ID_TP_DOC,
    cast(STD_ID_GENDER as {{ dbt_utils.type_string() }}) as STD_ID_GENDER,
    cast(STD_ID_PERSON as {{ dbt_utils.type_string() }}) as STD_ID_PERSON,
    cast(STD_SS_NUMBER as {{ dbt_utils.type_string() }}) as STD_SS_NUMBER,
    cast(CCL_ID_COUNTRY as {{ dbt_utils.type_string() }}) as CCL_ID_COUNTRY,
    cast(CCL_ID_GEO_DIV as {{ dbt_utils.type_string() }}) as CCL_ID_GEO_DIV,
    cast(DT_LAST_UPDATE as {{ dbt_utils.type_string() }}) as DT_LAST_UPDATE,
    cast(SCO_PHOTO_INET as {{ dbt_utils.type_string() }}) as SCO_PHOTO_INET,
    cast(STD_ID_COUNTRY as {{ dbt_utils.type_string() }}) as STD_ID_COUNTRY,
    cast(STD_ID_GEO_DIV as {{ dbt_utils.type_string() }}) as STD_ID_GEO_DIV,
    cast(SUS_DEATH_DATE as {{ dbt_utils.type_string() }}) as SUS_DEATH_DATE,
    cast(CCB_DRIVERS_NUM as {{ dbt_utils.type_string() }}) as CCB_DRIVERS_NUM,
    cast(ID_ORGANIZATION as {{ dbt_utils.type_string() }}) as ID_ORGANIZATION,
    cast(SGE_ID_ADD_NAME as {{ dbt_utils.type_string() }}) as SGE_ID_ADD_NAME,
    cast(SUK_MAIDEN_NAME as {{ dbt_utils.type_string() }}) as SUK_MAIDEN_NAME,
    cast(CCL_CONOCIDO_POR as {{ dbt_utils.type_string() }}) as CCL_CONOCIDO_POR,
    cast(DESCRIPCION_LOTE as {{ dbt_utils.type_string() }}) as DESCRIPCION_LOTE,
    cast(FECHA_APLIC_LOTE as {{ dbt_utils.type_string() }}) as FECHA_APLIC_LOTE,
    cast(SGE_N_NAME_COMPL as {{ dbt_utils.type_string() }}) as SGE_N_NAME_COMPL,
    cast(STD_ID_GEO_PLACE as {{ dbt_utils.type_string() }}) as STD_ID_GEO_PLACE,
    cast(STD_N_FAM_NAME_1 as {{ dbt_utils.type_string() }}) as STD_N_FAM_NAME_1,
    cast(STD_N_FIRST_NAME as {{ dbt_utils.type_string() }}) as STD_N_FIRST_NAME,
    cast(STD_N_USUAL_NAME as {{ dbt_utils.type_string() }}) as STD_N_USUAL_NAME,
    cast(STD_PERMISO_TRAB as {{ dbt_utils.type_string() }}) as STD_PERMISO_TRAB,
    cast(STD_TALLA_CAMISA as {{ dbt_utils.type_string() }}) as STD_TALLA_CAMISA,
    cast(SUS_ID_ETHNICITY as {{ dbt_utils.type_string() }}) as SUS_ID_ETHNICITY,
    cast(CCL_NOCONTRATABLE as {{ dbt_utils.type_float() }}) as CCL_NOCONTRATABLE,
    cast(SFR_N_MAIDEN_NAME as {{ dbt_utils.type_string() }}) as SFR_N_MAIDEN_NAME,
    cast(SSP_PRIM_APELLIDO as {{ dbt_utils.type_string() }}) as SSP_PRIM_APELLIDO,
    cast(STD_ID_SALUTATION as {{ dbt_utils.type_string() }}) as STD_ID_SALUTATION,
    cast(STD_N_MAIDEN_NAME as {{ dbt_utils.type_string() }}) as STD_N_MAIDEN_NAME,
    cast(SUS_ID_VET_DIS_TY as {{ dbt_utils.type_string() }}) as SUS_ID_VET_DIS_TY,
    cast(CCB_BIRTH_CERT_NUM as {{ dbt_utils.type_string() }}) as CCB_BIRTH_CERT_NUM,
    cast(CCL_DATE_ADITIONAL as {{ dbt_utils.type_string() }}) as CCL_DATE_ADITIONAL,
    cast(CCL_ID_SUB_GEO_DIV as {{ dbt_utils.type_string() }}) as CCL_ID_SUB_GEO_DIV,
    cast(CCL_ID_SUCURSAL_SS as {{ dbt_utils.type_string() }}) as CCL_ID_SUCURSAL_SS,
    cast(CCL_NAME_ADITIONA2 as {{ dbt_utils.type_string() }}) as CCL_NAME_ADITIONA2,
    cast(CCL_NAME_ADITIONA3 as {{ dbt_utils.type_string() }}) as CCL_NAME_ADITIONA3,
    cast(CCL_NAME_ADITIONA4 as {{ dbt_utils.type_string() }}) as CCL_NAME_ADITIONA4,
    cast(CCL_NAME_ADITIONAL as {{ dbt_utils.type_string() }}) as CCL_NAME_ADITIONAL,
    cast(IDENTIFICADOR_LOTE as {{ dbt_utils.type_string() }}) as IDENTIFICADOR_LOTE,
    cast(M4CCL_ALTURA_PERSO as {{ dbt_utils.type_float() }}) as M4CCL_ALTURA_PERSO,
    cast(M4CCL_ID_TIPO_ALTU as {{ dbt_utils.type_string() }}) as M4CCL_ID_TIPO_ALTU,
    cast(M4CCL_ID_TIPO_PESO as {{ dbt_utils.type_string() }}) as M4CCL_ID_TIPO_PESO,
    cast(M4CCL_PESO_PERSONA as {{ dbt_utils.type_float() }}) as M4CCL_PESO_PERSONA,
    cast(SAR_N_MARRIED_NAME as {{ dbt_utils.type_string() }}) as SAR_N_MARRIED_NAME,
    cast(SFR_NM_BIRTH_PLACE as {{ dbt_utils.type_string() }}) as SFR_NM_BIRTH_PLACE,
    cast(SSP_ID_PAIS_EMISOR as {{ dbt_utils.type_string() }}) as SSP_ID_PAIS_EMISOR,
    cast(STD_ID_SUB_GEO_DIV as {{ dbt_utils.type_string() }}) as STD_ID_SUB_GEO_DIV,
    cast(STD_TALLA_PANTALON as {{ dbt_utils.type_string() }}) as STD_TALLA_PANTALON,
    cast(SUK_OTHER_FORENAME as {{ dbt_utils.type_string() }}) as SUK_OTHER_FORENAME,
    cast(SUK_PREVIOUS_SURNA as {{ dbt_utils.type_string() }}) as SUK_PREVIOUS_SURNA,
    cast(SUS_ID_VETERAN_WHE as {{ dbt_utils.type_string() }}) as SUS_ID_VETERAN_WHE,
    cast(STD_AHORRO_EXT_ASOC as {{ dbt_utils.type_float() }}) as STD_AHORRO_EXT_ASOC,
    cast(STD_AHO_ESC_EXT_ASOC as {{ dbt_utils.type_float() }}) as STD_AHO_ESC_EXT_ASOC,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('LND_TBL_STD_PERSON_ab1') }}
-- LND_TBL_STD_PERSON
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

