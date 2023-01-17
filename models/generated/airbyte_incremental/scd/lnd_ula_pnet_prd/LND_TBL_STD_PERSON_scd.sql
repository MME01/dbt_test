{{ config(
    cluster_by = ["_airbyte_unique_key_scd","_airbyte_emitted_at"],
    partition_by = {"field": "_airbyte_active_row", "data_type": "int64", "range": {"start": 0, "end": 1, "interval": 1}},
    unique_key = "_airbyte_unique_key_scd",
    schema = "lnd_ula_pnet_prd",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='LND_TBL_STD_PERSON'
                        )
                    %}
                    {#
                    If the final table doesn't exist, then obviously we can't delete anything from it.
                    Also, after a reset, the final table is created without the _airbyte_unique_key column (this column is created during the first sync)
                    So skip this deletion if the column doesn't exist. (in this case, the table is guaranteed to be empty anyway)
                    #}
                    {%
                    if final_table_relation is not none and '_airbyte_unique_key' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    -- Delete records which are no longer active:
                    -- This query is equivalent, but the left join version is more performant:
                    -- delete from final_table where unique_key in (
                    --     select unique_key from scd_table where 1 = 1 <incremental_clause(normalized_at, final_table)>
                    -- ) and unique_key not in (
                    --     select unique_key from scd_table where active_row = 1 <incremental_clause(normalized_at, final_table)>
                    -- )
                    -- We're incremental against normalized_at rather than emitted_at because we need to fetch the SCD
                    -- entries that were _updated_ recently. This is because a deleted record will have an SCD record
                    -- which was emitted a long time ago, but recently re-normalized to have active_row = 0.
                    delete from {{ final_table_relation }} final_table where final_table._airbyte_unique_key in (
                        select recent_records.unique_key
                        from (
                                select distinct _airbyte_unique_key as unique_key
                                from {{ this }}
                                where 1=1 {{ incremental_clause('_airbyte_normalized_at', adapter.quote(this.schema) + '.' + adapter.quote('LND_TBL_STD_PERSON')) }}
                            ) recent_records
                            left join (
                                select _airbyte_unique_key as unique_key, count(_airbyte_unique_key) as active_count
                                from {{ this }}
                                where _airbyte_active_row = 1 {{ incremental_clause('_airbyte_normalized_at', adapter.quote(this.schema) + '.' + adapter.quote('LND_TBL_STD_PERSON')) }}
                                group by _airbyte_unique_key
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ","drop view _airbyte_lnd_ula_pnet_prd.LND_TBL_STD_PERSON_stg",'{{ 
        apply_default_policy_tag(this, "projects/df-central-command-prd/locations/us/taxonomies/3753529189283377636/policyTags/1570780607584273335")
    }}'],
    tags = [ "top-level" ]
) }}
-- depends_on: ref('LND_TBL_STD_PERSON_stg')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('LND_TBL_STD_PERSON_stg')  }}
    -- LND_TBL_STD_PERSON from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_STD_PERSON') }}
    where 1 = 1
    {{ incremental_clause('_airbyte_emitted_at', this) }}
),
new_data_ids as (
    -- build a subset of _airbyte_unique_key from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'ID_ORGANIZATION',
            'STD_ID_PERSON',
        ]) }} as _airbyte_unique_key
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('LND_TBL_STD_PERSON_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._airbyte_unique_key = new_data_ids._airbyte_unique_key
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._airbyte_ab_id = inc_data._airbyte_ab_id
    where _airbyte_active_row = 1
),
input_data as (
    select {{ dbt_utils.star(ref('LND_TBL_STD_PERSON_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('LND_TBL_STD_PERSON_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('LND_TBL_STD_PERSON_stg')  }}
    -- LND_TBL_STD_PERSON from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_STD_PERSON') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
      'ID_ORGANIZATION',
      'STD_ID_PERSON',
      ]) }} as _airbyte_unique_key,
      STD_SSN,
      SCO_PHOTO,
      ID_APPROLE,
      ID_SECUSER,
      SCO_SMOKER,
      SGE_ID_VSW,
      STD_SS_KEY,
      SBR_ID_RACA,
      SCB_DIG_VER,
      SCO_GB_NAME,
      STD_COMMENT,
      SUS_VETERAN,
      CCL_CK_BENEF,
      SAR_EXPEDIDO,
      SGE_ID_TITEL,
      STD_AFILIADO,
      STD_DT_BIRTH,
      CCL_LACTANCIA,
      CCL_OCUPACION,
      SCO_HOME_PAGE,
      SCO_ID_REGION,
      SSP_ID_TP_DOC,
      STD_ID_GENDER,
      STD_ID_PERSON,
      STD_SS_NUMBER,
      CCL_ID_COUNTRY,
      CCL_ID_GEO_DIV,
      DT_LAST_UPDATE,
      SCO_PHOTO_INET,
      STD_ID_COUNTRY,
      STD_ID_GEO_DIV,
      SUS_DEATH_DATE,
      CCB_DRIVERS_NUM,
      ID_ORGANIZATION,
      SGE_ID_ADD_NAME,
      SUK_MAIDEN_NAME,
      CCL_CONOCIDO_POR,
      DESCRIPCION_LOTE,
      FECHA_APLIC_LOTE,
      SGE_N_NAME_COMPL,
      STD_ID_GEO_PLACE,
      STD_N_FAM_NAME_1,
      STD_N_FIRST_NAME,
      STD_N_USUAL_NAME,
      STD_PERMISO_TRAB,
      STD_TALLA_CAMISA,
      SUS_ID_ETHNICITY,
      CCL_NOCONTRATABLE,
      SFR_N_MAIDEN_NAME,
      SSP_PRIM_APELLIDO,
      STD_ID_SALUTATION,
      STD_N_MAIDEN_NAME,
      SUS_ID_VET_DIS_TY,
      CCB_BIRTH_CERT_NUM,
      CCL_DATE_ADITIONAL,
      CCL_ID_SUB_GEO_DIV,
      CCL_ID_SUCURSAL_SS,
      CCL_NAME_ADITIONA2,
      CCL_NAME_ADITIONA3,
      CCL_NAME_ADITIONA4,
      CCL_NAME_ADITIONAL,
      IDENTIFICADOR_LOTE,
      M4CCL_ALTURA_PERSO,
      M4CCL_ID_TIPO_ALTU,
      M4CCL_ID_TIPO_PESO,
      M4CCL_PESO_PERSONA,
      SAR_N_MARRIED_NAME,
      SFR_NM_BIRTH_PLACE,
      SSP_ID_PAIS_EMISOR,
      STD_ID_SUB_GEO_DIV,
      STD_TALLA_PANTALON,
      SUK_OTHER_FORENAME,
      SUK_PREVIOUS_SURNA,
      SUS_ID_VETERAN_WHE,
      STD_AHORRO_EXT_ASOC,
      STD_AHO_ESC_EXT_ASOC,
      DT_LAST_UPDATE as _airbyte_start_at,
      lag(DT_LAST_UPDATE) over (
        partition by ID_ORGANIZATION, STD_ID_PERSON
        order by
            DT_LAST_UPDATE is null asc,
            DT_LAST_UPDATE desc,
            _airbyte_emitted_at desc
      ) as _airbyte_end_at,
      case when row_number() over (
        partition by ID_ORGANIZATION, STD_ID_PERSON
        order by
            DT_LAST_UPDATE is null asc,
            DT_LAST_UPDATE desc,
            _airbyte_emitted_at desc
      ) = 1 then 1 else 0 end as _airbyte_active_row,
      _airbyte_ab_id,
      _airbyte_emitted_at,
      _airbyte_LND_TBL_STD_PERSON_hashid
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by
                _airbyte_unique_key,
                _airbyte_start_at,
                _airbyte_emitted_at
            order by _airbyte_active_row desc, _airbyte_ab_id
        ) as _airbyte_row_num,
        {{ dbt_utils.surrogate_key([
          '_airbyte_unique_key',
          '_airbyte_start_at',
          '_airbyte_emitted_at'
        ]) }} as _airbyte_unique_key_scd,
        scd_data.*
    from scd_data
)
select
    _airbyte_unique_key,
    _airbyte_unique_key_scd,
    STD_SSN,
    SCO_PHOTO,
    ID_APPROLE,
    ID_SECUSER,
    SCO_SMOKER,
    SGE_ID_VSW,
    STD_SS_KEY,
    SBR_ID_RACA,
    SCB_DIG_VER,
    SCO_GB_NAME,
    STD_COMMENT,
    SUS_VETERAN,
    CCL_CK_BENEF,
    SAR_EXPEDIDO,
    SGE_ID_TITEL,
    STD_AFILIADO,
    STD_DT_BIRTH,
    CCL_LACTANCIA,
    CCL_OCUPACION,
    SCO_HOME_PAGE,
    SCO_ID_REGION,
    SSP_ID_TP_DOC,
    STD_ID_GENDER,
    STD_ID_PERSON,
    STD_SS_NUMBER,
    CCL_ID_COUNTRY,
    CCL_ID_GEO_DIV,
    DT_LAST_UPDATE,
    SCO_PHOTO_INET,
    STD_ID_COUNTRY,
    STD_ID_GEO_DIV,
    SUS_DEATH_DATE,
    CCB_DRIVERS_NUM,
    ID_ORGANIZATION,
    SGE_ID_ADD_NAME,
    SUK_MAIDEN_NAME,
    CCL_CONOCIDO_POR,
    DESCRIPCION_LOTE,
    FECHA_APLIC_LOTE,
    SGE_N_NAME_COMPL,
    STD_ID_GEO_PLACE,
    STD_N_FAM_NAME_1,
    STD_N_FIRST_NAME,
    STD_N_USUAL_NAME,
    STD_PERMISO_TRAB,
    STD_TALLA_CAMISA,
    SUS_ID_ETHNICITY,
    CCL_NOCONTRATABLE,
    SFR_N_MAIDEN_NAME,
    SSP_PRIM_APELLIDO,
    STD_ID_SALUTATION,
    STD_N_MAIDEN_NAME,
    SUS_ID_VET_DIS_TY,
    CCB_BIRTH_CERT_NUM,
    CCL_DATE_ADITIONAL,
    CCL_ID_SUB_GEO_DIV,
    CCL_ID_SUCURSAL_SS,
    CCL_NAME_ADITIONA2,
    CCL_NAME_ADITIONA3,
    CCL_NAME_ADITIONA4,
    CCL_NAME_ADITIONAL,
    IDENTIFICADOR_LOTE,
    M4CCL_ALTURA_PERSO,
    M4CCL_ID_TIPO_ALTU,
    M4CCL_ID_TIPO_PESO,
    M4CCL_PESO_PERSONA,
    SAR_N_MARRIED_NAME,
    SFR_NM_BIRTH_PLACE,
    SSP_ID_PAIS_EMISOR,
    STD_ID_SUB_GEO_DIV,
    STD_TALLA_PANTALON,
    SUK_OTHER_FORENAME,
    SUK_PREVIOUS_SURNA,
    SUS_ID_VETERAN_WHE,
    STD_AHORRO_EXT_ASOC,
    STD_AHO_ESC_EXT_ASOC,
    _airbyte_start_at,
    _airbyte_end_at,
    _airbyte_active_row,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_LND_TBL_STD_PERSON_hashid
from dedup_data where _airbyte_row_num = 1

