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
                            identifier='LND_TBL_STD_LEG_ENT'
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
                                where 1=1 {{ incremental_clause('_airbyte_normalized_at', adapter.quote(this.schema) + '.' + adapter.quote('LND_TBL_STD_LEG_ENT')) }}
                            ) recent_records
                            left join (
                                select _airbyte_unique_key as unique_key, count(_airbyte_unique_key) as active_count
                                from {{ this }}
                                where _airbyte_active_row = 1 {{ incremental_clause('_airbyte_normalized_at', adapter.quote(this.schema) + '.' + adapter.quote('LND_TBL_STD_LEG_ENT')) }}
                                group by _airbyte_unique_key
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ","drop view _airbyte_lnd_ula_pnet_prd.LND_TBL_STD_LEG_ENT_stg",'{{ 
        apply_default_policy_tag(this, "projects/df-central-command-prd/locations/us/taxonomies/3753529189283377636/policyTags/1570780607584273335")
    }}'],
    tags = [ "top-level" ]
) }}
-- depends_on: ref('LND_TBL_STD_LEG_ENT_stg')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('LND_TBL_STD_LEG_ENT_stg')  }}
    -- LND_TBL_STD_LEG_ENT from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_STD_LEG_ENT') }}
    where 1 = 1
    {{ incremental_clause('_airbyte_emitted_at', this) }}
),
new_data_ids as (
    -- build a subset of _airbyte_unique_key from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'ID_ORGANIZATION',
            'STD_ID_LEG_ENT',
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
        {{ star_intersect(ref('LND_TBL_STD_LEG_ENT_stg'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._airbyte_unique_key = new_data_ids._airbyte_unique_key
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._airbyte_ab_id = inc_data._airbyte_ab_id
    where _airbyte_active_row = 1
),
input_data as (
    select {{ dbt_utils.star(ref('LND_TBL_STD_LEG_ENT_stg')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('LND_TBL_STD_LEG_ENT_stg')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('LND_TBL_STD_LEG_ENT_stg')  }}
    -- LND_TBL_STD_LEG_ENT from {{ source('lnd_ula_pnet_prd', '_airbyte_raw_LND_TBL_STD_LEG_ENT') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
      'ID_ORGANIZATION',
      'STD_ID_LEG_ENT',
      ]) }} as _airbyte_unique_key,
      ID_APPROLE,
      ID_SECUSER,
      SAR_ID_ART,
      SSP_SUBCIF,
      STD_DT_END,
      STD_NID_ACT,
      STD_NID_ENT,
      SSP_ID_LEGAL,
      STD_DT_START,
      STD_N_LEG_ENT,
      DT_LAST_UPDATE,
      SFR_REG_LEGENT,
      STD_ID_LEG_ENT,
      CCB_SOC_SEC_NUM,
      ID_ORGANIZATION,
      SPO_ANO_LEG_ENT,
      SPO_ID_NATUREZA,
      STD_DESCRIPTION,
      SME_FEC_PRES_DIM,
      SME_FOLIO_OPERAC,
      SPO_COD_CONCELHO,
      SPO_DT_PUBLIC_BT,
      SPO_REP_PERC_PUB,
      SSP_COD_LOC_EMPR,
      SPO_ID_ACTIVIDADE,
      SPO_INST_REG_TRAB,
      SPO_VOLUME_VENDAS,
      SSP_CARGO_CERTBRA,
      SSP_CARGO_CERTENG,
      SSP_CARGO_CERTESP,
      SSP_CARGO_CERTFRA,
      SSP_CARGO_CERTGEN,
      SSP_CARGO_CERTGER,
      SSP_CARGO_CERTITA,
      SSP_ID_TP_ALF_EMP,
      SSP_ID_TP_ID_LGAL,
      SSP_NUM_TRAB_EMPR,
      SFR_ID_ACTIV_SECTO,
      SFR_ID_LEGAL_HOLDI,
      SFR_LEGAL_ENTITY_L,
      SFR_REG_SUB_LEGENT,
      SPO_CAPITAL_SOCIAL,
      SPO_DT_ULT_TAB_SAL,
      SPO_REPARTICAO_PER,
      SPO_REP_PERC_ESTRA,
      SSP_CARGO_CERT_HAB,
      SSP_FIRMT_CERT_HAB,
      SSP_ID_LEG_CER_HAB,
      SSP_ID_PAIS_EMISOR,
      SSP_ID_RESPONSABLE,
      SSP_LIT_PLURIEMPLE,
      DT_LAST_UPDATE as _airbyte_start_at,
      lag(DT_LAST_UPDATE) over (
        partition by ID_ORGANIZATION, STD_ID_LEG_ENT
        order by
            DT_LAST_UPDATE is null asc,
            DT_LAST_UPDATE desc,
            _airbyte_emitted_at desc
      ) as _airbyte_end_at,
      case when row_number() over (
        partition by ID_ORGANIZATION, STD_ID_LEG_ENT
        order by
            DT_LAST_UPDATE is null asc,
            DT_LAST_UPDATE desc,
            _airbyte_emitted_at desc
      ) = 1 then 1 else 0 end as _airbyte_active_row,
      _airbyte_ab_id,
      _airbyte_emitted_at,
      _airbyte_LND_TBL_STD_LEG_ENT_hashid
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
    ID_APPROLE,
    ID_SECUSER,
    SAR_ID_ART,
    SSP_SUBCIF,
    STD_DT_END,
    STD_NID_ACT,
    STD_NID_ENT,
    SSP_ID_LEGAL,
    STD_DT_START,
    STD_N_LEG_ENT,
    DT_LAST_UPDATE,
    SFR_REG_LEGENT,
    STD_ID_LEG_ENT,
    CCB_SOC_SEC_NUM,
    ID_ORGANIZATION,
    SPO_ANO_LEG_ENT,
    SPO_ID_NATUREZA,
    STD_DESCRIPTION,
    SME_FEC_PRES_DIM,
    SME_FOLIO_OPERAC,
    SPO_COD_CONCELHO,
    SPO_DT_PUBLIC_BT,
    SPO_REP_PERC_PUB,
    SSP_COD_LOC_EMPR,
    SPO_ID_ACTIVIDADE,
    SPO_INST_REG_TRAB,
    SPO_VOLUME_VENDAS,
    SSP_CARGO_CERTBRA,
    SSP_CARGO_CERTENG,
    SSP_CARGO_CERTESP,
    SSP_CARGO_CERTFRA,
    SSP_CARGO_CERTGEN,
    SSP_CARGO_CERTGER,
    SSP_CARGO_CERTITA,
    SSP_ID_TP_ALF_EMP,
    SSP_ID_TP_ID_LGAL,
    SSP_NUM_TRAB_EMPR,
    SFR_ID_ACTIV_SECTO,
    SFR_ID_LEGAL_HOLDI,
    SFR_LEGAL_ENTITY_L,
    SFR_REG_SUB_LEGENT,
    SPO_CAPITAL_SOCIAL,
    SPO_DT_ULT_TAB_SAL,
    SPO_REPARTICAO_PER,
    SPO_REP_PERC_ESTRA,
    SSP_CARGO_CERT_HAB,
    SSP_FIRMT_CERT_HAB,
    SSP_ID_LEG_CER_HAB,
    SSP_ID_PAIS_EMISOR,
    SSP_ID_RESPONSABLE,
    SSP_LIT_PLURIEMPLE,
    _airbyte_start_at,
    _airbyte_end_at,
    _airbyte_active_row,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_LND_TBL_STD_LEG_ENT_hashid
from dedup_data where _airbyte_row_num = 1

