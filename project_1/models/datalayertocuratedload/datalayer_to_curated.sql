
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(schema='bdt_gold',alias='datalayer_to_curated',materialized='incremental',unique_key=['emp_no']) }}

with source_data as (

    select *,row_number() over(partition by emp_no) as row_num from dbtsample.dbt_bronze.stg_file_datalayer

)

select sd.*,1 as batch_nbr,current_timestamp as created_at,current_timestamp as updated_at
from source_data sd
where row_num=1
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
