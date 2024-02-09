
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(schema='datalayer',alias='stg_file_datalayer',materialized='incremental',pre_hook=["truncate table {{this}}"]) }}

with source_data as (

    select * from dbtsample.dbt_bronze.stg_file

)

select sd.*
from source_data sd

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
