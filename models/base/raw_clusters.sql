{{
    config(
        materialized = 'view'
    )
}}
select * 
from system.compute.clusters

-- select * 
-- from stream system.compute.clusters
--where
--owned_by like '%{{ var('DBT_CUSTOMER') }}%'
   