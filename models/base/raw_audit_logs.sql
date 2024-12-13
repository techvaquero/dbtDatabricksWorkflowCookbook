{{
    config(
        materialized = 'view'
    )
}}

select * from system.access.audit where event_date > '2024-08-01'