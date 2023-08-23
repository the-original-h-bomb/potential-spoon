create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_BOOLEAN as (VAL BOOLEAN) 
returns BOOLEAN ->
CASE
              WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
              ELSE NULL
            END
;