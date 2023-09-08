create or replace masking policy CRIMSON_MASKING_POLICY_BOOLEAN as (VAL BOOLEAN) 
returns BOOLEAN ->
CASE
              WHEN IS_ROLE_IN_SESSION('CRIMSON_READONLY') then val
              ELSE NULL
            END
;