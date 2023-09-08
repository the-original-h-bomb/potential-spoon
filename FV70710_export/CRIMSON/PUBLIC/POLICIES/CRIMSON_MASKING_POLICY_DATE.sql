create or replace masking policy CRIMSON_MASKING_POLICY_DATE as (VAL DATE) 
returns DATE ->
CASE
          WHEN IS_ROLE_IN_SESSION('CRIMSON_READONLY') then val
          ELSE NULL
        END
;