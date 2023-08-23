create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_DATE as (VAL DATE) 
returns DATE ->
CASE
          WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
          ELSE NULL
        END
;