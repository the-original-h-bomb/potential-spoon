create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_STRING as (VAL VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE
              WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
              ELSE '*******'
            END
;