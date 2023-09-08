create or replace masking policy CRIMSON_MASKING_POLICY_NUMERIC as (VAL NUMBER(38,0)) 
returns NUMBER(38,0) ->
CASE
          WHEN IS_ROLE_IN_SESSION('CRIMSON_READONLY') then val
          ELSE NULL
        END
;