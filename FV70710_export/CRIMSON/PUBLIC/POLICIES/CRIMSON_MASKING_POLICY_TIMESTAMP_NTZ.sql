create or replace masking policy CRIMSON_MASKING_POLICY_TIMESTAMP_NTZ as (VAL TIMESTAMP_NTZ(9)) 
returns TIMESTAMP_NTZ(9) ->
CASE
          WHEN IS_ROLE_IN_SESSION('CRIMSON_READONLY') then val
          ELSE NULL
        END
;