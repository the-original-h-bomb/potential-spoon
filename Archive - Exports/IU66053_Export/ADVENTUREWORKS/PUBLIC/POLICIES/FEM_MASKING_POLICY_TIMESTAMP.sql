create or replace masking policy FEM_MASKING_POLICY_TIMESTAMP as (VAL TIMESTAMP_NTZ(9)) 
returns TIMESTAMP_NTZ(9) ->
CASE
          WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
          ELSE NULL
        END
;