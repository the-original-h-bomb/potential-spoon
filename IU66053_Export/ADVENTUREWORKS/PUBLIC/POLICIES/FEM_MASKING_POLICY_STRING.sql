create or replace masking policy FEM_MASKING_POLICY_STRING as (VAL VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE
              WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
              ELSE '*******'
            END
;