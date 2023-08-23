create or replace masking policy FEM_MASKING_POLICY_BOOLEAN as (VAL BOOLEAN) 
returns BOOLEAN ->
CASE
              WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
              ELSE NULL
            END
;