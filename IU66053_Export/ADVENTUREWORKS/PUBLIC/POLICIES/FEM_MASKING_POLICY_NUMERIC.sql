create or replace masking policy FEM_MASKING_POLICY_NUMERIC as (VAL NUMBER(38,0)) 
returns NUMBER(38,0) ->
CASE
          WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
          ELSE NULL
        END
;