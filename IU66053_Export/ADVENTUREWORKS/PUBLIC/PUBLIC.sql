create or replace schema PUBLIC;

create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_BOOLEAN as (VAL BOOLEAN) 
returns BOOLEAN ->
CASE
              WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
              ELSE NULL
            END
;
create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_DATE as (VAL DATE) 
returns DATE ->
CASE
          WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
          ELSE NULL
        END
;
create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_NUMERIC as (VAL NUMBER(38,0)) 
returns NUMBER(38,0) ->
CASE
          WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
          ELSE NULL
        END
;
create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_STRING as (VAL VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE
              WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
              ELSE '*******'
            END
;
create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP as (VAL TIMESTAMP_NTZ(9)) 
returns TIMESTAMP_NTZ(9) ->
CASE
          WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
          ELSE NULL
        END
;
create or replace masking policy ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ as (VAL TIMESTAMP_NTZ(9)) 
returns TIMESTAMP_NTZ(9) ->
CASE
          WHEN IS_ROLE_IN_SESSION('ADVENTUREWORKS_READONLY') then val
          ELSE NULL
        END
;
create or replace masking policy FEM_MASKING_POLICY_BOOLEAN as (VAL BOOLEAN) 
returns BOOLEAN ->
CASE
              WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
              ELSE NULL
            END
;
create or replace masking policy FEM_MASKING_POLICY_DATE as (VAL DATE) 
returns DATE ->
CASE
          WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
          ELSE NULL
        END
;
create or replace masking policy FEM_MASKING_POLICY_NUMERIC as (VAL NUMBER(38,0)) 
returns NUMBER(38,0) ->
CASE
          WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
          ELSE NULL
        END
;
create or replace masking policy FEM_MASKING_POLICY_STRING as (VAL VARCHAR(16777216)) 
returns VARCHAR(16777216) ->
CASE
              WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
              ELSE '*******'
            END
;
create or replace masking policy FEM_MASKING_POLICY_TIMESTAMP as (VAL TIMESTAMP_NTZ(9)) 
returns TIMESTAMP_NTZ(9) ->
CASE
          WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
          ELSE NULL
        END
;
create or replace masking policy FEM_MASKING_POLICY_TIMESTAMP_NTZ as (VAL TIMESTAMP_NTZ(9)) 
returns TIMESTAMP_NTZ(9) ->
CASE
          WHEN IS_ROLE_IN_SESSION('PRD_INFORMATICA_READONLY') then val
          ELSE NULL
        END
;