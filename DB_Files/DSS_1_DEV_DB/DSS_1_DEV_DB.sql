create or replace database DSS_1_DEV_DB;

create or replace schema DEMO;

create or replace TABLE FOGGY_TABLE (
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	AGE NUMBER(38,0),
	NICKNAME NUMBER(38,0)
);
create or replace schema PUBLIC;
