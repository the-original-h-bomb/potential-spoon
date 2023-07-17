create or replace TABLE INFORMATICA_CDC_RECOVERY (
	TASK_ID VARCHAR(1024) NOT NULL,
	TYPE VARCHAR(128) NOT NULL,
	SCHEMA_NAME VARCHAR(512) NOT NULL,
	TABLE_NAME VARCHAR(512) NOT NULL,
	CYCLE_NUMBER NUMBER(38,0),
	SEQUENCE VARCHAR(2048),
	primary key (TASK_ID, TYPE, SCHEMA_NAME, TABLE_NAME)
);