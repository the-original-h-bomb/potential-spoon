create or replace TABLE ADWKS_SD_MYNEWTABLE (
	ID NUMBER(38,0) NOT NULL,
	DESCRIPTION VARCHAR(100),
	SYS_OPERATION_TYPE VARCHAR(1),
	SYS_OPERATION_TIME TIMESTAMP_NTZ(9),
	SYS_OPERATION_OWNER VARCHAR(256),
	SYS_TRANSACTION_ID VARCHAR(100)
);