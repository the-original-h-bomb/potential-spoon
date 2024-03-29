create or replace TABLE ADWKS_SD_ADDRESSTYPE (
	ADDRESSTYPEID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(200),
	ROWGUID VARCHAR(36),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	SYS_OPERATION_TYPE VARCHAR(1),
	SYS_OPERATION_TIME TIMESTAMP_NTZ(9),
	SYS_OPERATION_OWNER VARCHAR(256),
	SYS_TRANSACTION_ID VARCHAR(100)
);