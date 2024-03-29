create or replace TABLE SHIPMETHOD_BT (
	SHIPMETHODID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(200),
	SHIPBASE NUMBER(19,4),
	SHIPRATE NUMBER(19,4),
	ROWGUID VARCHAR(36),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	OPERATION_TYPE VARCHAR(1),
	OPERATION_TIME TIMESTAMP_NTZ(9),
	OPERATION_OWNER VARCHAR(256),
	TRANSACTION_ID VARCHAR(100)
);