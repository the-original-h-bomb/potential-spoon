create or replace TABLE INFO_EMAILADDRESS (
	BUSINESSENTITYID_OLD NUMBER(38,0),
	BUSINESSENTITYID NUMBER(38,0),
	EMAILADDRESSID_OLD NUMBER(38,0),
	EMAILADDRESSID NUMBER(38,0),
	EMAILADDRESS_OLD VARCHAR(2000),
	EMAILADDRESS VARCHAR(2000),
	ROWGUID_OLD VARCHAR(36),
	ROWGUID VARCHAR(36),
	MODIFIEDDATE_OLD TIMESTAMP_NTZ(3),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	TEST_NEW_COLUMN2_OLD NUMBER(38,0),
	TEST_NEW_COLUMN2 NUMBER(38,0),
	INFA_OPERATION_TYPE VARCHAR(1),
	INFA_OPERATION_TIME TIMESTAMP_NTZ(9),
	INFA_OPERATION_OWNER VARCHAR(256),
	INFA_TRANSACTION_ID VARCHAR(100),
	INFA_OPERATION_SEQUENCE NUMBER(20,0)
);