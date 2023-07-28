create or replace database CRIMSON_STG;

create or replace schema PUBLIC;

create or replace TABLE ADWKS_SD_PERSON (
	BUSINESSENTITYID NUMBER(38,0) NOT NULL,
	PERSONTYPE VARCHAR(8),
	NAMESTYLE BOOLEAN,
	TITLE VARCHAR(32),
	FIRSTNAME VARCHAR(200),
	MIDDLENAME VARCHAR(200),
	LASTNAME VARCHAR(200),
	SUFFIX VARCHAR(40),
	EMAILPROMOTION NUMBER(38,0),
	ADDITIONALCONTACTINFO VARCHAR(16777216) COLLATE 'utf8',
	DEMOGRAPHICS VARCHAR(16777216) COLLATE 'utf8',
	ROWGUID VARCHAR(36),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	SYS_OPERATION_TYPE VARCHAR(1),
	SYS_OPERATION_TIME TIMESTAMP_NTZ(9),
	SYS_OPERATION_OWNER VARCHAR(256),
	SYS_TRANSACTION_ID VARCHAR(100)
);