create or replace TABLE PROCEDURES_T (
	PROCEDURE_CATALOG VARCHAR(16777216),
	PROCEDURE_SCHEMA VARCHAR(16777216),
	PROCEDURE_NAME VARCHAR(16777216),
	PROCEDURE_OWNER VARCHAR(16777216),
	ARGUMENT_SIGNATURE VARCHAR(16777216),
	DATA_TYPE VARCHAR(16777216),
	CHARACTER_MAXIMUM_LENGTH NUMBER(38,0),
	CHARACTER_OCTET_LENGTH NUMBER(38,0),
	NUMERIC_PRECISION NUMBER(38,0),
	NUMERIC_PRECISION_RADIX NUMBER(2,0),
	NUMERIC_SCALE NUMBER(38,0),
	PROCEDURE_LANGUAGE VARCHAR(16777216),
	PROCEDURE_DEFINITION VARCHAR(16777216),
	CREATED TIMESTAMP_LTZ(3),
	LAST_ALTERED TIMESTAMP_LTZ(3),
	COMMENT VARCHAR(16777216)
);