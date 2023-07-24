create or replace TABLE DBGRANTS (
	CREATED_ON TIMESTAMP_LTZ(9),
	PRIVILEGE VARCHAR(16777216),
	GRANTED_ON VARCHAR(16777216),
	NAME VARCHAR(16777216),
	GRANTED_TO VARCHAR(16777216),
	GRANTEE_NAME VARCHAR(16777216),
	GRANT_OPTION VARCHAR(16777216),
	GRANTED_BY VARCHAR(16777216),
	REFRESH_DATE TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='stores snapshot of current grants'
;