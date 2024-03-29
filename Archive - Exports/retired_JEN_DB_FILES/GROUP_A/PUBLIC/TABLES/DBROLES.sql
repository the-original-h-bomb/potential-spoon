create or replace TABLE DBROLES (
	CREATED_ON TIMESTAMP_LTZ(9),
	NAME VARCHAR(16777216),
	IS_DEFAULT VARCHAR(16777216),
	IS_CURRENT VARCHAR(16777216),
	IS_INHERITED VARCHAR(16777216),
	ASSIGNED_TO_USERS NUMBER(38,0),
	GRANTED_TO_ROLES NUMBER(38,0),
	GRANTED_ROLES NUMBER(38,0),
	OWNER VARCHAR(16777216),
	RCOMMENT VARCHAR(16777216),
	REFRESH_DATE TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='stores snapshot of current snowflake roles'
;