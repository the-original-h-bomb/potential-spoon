create or replace schema PUBLIC;

create or replace tag AUDIENCE  allowed_values  'department' , 'iu' , 'iuf' ;
create or replace tag INSTANCE  allowed_values  'dev' , 'prd' , 'stg' ;
create or replace tag VISIBILITY  allowed_values  'department' , 'iu' , 'iuf' , 'public' ;
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
create or replace TABLE DBUSERS (
	NAME VARCHAR(16777216),
	CREATED_ON TIMESTAMP_LTZ(9),
	LOGIN_NAME VARCHAR(16777216),
	DISPLAY_NAME VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	MINS_TO_UNLOCK VARCHAR(16777216),
	DAYS_TO_EXPIRY VARCHAR(16777216),
	TCOMMENT VARCHAR(16777216),
	DISABLED VARCHAR(16777216),
	MUST_CHANGE_PASSWORD VARCHAR(16777216),
	SNOWFLAKE_LOCK VARCHAR(16777216),
	DEFAULT_WAREHOUSE VARCHAR(16777216),
	DEFAULT_NAMESPACE VARCHAR(16777216),
	DEFAULT_ROLE VARCHAR(16777216),
	DEFAULT_SECONDARY_ROLES VARCHAR(16777216),
	EXT_AUTHN_DUO VARCHAR(16777216),
	EXT_AUTHN_UID VARCHAR(16777216),
	MINS_TO_BYPASS_MFA VARCHAR(16777216),
	OWNER VARCHAR(16777216),
	LAST_SUCCESS_LOGIN TIMESTAMP_LTZ(9),
	EXPIRES_AT_TIME TIMESTAMP_LTZ(9),
	LOCKED_UNTIL_TIME TIMESTAMP_LTZ(9),
	HAS_PASSWORD VARCHAR(16777216),
	HAS_RSA_PUBLIC_KEY VARCHAR(16777216),
	REFRESH_DATE TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='stores snapshot of current snowflake users'
;
create or replace TABLE INDIANA_CERTIFIED_DIVERSITY_VENDORS (
	COMPANY_NAME VARCHAR(16777216),
	DBA VARCHAR(16777216),
	UNSPSC VARCHAR(16777216),
	UNSPSC_DESCRIPTION VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LASTNAME VARCHAR(16777216),
	MAILING_ADDRESS_1 VARCHAR(16777216),
	MAILING_ADDRESS_2 VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	APPLICATION_TYPE VARCHAR(16777216),
	ETHNIC_GROUP VARCHAR(16777216),
	CERTIFICATION_DATE DATE,
	EXPIRATION_DTE DATE,
	BIDDER_ID VARCHAR(16777216),
	EMAIL_ID VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	APPLICATION_STATUS VARCHAR(16777216),
	COMPANY_NAME_UPPER VARCHAR(16777216)
);
create or replace TABLE INDIANA_CERTIFIED_DIVERSITY_VENDORS_A (
	COMPANY_NAME VARCHAR(16777216),
	DBA VARCHAR(16777216),
	UNSPSC VARCHAR(16777216),
	UNSPSC_DESCRIPTION VARCHAR(16777216),
	FIRST_NAME VARCHAR(16777216),
	LASTNAME VARCHAR(16777216),
	MAILING_ADDRESS_1 VARCHAR(16777216),
	MAILING_ADDRESS_2 VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIP_CODE VARCHAR(16777216),
	APPLICATION_TYPE VARCHAR(16777216),
	ETHNIC_GROUP VARCHAR(16777216),
	CERTIFICATION_DATE DATE,
	EXPIRATION_DTE DATE,
	BIDDER_ID VARCHAR(16777216),
	EMAIL_ID VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	APPLICATION_STATUS VARCHAR(16777216),
	COMPANY_NAME_UPPER VARCHAR(16777216)
);
CREATE OR REPLACE PROCEDURE "SNAPSHOT_GRANTS"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
COMMENT='Captures the snapshot of grants and inserts the records into dbgrants'
EXECUTE AS CALLER
AS '
function role_grants() 
{
    var obj_rs = snowflake.execute({sqlText: `SELECT NAME FROM DBROLES;`});
    while(obj_rs.next()) {
        snowflake.execute({sqlText: `show grants to role "` + obj_rs.getColumnValue(1) + `" ;` });
        snowflake.execute( {sqlText:`insert into dbgrants select *,CURRENT_TIMESTAMP() from table(result_scan(last_query_id()));`});
        snowflake.execute({sqlText: `show grants on role "` + obj_rs.getColumnValue(1) + `" ;` });
        snowflake.execute( {sqlText:`insert into dbgrants select *,CURRENT_TIMESTAMP()from table(result_scan(last_query_id()));`});
    }
}
// — — — — — — — — — — — — — — — — — — — — — — — —
function user_grants()
{
    var obj_rs = snowflake.execute({sqlText: `SELECT NAME FROM DBUSERS;`});
    while(obj_rs.next()) {
      snowflake.execute({sqlText: `show grants to user "` + obj_rs.getColumnValue(1) + `" ;` });
      snowflake.execute( {sqlText:`insert into dbgrants select *, null,null,null,CURRENT_TIMESTAMP() from table(result_scan(last_query_id()));`});
      snowflake.execute({sqlText: `show grants on user "` + obj_rs.getColumnValue(1) + `" ;` });
      snowflake.execute( {sqlText:`insert into dbgrants select *, CURRENT_TIMESTAMP() from table(result_scan(last_query_id()));`});
    }
}
// — — — — — — — — — — — — — — — — — — — — — — — —
var result = "SUCCESS";
try {
    snowflake.execute( {sqlText: "truncate table DBGRANTS;"} );
    role_grants();
    user_grants();
} 
catch (err) {
    result = "FAILED: Code: " + err.code + "\\n State: " + err.state;result += "\\n Message: " + err.message;result += "\\nStack Trace:\\n" + err.stackTraceTxt;
}
    return result;
';
CREATE OR REPLACE PROCEDURE "SNAPSHOT_ROLES"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
COMMENT='Captures the snapshot of roles and inserts the records into dbroles'
EXECUTE AS CALLER
AS '
var result = "SUCCESS";
try {
    snowflake.execute( {sqlText: "truncate table DBROLES;"} );
    snowflake.execute( {sqlText: "show roles;"} );
    var dbroles_tbl_sql = ''insert into dbroles select *, CURRENT_TIMESTAMP() from table(result_scan(last_query_id()));'';
    snowflake.execute( {sqlText: dbroles_tbl_sql} );
} 
catch (err) {
    result = "FAILED: Code: " + err.code + "\\n State: " + err.state;result += "\\n Message: " + err.message;result += "\\nStack Trace:\\n" + err.stackTraceTxt;
}
    return result;
';
CREATE OR REPLACE PROCEDURE "SNAPSHOT_USERS"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
COMMENT='Captures the snapshot of users and inserts the records into dbusers'
EXECUTE AS CALLER
AS '
var result = "SUCCESS";
try {
    snowflake.execute( {sqlText: "TRUNCATE TABLE DBUSERS;"} );
    snowflake.execute( {sqlText: "show users;"} );
    var dbusers_tbl_sql = `insert into dbusers select * ,CURRENT_TIMESTAMP() from table(result_scan(last_query_id()));`;
    snowflake.execute( {sqlText: dbusers_tbl_sql} );
} 
catch (err) {
    result = "FAILED: Code: " + err.code + "\\n State: " + err.state;result += "\\n Message: " + err.message;result += "\\nStack Trace:\\n" + err.stackTraceTxt;
}
    return result;
';
alter schema PUBLIC set tag GROUP_A.PUBLIC.VISIBILITY='iuf', TAGS.PUBLIC.GROUP_A_EDIT='PRD', TAGS.PUBLIC.GROUP_A_READONLY='PRD';
