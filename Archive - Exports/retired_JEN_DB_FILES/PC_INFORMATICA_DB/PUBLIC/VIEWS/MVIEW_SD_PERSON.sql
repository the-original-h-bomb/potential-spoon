create or replace materialized view MVIEW_SD_PERSON(
	HEXID,
	RANDOMVAR,
	BUSINESSENTITYID,
	PERSONTYPE,
	NAMESTYLE,
	TITLE,
	FIRSTNAME,
	MIDDLENAME,
	LASTNAME,
	SUFFIX,
	EMAILPROMOTION,
	ADDITIONALCONTACTINFO,
	DEMOGRAPHICS,
	ROWGUID,
	MODIFIEDDATE,
	SYS_OPERATION_TYPE,
	SYS_OPERATION_TIME,
	SYS_OPERATION_OWNER,
	SYS_TRANSACTION_ID
) as
SELECT MD5(src.ROWGUID) as HEXID
    ,  EMAILPROMOTION + 1 as RANDOMVAR,
    BUSINESSENTITYID, PERSONTYPE, NAMESTYLE, TITLE, FIRSTNAME, MIDDLENAME, LASTNAME, SUFFIX, EMAILPROMOTION, ADDITIONALCONTACTINFO, DEMOGRAPHICS, ROWGUID, MODIFIEDDATE, SYS_OPERATION_TYPE, SYS_OPERATION_TIME, SYS_OPERATION_OWNER, SYS_TRANSACTION_ID
    FROM PC_INFORMATICA_DB.PUBLIC.ADWKS_SD_PERSON src;