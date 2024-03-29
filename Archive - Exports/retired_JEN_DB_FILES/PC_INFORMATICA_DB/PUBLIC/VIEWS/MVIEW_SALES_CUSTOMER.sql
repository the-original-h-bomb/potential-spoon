create or replace materialized view MVIEW_SALES_CUSTOMER(
	HEXID,
	CUSTOMERID,
	PERSONID,
	STOREID,
	TERRITORYID,
	ROWGUID,
	MODIFIEDDATE
) as
SELECT MD5(CUSTOMERID) as HEXID, CUSTOMERID, PERSONID, STOREID, TERRITORYID, ROWGUID, MODIFIEDDATE
FROM PC_INFORMATICA_DB.PUBLIC.SALES_CUSTOMER;