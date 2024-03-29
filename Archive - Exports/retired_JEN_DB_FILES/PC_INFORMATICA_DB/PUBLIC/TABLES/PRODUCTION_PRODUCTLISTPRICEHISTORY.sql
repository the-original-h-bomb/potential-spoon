create or replace TABLE PRODUCTION_PRODUCTLISTPRICEHISTORY (
	PRODUCTID NUMBER(38,0) NOT NULL,
	STARTDATE TIMESTAMP_NTZ(3) NOT NULL,
	ENDDATE TIMESTAMP_NTZ(3),
	LISTPRICE NUMBER(19,4),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_PRODUCTION_PRODUCTLISTPRICEHISTORY_PK_PRODUCTLISTPRICEHISTORY_PRODUCTID_STARTDATE primary key (PRODUCTID, STARTDATE)
);