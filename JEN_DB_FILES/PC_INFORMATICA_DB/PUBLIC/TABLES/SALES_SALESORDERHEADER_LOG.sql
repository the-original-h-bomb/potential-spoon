create or replace TABLE SALES_SALESORDERHEADER_LOG (
	OP_XID VARCHAR(100),
	OP_CODE VARCHAR(1),
	OP_CMT_SCN NUMBER(20,0),
	OP_CMT_TIME TIMESTAMP_NTZ(9),
	OP_NUM_IN_TX NUMBER(20,0),
	OP_KEY_LEVEL NUMBER(6,0),
	OP_ROOT_KEY_ROWID VARCHAR(100),
	OPERATION_OWNER VARCHAR(100),
	SALESORDERID_OLD NUMBER(38,0),
	SALESORDERID_NEW NUMBER(38,0),
	REVISIONNUMBER_OLD NUMBER(38,0),
	REVISIONNUMBER_NEW NUMBER(38,0),
	ORDERDATE_OLD TIMESTAMP_NTZ(3),
	ORDERDATE_NEW TIMESTAMP_NTZ(3),
	DUEDATE_OLD TIMESTAMP_NTZ(3),
	DUEDATE_NEW TIMESTAMP_NTZ(3),
	SHIPDATE_OLD TIMESTAMP_NTZ(3),
	SHIPDATE_NEW TIMESTAMP_NTZ(3),
	STATUS_OLD NUMBER(38,0),
	STATUS_NEW NUMBER(38,0),
	ONLINEORDERFLAG_OLD BOOLEAN,
	ONLINEORDERFLAG_NEW BOOLEAN,
	PURCHASEORDERNUMBER_OLD VARCHAR(100),
	PURCHASEORDERNUMBER_NEW VARCHAR(100),
	ACCOUNTNUMBER_OLD VARCHAR(60),
	ACCOUNTNUMBER_NEW VARCHAR(60),
	CUSTOMERID_OLD NUMBER(38,0),
	CUSTOMERID_NEW NUMBER(38,0),
	SALESPERSONID_OLD NUMBER(38,0),
	SALESPERSONID_NEW NUMBER(38,0),
	TERRITORYID_OLD NUMBER(38,0),
	TERRITORYID_NEW NUMBER(38,0),
	BILLTOADDRESSID_OLD NUMBER(38,0),
	BILLTOADDRESSID_NEW NUMBER(38,0),
	SHIPTOADDRESSID_OLD NUMBER(38,0),
	SHIPTOADDRESSID_NEW NUMBER(38,0),
	SHIPMETHODID_OLD NUMBER(38,0),
	SHIPMETHODID_NEW NUMBER(38,0),
	CREDITCARDID_OLD NUMBER(38,0),
	CREDITCARDID_NEW NUMBER(38,0),
	CREDITCARDAPPROVALCODE_OLD VARCHAR(15),
	CREDITCARDAPPROVALCODE_NEW VARCHAR(15),
	CURRENCYRATEID_OLD NUMBER(38,0),
	CURRENCYRATEID_NEW NUMBER(38,0),
	SUBTOTAL_OLD NUMBER(19,4),
	SUBTOTAL_NEW NUMBER(19,4),
	TAXAMT_OLD NUMBER(19,4),
	TAXAMT_NEW NUMBER(19,4),
	FREIGHT_OLD NUMBER(19,4),
	FREIGHT_NEW NUMBER(19,4),
	COMMENT_OLD VARCHAR(512),
	COMMENT_NEW VARCHAR(512),
	ROWGUID_OLD VARCHAR(36),
	ROWGUID_NEW VARCHAR(36),
	MODIFIEDDATE_OLD TIMESTAMP_NTZ(3),
	MODIFIEDDATE_NEW TIMESTAMP_NTZ(3)
);