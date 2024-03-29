create or replace TABLE PURCHASING_PURCHASEORDERHEADER (
	PURCHASEORDERID NUMBER(38,0) NOT NULL,
	REVISIONNUMBER NUMBER(38,0),
	STATUS NUMBER(38,0),
	EMPLOYEEID NUMBER(38,0),
	VENDORID NUMBER(38,0),
	SHIPMETHODID NUMBER(38,0),
	ORDERDATE TIMESTAMP_NTZ(3),
	SHIPDATE TIMESTAMP_NTZ(3),
	SUBTOTAL NUMBER(19,4),
	TAXAMT NUMBER(19,4),
	FREIGHT NUMBER(19,4),
	TOTALDUE NUMBER(19,4),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_PURCHASING_PURCHASEORDERHEADER_PK_PURCHASEORDERHEADER_PURCHASEORDERID primary key (PURCHASEORDERID)
);