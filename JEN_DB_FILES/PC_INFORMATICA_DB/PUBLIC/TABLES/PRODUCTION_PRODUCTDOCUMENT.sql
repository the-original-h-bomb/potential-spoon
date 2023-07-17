create or replace TABLE PRODUCTION_PRODUCTDOCUMENT (
	PRODUCTID NUMBER(38,0) NOT NULL,
	DOCUMENTNODE BINARY(892) NOT NULL,
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_PRODUCTION_PRODUCTDOCUMENT_PK_PRODUCTDOCUMENT_PRODUCTID_DOCUMENTNODE primary key (DOCUMENTNODE, PRODUCTID)
);