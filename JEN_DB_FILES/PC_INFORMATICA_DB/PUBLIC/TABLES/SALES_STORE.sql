create or replace TABLE SALES_STORE (
	BUSINESSENTITYID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(200),
	SALESPERSONID NUMBER(38,0),
	DEMOGRAPHICS VARCHAR(16777216) COLLATE 'utf8',
	ROWGUID VARCHAR(36),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_SALES_STORE_PK_STORE_BUSINESSENTITYID primary key (BUSINESSENTITYID)
);