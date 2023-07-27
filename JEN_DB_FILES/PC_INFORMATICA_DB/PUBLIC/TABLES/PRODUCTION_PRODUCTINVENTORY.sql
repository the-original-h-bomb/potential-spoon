create or replace TABLE PRODUCTION_PRODUCTINVENTORY (
	PRODUCTID NUMBER(38,0) NOT NULL,
	LOCATIONID NUMBER(38,0) NOT NULL,
	SHELF VARCHAR(40),
	BIN NUMBER(38,0),
	QUANTITY NUMBER(38,0),
	ROWGUID VARCHAR(36),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_PRODUCTION_PRODUCTINVENTORY_PK_PRODUCTINVENTORY_PRODUCTID_LOCATIONID primary key (LOCATIONID, PRODUCTID)
);