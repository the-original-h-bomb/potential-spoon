create or replace TABLE PRODUCTVENDOR_BT (
	PRODUCTID NUMBER(38,0) NOT NULL,
	BUSINESSENTITYID NUMBER(38,0) NOT NULL,
	AVERAGELEADTIME NUMBER(38,0),
	STANDARDPRICE NUMBER(19,4),
	LASTRECEIPTCOST NUMBER(19,4),
	LASTRECEIPTDATE TIMESTAMP_NTZ(3),
	MINORDERQTY NUMBER(38,0),
	MAXORDERQTY NUMBER(38,0),
	ONORDERQTY NUMBER(38,0),
	UNITMEASURECODE VARCHAR(12),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	OPERATION_TYPE VARCHAR(1),
	OPERATION_TIME TIMESTAMP_NTZ(9),
	OPERATION_OWNER VARCHAR(256),
	TRANSACTION_ID VARCHAR(100)
);