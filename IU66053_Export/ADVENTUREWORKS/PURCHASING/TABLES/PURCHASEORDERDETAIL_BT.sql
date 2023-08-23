create or replace TABLE PURCHASEORDERDETAIL_BT (
	PURCHASEORDERID NUMBER(38,0) NOT NULL,
	PURCHASEORDERDETAILID NUMBER(38,0) NOT NULL,
	DUEDATE TIMESTAMP_NTZ(3),
	ORDERQTY NUMBER(38,0),
	PRODUCTID NUMBER(38,0),
	UNITPRICE NUMBER(19,4),
	RECEIVEDQTY NUMBER(8,2),
	REJECTEDQTY NUMBER(8,2),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	OPERATION_TYPE VARCHAR(1),
	OPERATION_TIME TIMESTAMP_NTZ(9),
	OPERATION_OWNER VARCHAR(256),
	TRANSACTION_ID VARCHAR(100)
);