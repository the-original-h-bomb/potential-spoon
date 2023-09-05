create or replace TABLE SALES_SALESORDERDETAIL (
	SALESORDERID NUMBER(38,0) NOT NULL,
	SALESORDERDETAILID NUMBER(38,0) NOT NULL,
	CARRIERTRACKINGNUMBER VARCHAR(100),
	ORDERQTY NUMBER(38,0),
	PRODUCTID NUMBER(38,0),
	SPECIALOFFERID NUMBER(38,0),
	UNITPRICE NUMBER(19,4),
	UNITPRICEDISCOUNT NUMBER(19,4),
	ROWGUID VARCHAR(36),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_SALES_SALESORDERDETAIL_PK_SALESORDERDETAIL_SALESORDERID_SALESORDERDETAILID primary key (SALESORDERDETAILID, SALESORDERID)
);