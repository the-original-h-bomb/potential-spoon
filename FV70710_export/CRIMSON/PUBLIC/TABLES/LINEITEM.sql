create or replace TABLE LINEITEM (
	L_ORDERKEY NUMBER(38,0),
	L_PARTKEY NUMBER(38,0),
	L_SUPPKEY NUMBER(38,0),
	L_LINENUMBER NUMBER(38,0),
	L_QUANTITY NUMBER(12,2),
	L_EXTENDEDPRICE NUMBER(12,2),
	L_DISCOUNT NUMBER(12,2),
	L_TAX NUMBER(12,2),
	L_RETURNFLAG VARCHAR(1),
	L_LINESTATUS VARCHAR(1),
	L_SHIPDATE DATE,
	L_COMMITDATE DATE,
	L_RECEIPTDATE DATE,
	L_SHIPINSTRUCT VARCHAR(25),
	L_SHIPMODE VARCHAR(10),
	L_COMMENT VARCHAR(44)
);