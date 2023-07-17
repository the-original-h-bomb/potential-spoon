create or replace TABLE SALES_CURRENCYRATE (
	CURRENCYRATEID NUMBER(38,0) NOT NULL,
	CURRENCYRATEDATE TIMESTAMP_NTZ(3),
	FROMCURRENCYCODE VARCHAR(12),
	TOCURRENCYCODE VARCHAR(12),
	AVERAGERATE NUMBER(19,4),
	ENDOFDAYRATE NUMBER(19,4),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_SALES_CURRENCYRATE_PK_CURRENCYRATE_CURRENCYRATEID primary key (CURRENCYRATEID)
);