create or replace TABLE SALES_COUNTRYREGIONCURRENCY (
	COUNTRYREGIONCODE VARCHAR(12) NOT NULL,
	CURRENCYCODE VARCHAR(12) NOT NULL,
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_SALES_COUNTRYREGIONCURRENCY_PK_COUNTRYREGIONCURRENCY_COUNTRYREGIONCODE_CURRENCYCODE primary key (COUNTRYREGIONCODE, CURRENCYCODE)
);