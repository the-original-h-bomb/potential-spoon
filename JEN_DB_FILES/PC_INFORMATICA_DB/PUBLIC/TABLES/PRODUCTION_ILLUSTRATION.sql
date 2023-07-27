create or replace TABLE PRODUCTION_ILLUSTRATION (
	ILLUSTRATIONID NUMBER(38,0) NOT NULL,
	DIAGRAM VARCHAR(16777216) COLLATE 'utf8',
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_PRODUCTION_ILLUSTRATION_PK_ILLUSTRATION_ILLUSTRATIONID primary key (ILLUSTRATIONID)
);