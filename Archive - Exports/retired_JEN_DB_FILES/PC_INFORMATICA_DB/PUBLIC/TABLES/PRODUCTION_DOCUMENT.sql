create or replace TABLE PRODUCTION_DOCUMENT (
	DOCUMENTNODE BINARY(892) NOT NULL,
	TITLE VARCHAR(200),
	OWNER NUMBER(38,0),
	FOLDERFLAG BOOLEAN,
	FILENAME VARCHAR(1600),
	FILEEXTENSION VARCHAR(32),
	REVISION VARCHAR(20),
	CHANGENUMBER NUMBER(38,0),
	STATUS NUMBER(38,0),
	DOCUMENTSUMMARY VARCHAR(16777216) COLLATE 'utf8',
	DOCUMENT BINARY(8388608),
	ROWGUID VARCHAR(36),
	MODIFIEDDATE TIMESTAMP_NTZ(3),
	constraint PK_PRODUCTION_DOCUMENT_PK_DOCUMENT_DOCUMENTNODE primary key (DOCUMENTNODE)
);