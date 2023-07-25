create table DEMO.ADDRESSES
(
    STREET_ADDRESS VARCHAR(100),
    CITY           VARCHAR(100),
    STATE          VARCHAR(100),
    ZIP            NUMBER
);

create table DEMO.ADDRESSES_REDUX
(
    STREET_ADDRESS VARCHAR(100),
    CITY           VARCHAR(100),
    STATE          VARCHAR(100),
    ZIP            NUMBER
);

create table DEMO.DATAGRIP_TABLE
(
    COLUMN_2      NUMBER comment 'and one more',
    BUILDING_CODE VARCHAR comment 'it builds the code on this mess as I go!'
)
    comment = 'oh I can add a comment';

create table DEMO.FOGGY_TABLE
(
    FIRST_NAME VARCHAR,
    LAST_NAME  VARCHAR,
    AGE        NUMBER,
    NICKNAME   NUMBER
);

create table DEMO.HELLO_WORLD
(
    FIRST_NAME VARCHAR,
    LAST_NAME  VARCHAR,
    AGE        NUMBER
);

create table DEMO.NAMES_T
(
    FIRST_NAME VARCHAR,
    LAST_NAME  VARCHAR,
    NICK_NM    VARCHAR(100)
);

create table DEMO.NEW_TABLE
(
    COLUMN1 NUMBER
);

create table DEMO.PROCEDURES_T
(
    PROCEDURE_CATALOG        VARCHAR,
    PROCEDURE_SCHEMA         VARCHAR,
    PROCEDURE_NAME           VARCHAR,
    PROCEDURE_OWNER          VARCHAR,
    ARGUMENT_SIGNATURE       VARCHAR,
    DATA_TYPE                VARCHAR,
    CHARACTER_MAXIMUM_LENGTH NUMBER,
    CHARACTER_OCTET_LENGTH   NUMBER,
    NUMERIC_PRECISION        NUMBER,
    NUMERIC_PRECISION_RADIX  NUMBER(2),
    NUMERIC_SCALE            NUMBER,
    PROCEDURE_LANGUAGE       VARCHAR,
    PROCEDURE_DEFINITION     VARCHAR,
    CREATED                  TIMESTAMPLTZ(3),
    LAST_ALTERED             TIMESTAMPLTZ(3),
    COMMENT                  VARCHAR
);

create table DEMO.TEST
(
    NOPE NUMBER
);

create table DEMO.TESTING_CHANGES
(
    POTATO1 NUMBER,
    POTATO2 NUMBER
);

create or replace view ADDRESSES_V(
	STREET_ADDRESS,
	CITY,
	STATE,
	ZIP
) as select * from DEMO.addresses;

create or replace view HOT_CHICKEN_V(
	STREET_ADDRESS,
	CITY,
	STATE,
	ZIP
) as select * from DEMO.addresses;

