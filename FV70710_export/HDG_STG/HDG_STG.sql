create or replace database HDG_STG;

create or replace schema PUBLIC;

create or replace schema ROLLBACK;

create or replace dynamic table CUSTOMER_FAVORITES_DT(
	NAME,
	FOOD
) lag = '1 minute' warehouse = COMPUTE_WH
 as
select n.name, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;
create or replace TABLE FOODS (
	CODE VARCHAR(255),
	FOOD VARCHAR(255)
);
create or replace TABLE NAMES (
	ID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(255),
	CODE VARCHAR(255),
	primary key (ID)
);
create or replace view CUSTOMER_FAVORITES(
	NAME,
	FOOD
) as

select n.name, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;
create or replace view FOOD_ID(
	ID,
	FOOD
) as

select n.id, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;
create or replace schema SCHEMACHANGE;

create or replace TABLE CHANGE_HISTORY (
	VERSION VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	SCRIPT VARCHAR(16777216),
	SCRIPT_TYPE VARCHAR(16777216),
	CHECKSUM VARCHAR(16777216),
	EXECUTION_TIME NUMBER(38,0),
	STATUS VARCHAR(16777216),
	INSTALLED_BY VARCHAR(16777216),
	INSTALLED_ON TIMESTAMP_LTZ(9)
);