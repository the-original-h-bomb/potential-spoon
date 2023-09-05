create or replace database HDG_DEV;

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
create or replace dynamic table FOOD_ID_DT(
	ID,
	FOOD
) lag = '1 minute' warehouse = COMPUTE_WH
 as
select n.id, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;
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