--tables
create table rollback.names clone hdg_dev.rollback.names;
create table rollback.foods clone hdg_dev.rollback.foods;

--views
create or replace view ROLLBACK.CUSTOMER_FAVORITES(
	NAME,
	FOOD
) as

select n.name, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;

create or replace view ROLLBACK.FOOD_ID(
	ID,
	FOOD
) as

select n.id, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;

--dynamic tables
create or replace dynamic table ROLLBACK.CUSTOMER_FAVORITES_DT(
	NAME,
	FOOD
) lag = '1 minute' warehouse = COMPUTE_WH
 as
select n.name, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;

create or replace dynamic table HDG_DEV.ROLLBACK.FOOD_ID_DT(
	ID,
	FOOD
) lag = '1 minute' warehouse = COMPUTE_WH
 as
select n.id, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;