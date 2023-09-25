create or replace dynamic table ROLLBACK.CUSTOMER_FAVORITES_DT(
	NAME,
	FOOD
) lag = '1 minute' warehouse = WH_HEATHER
 as
select n.name, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;

create or replace dynamic table ROLLBACK.FOOD_ID_DT(
	ID,
	FOOD
) lag = '1 minute' warehouse = COMPUTE_WH
 as
select n.id, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;
