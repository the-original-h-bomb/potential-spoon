create or replace dynamic table CUSTOMER_FAVORITES_DT(
	NAME,
	FOOD
) lag = '1 minute' warehouse = COMPUTE_WH
 as
select n.name, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;