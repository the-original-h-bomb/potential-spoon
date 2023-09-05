create or replace dynamic table FOOD_ID_DT(
	ID,
	FOOD
) lag = '1 minute' warehouse = COMPUTE_WH
 as
select n.id, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;