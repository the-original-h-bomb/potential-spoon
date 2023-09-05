create or replace view FOOD_ID(
	ID,
	FOOD
) as

select n.id, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;