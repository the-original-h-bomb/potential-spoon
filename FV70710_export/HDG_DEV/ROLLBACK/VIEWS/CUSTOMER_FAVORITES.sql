create or replace view CUSTOMER_FAVORITES(
	NAME,
	FOOD
) as

select n.name, f.food from rollback.names n inner join rollback.foods f on n.code = f.code;