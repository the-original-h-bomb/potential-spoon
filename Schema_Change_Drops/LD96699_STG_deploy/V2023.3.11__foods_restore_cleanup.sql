drop table rollback.foods;
create or replace table rollback.foods clone rollback.foods_1;
drop table rollback.foods_1;
