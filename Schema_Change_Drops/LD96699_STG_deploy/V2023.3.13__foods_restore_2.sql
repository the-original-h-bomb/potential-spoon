create or replace table rollback.foods_1 clone rollback.foods at(timestamp => '2023-09-25 12:27:44.862 -0700'::timestamp_tz); 
drop table rollback.foods;
create or replace table rollback.foods clone rollback.foods_1;
drop table rollback.foods_1;
