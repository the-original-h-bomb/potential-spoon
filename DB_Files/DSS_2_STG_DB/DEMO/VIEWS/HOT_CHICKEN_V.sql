create or replace view HOT_CHICKEN_V(
	STREET_ADDRESS,
	CITY,
	STATE,
	ZIP
) as select * from DEMO.addresses;