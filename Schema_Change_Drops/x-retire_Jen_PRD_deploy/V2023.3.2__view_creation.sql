create or replace view DEMO.ADDRESSES_V(
	STREET_ADDRESS,
	CITY,
	STATE,
	ZIP
) as select * from DEMO.addresses;

create or replace view DEMO.HOT_CHICKEN_V(
	STREET_ADDRESS,
	CITY,
	STATE,
	ZIP
) as select * from DEMO.addresses;

