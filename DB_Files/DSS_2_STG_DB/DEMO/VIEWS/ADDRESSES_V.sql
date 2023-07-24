create or replace view ADDRESSES_V(
	STREET_ADDRESS,
	CITY,
	STATE,
	ZIP
) as select * from DEMO.addresses;