create or replace view ADDRESSES_TO_DELETE_V(
	STREET_ADDRESS,
	CITY,
	STATE,
	ZIP
) as select * from DEMO.addresses;