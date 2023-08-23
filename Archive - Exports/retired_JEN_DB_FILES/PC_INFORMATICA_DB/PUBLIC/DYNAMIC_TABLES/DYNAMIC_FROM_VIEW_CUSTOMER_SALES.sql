create or replace dynamic table DYNAMIC_FROM_VIEW_CUSTOMER_SALES(
	PERSONID,
	CUSTOMERID,
	FIRSTNAME,
	LASTNAME,
	TOTAL,
	TOTAL_PURCHASED_VISITS
) lag = '1 minute' warehouse = COMPUTE_WH
 as Select  c.PersonID , c.customerid, c.FirstName , c.LastName , c.Total, c.Total_Purchased_visits
From PC_INFORMATICA_DB.PUBLIC.VIEW_CUSTOMER_SALES c;