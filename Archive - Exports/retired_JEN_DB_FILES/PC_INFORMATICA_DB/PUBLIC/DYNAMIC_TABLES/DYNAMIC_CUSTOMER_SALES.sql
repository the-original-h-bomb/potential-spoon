create or replace dynamic table DYNAMIC_CUSTOMER_SALES(
	PERSONID,
	CUSTOMERID,
	FIRSTNAME,
	LASTNAME,
	TOTAL,
	TOTAL_PURCHASED_VISITS
) lag = '5 minutes' warehouse = TEST_GROUP_1_WH
 as Select  c.PersonID , c.customerid, p.FirstName , p.LastName , sum(soh.TotalDue) as Total, count(*) as Total_Purchased_visits
From DYNAMIC_SALES_CUSTOMER c
inner join DYNAMIC_SD_PERSON p on c.PersonID = p.BusinessEntityID 
inner join DYNAMIC_SALES_SALESORDERHEADER soh on soh.CustomerID = c.CustomerID 
GROUP BY /*ROLLUP*/  c.PersonID, c.customerid, p.FirstName , p.LastName;