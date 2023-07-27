create or replace dynamic table DYNAMIC_VIEW_CUSTOMER_SALES(
	PERSONID,
	CUSTOMERID,
	FIRSTNAME,
	LASTNAME,
	TOTAL,
	TOTAL_PURCHASED_VISITS
) lag = '5 minutes' warehouse = COMPUTE_WH
 as Select  c.PersonID , c.customerid, p.FirstName , p.LastName , sum(soh.TotalDue) as Total, count(*) as Total_Purchased_visits
From VIEW_SALES_CUSTOMER c
inner join VIEW_SD_PERSON p on c.PersonID = p.BusinessEntityID 
inner join VIEW_SALES_SALESORDERHEADER soh on soh.CustomerID = c.CustomerID 
GROUP BY /*ROLLUP*/  c.PersonID, c.customerid, p.FirstName , p.LastName;