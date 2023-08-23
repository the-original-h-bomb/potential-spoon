create or replace secure view PURCHASEORDERDETAIL(
	PRODUCTID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	UNITPRICE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	MODIFIEDDATE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	PURCHASEORDERID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	OPERATION_OWNER WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	OPERATION_TYPE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	TRANSACTION_ID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	RECEIVEDQTY WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	DUEDATE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	OPERATION_TIME WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	ORDERQTY WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	REJECTEDQTY WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	PURCHASEORDERDETAILID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC
) as
                       select PRODUCTID, UNITPRICE, MODIFIEDDATE, PURCHASEORDERID, OPERATION_OWNER, OPERATION_TYPE, TRANSACTION_ID, RECEIVEDQTY, DUEDATE, OPERATION_TIME, ORDERQTY, REJECTEDQTY, PURCHASEORDERDETAILID
                       from PURCHASING.PURCHASEORDERDETAIL_BT;