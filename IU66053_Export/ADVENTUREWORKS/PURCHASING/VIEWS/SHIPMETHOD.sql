create or replace secure view SHIPMETHOD(
	OPERATION_TYPE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	MODIFIEDDATE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	SHIPBASE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	SHIPMETHODID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	SHIPRATE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	OPERATION_OWNER WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	OPERATION_TIME WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	NAME WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	TRANSACTION_ID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	ROWGUID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING
) as
                       select OPERATION_TYPE, MODIFIEDDATE, SHIPBASE, SHIPMETHODID, SHIPRATE, OPERATION_OWNER, OPERATION_TIME, NAME, TRANSACTION_ID, ROWGUID
                       from PURCHASING.SHIPMETHOD_BT;