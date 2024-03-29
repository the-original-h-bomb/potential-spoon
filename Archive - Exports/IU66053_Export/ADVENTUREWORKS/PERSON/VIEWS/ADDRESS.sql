create or replace secure view ADDRESS(
	SPATIALLOCATION,
	SYS_OPERATION_TYPE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	ROWGUID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	SYS_OPERATION_OWNER WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	ADDRESSLINE1 WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	SYS_OPERATION_TIME WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	MODIFIEDDATE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	POSTALCODE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	ADDRESSID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	SYS_TRANSACTION_ID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	CITY WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	ADDRESSLINE2 WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	STATEPROVINCEID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC
) as
                       select SPATIALLOCATION, SYS_OPERATION_TYPE, ROWGUID, SYS_OPERATION_OWNER, ADDRESSLINE1, SYS_OPERATION_TIME, MODIFIEDDATE, POSTALCODE, ADDRESSID, SYS_TRANSACTION_ID, CITY, ADDRESSLINE2, STATEPROVINCEID
                       from PERSON.ADDRESS_BT;