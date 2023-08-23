create or replace secure view CONTACTTYPE(
	SYS_TRANSACTION_ID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	MODIFIEDDATE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	SYS_OPERATION_TIME WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_TIMESTAMP_NTZ,
	CONTACTTYPEID WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_NUMERIC,
	NAME WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	SYS_OPERATION_OWNER WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING,
	SYS_OPERATION_TYPE WITH MASKING POLICY ADVENTUREWORKS.PUBLIC.ADVENTUREWORKS_MASKING_POLICY_STRING
) as
                       select SYS_TRANSACTION_ID, MODIFIEDDATE, SYS_OPERATION_TIME, CONTACTTYPEID, NAME, SYS_OPERATION_OWNER, SYS_OPERATION_TYPE
                       from PERSON.CONTACTTYPE_BT;