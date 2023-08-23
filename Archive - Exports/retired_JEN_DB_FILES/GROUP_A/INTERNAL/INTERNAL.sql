create or replace schema INTERNAL;

create or replace tag GROUP_A_EDIT COMMENT='Edit Access to Group A tag'
;
create or replace TABLE TABLE_NAME (
	FIELD VARCHAR(16777216),
	ADDED_DATE TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='testing'
;
alter schema INTERNAL set tag GROUP_A.INTERNAL.GROUP_A_EDIT='edit', TAGS.PUBLIC.GROUP_A_EDIT='PRD';
