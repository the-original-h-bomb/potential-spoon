create or replace schema INTERNAL;

create or replace tag GROUP_A_EDIT COMMENT='Edit Access to Group A tag'
;
create or replace tag STG_GROUP_A_EDIT COMMENT='Edit Access to Stage Group A tag'
;
create or replace tag STG_GROUP_A_VIEW COMMENT='View Access to Stage Group A tag'
;
alter schema INTERNAL set tag TAGS.PUBLIC.GROUP_A_EDIT='STG';
