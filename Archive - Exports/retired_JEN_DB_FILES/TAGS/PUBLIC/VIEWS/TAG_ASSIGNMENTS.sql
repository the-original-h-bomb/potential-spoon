create or replace view TAG_ASSIGNMENTS(
	TAG_NAME,
	ROLE_LIST,
	DB_LIST,
	SCHEMA_LIST
) as Select distinct t.tag_name, role_info.role_list, db_info.db_list,  schema_info.schema_list
  From 
     SNOWFLAKE.ACCOUNT_USAGE.TAGS t
     full outer join
      (select ARRAY_AGG(object_name) WITHIN GROUP (ORDER BY object_name ASC) as role_list,  tag_name as role_tag_name 
          from SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
          where domain = 'ROLE'
          group by  tag_name
      ) role_info on t.tag_name = role_info.role_tag_name
     FULL outer join (select ARRAY_AGG(object_name) WITHIN GROUP (ORDER BY object_name ASC) as db_list,  tag_name as db_tag_name 
        from SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
        where domain = 'DATABASE'
        group by  tag_name
      ) as db_info on db_info.db_tag_name = t.tag_name
      FULL  outer join (select ARRAY_AGG( distinct concat(object_database,'.',object_name)) WITHIN GROUP (ORDER BY  concat(object_database,'.',object_name) ASC) as schema_list,   tag_name 
          from SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
          where domain = 'SCHEMA'
          group by tag_name
      ) schema_info on schema_info.tag_name = t.tag_name
      order by tag_name;