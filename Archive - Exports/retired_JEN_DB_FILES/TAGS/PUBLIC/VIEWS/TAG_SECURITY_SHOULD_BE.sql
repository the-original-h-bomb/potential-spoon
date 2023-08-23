create or replace view TAG_SECURITY_SHOULD_BE(
	TAG_NAME,
	ROLE_NEEDED,
	SCHEMA_NEEDED,
	VALUE
) as 
 select p.tag_name, r.value as role_needed, s.value as schema_needed, tpr.value 
     from TAGS.PUBLIC.tag_assignments p
      left outer join TAGS.PUBLIC.tag_permissions_rows tpr on tpr.tag_name = p.tag_name,
      lateral flatten(input => p.role_list) r,     
  lateral flatten(input => p.schema_list) s;