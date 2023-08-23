create or replace view TAG_PERMISSIONS_ROWS(
	TAG_NAME,
	VALUE
) as 
SELECT p.tag_name, f.value
FROM TAGS.PUBLIC.tags_assigned_permissions p,
   lateral flatten(input => p.schema_permissions) f;