create or replace alert COMPUTE_WH_GIRDLE
	warehouse=SYSADMIN
	schedule='1 minute'
	if (exists(
		select warehouse_name 
        from snowflake.organization_usage.warehouse_metering_history 
        where start_time > current_date() - 30 
        and warehouse_name = 'COMPUTE_WH'
        GROUP BY warehouse_name
        HAVING sum(credits_used) > 7.671431943
	))
	then
	ALTER WAREHOUSE IF EXISTS COMPUTE_WH SUSPEND;