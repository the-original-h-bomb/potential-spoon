create or replace database VHOL_ST;

create or replace schema PUBLIC;

create or replace TABLE CC_TRANS_ALL (
	CARD_ID VARCHAR(16777216),
	MERCHANT_ID VARCHAR(16777216),
	TRANSACTION_ID VARCHAR(16777216),
	AMOUNT FLOAT,
	CURRENCY VARCHAR(16777216),
	APPROVED BOOLEAN,
	TYPE VARCHAR(16777216),
	TIMESTAMP TIMESTAMP_NTZ(9)
);
create or replace TABLE CC_TRANS_STAGING (
	RECORD_CONTENT VARIANT
);
create or replace view CC_TRANS_STAGING_VIEW(
	CARD_ID,
	MERCHANT_ID,
	TRANSACTION_ID,
	AMOUNT,
	CURRENCY,
	APPROVED,
	TYPE,
	TIMESTAMP
) as (
select
RECORD_CONTENT:card:number::varchar card_id,
RECORD_CONTENT:merchant:id::varchar merchant_id,
RECORD_CONTENT:transaction:id::varchar transaction_id,
RECORD_CONTENT:transaction:amount::float amount,
RECORD_CONTENT:transaction:currency::varchar currency,
RECORD_CONTENT:transaction:approved::boolean approved,
RECORD_CONTENT:transaction:type::varchar type,
RECORD_CONTENT:transaction:timestamp::datetime timestamp
from CC_TRANS_STAGING);
CREATE OR REPLACE PROCEDURE "SIMULATE_KAFKA_STREAM"("MYSTAGE" VARCHAR(16777216), "PREFIX" VARCHAR(16777216), "NUMLINES" NUMBER(38,0))
RETURNS VARCHAR(16777216)
LANGUAGE JAVA
PACKAGES = ('com.snowflake:snowpark:1.8.0')
HANDLER = 'StreamDemo.run'
EXECUTE AS OWNER
AS '
    import com.snowflake.snowpark_java.Session;
    import java.io.*;
    import java.util.HashMap;
    public class StreamDemo {
      public String run(Session session, String mystage,String prefix,int numlines) {
        SampleData SD=new SampleData();
        BufferedWriter bw = null;
        File f=null;
        try {
            f = File.createTempFile(prefix, ".json");
            FileWriter fw = new FileWriter(f);
	        bw = new BufferedWriter(fw);
            boolean first=true;
            bw.write("[");
            for(int i=1;i<=numlines;i++){
                if (first) first = false;
                else {bw.write(",");bw.newLine();}
                bw.write(SD.getDataLine(i));
            }
            bw.write("]");
            bw.close();
            return session.file().put(f.getAbsolutePath(),mystage,options)[0].getStatus();
        }
        catch (Exception ex){
            return ex.getMessage();
        }
        finally {
            try{
	            if(bw!=null) bw.close();
                if(f!=null && f.exists()) f.delete();
	        }
            catch(Exception ex){
	            return ("Error in closing:  "+ex);
	        }
        }
      }

      private static final HashMap<String,String> options = new HashMap<String, String>() {
        { put("AUTO_COMPRESS", "TRUE"); }
      };

      // sample data generator (credit card transactions)
    public static class SampleData {
      private static final java.util.Random R=new java.util.Random();
      private static final java.text.NumberFormat NF_AMT = java.text.NumberFormat.getInstance();
      String[] transactionType={"PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","REFUND"};
      String[] approved={"true","true","true","true","true","true","true","true","true","true","false"};
      static {
        NF_AMT.setMinimumFractionDigits(2);
        NF_AMT.setMaximumFractionDigits(2);
        NF_AMT.setGroupingUsed(false);
      }

      private static int randomQty(int low, int high){
        return R.nextInt(high-low) + low;
      }

      private static double randomAmount(int low, int high){
        return R.nextDouble()*(high-low) + low;
      }

      private String getDataLine(int rownum){
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append("\\"element\\":"+rownum+",")
            .append("\\"object\\":\\"basic-card\\",")
            .append("\\"transaction\\":{")
            .append("\\"id\\":"+(1000000000 + R.nextInt(900000000))+",")
            .append("\\"type\\":"+"\\""+transactionType[R.nextInt(transactionType.length)]+"\\",")
            .append("\\"amount\\":"+NF_AMT.format(randomAmount(1,5000)) +",")
            .append("\\"currency\\":"+"\\"USD\\",")
            .append("\\"timestamp\\":\\""+java.time.Instant.now()+"\\",")
            .append("\\"approved\\":"+approved[R.nextInt(approved.length)]+"")
            .append("},")
            .append("\\"card\\":{")
                .append("\\"number\\":"+ java.lang.Math.abs(R.nextLong()) +"")
            .append("},")
            .append("\\"merchant\\":{")
            .append("\\"id\\":"+(100000000 + R.nextInt(90000000))+"")
            .append("}")
            .append("}");
        return sb.toString();
      }
    }
}
';
create or replace stream CC_TRANS_STAGING_VIEW_STREAM on view CC_TRANS_STAGING_VIEW;
create or replace task GENERATE_TASK
	warehouse=VHOL_WH
	schedule='1 minute'
	COMMENT='Generates simulated real-time data for ingestion'
	as call SIMULATE_KAFKA_STREAM('@VHOL_STAGE','SNOW_',1000000);
create or replace task LOAD_TASK
	warehouse=VHOL_WH
	schedule='1 minute'
	COMMENT='Full Sequential Orchestration'
	as call SIMULATE_KAFKA_STREAM('@VHOL_STAGE','SNOW_',1000000);
create or replace task PROCESS_FILES_TASK
	schedule='3 minute'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	COMMENT='Ingests Incoming Staging Datafiles into Staging Table'
	as copy into CC_TRANS_STAGING from @VHOL_STAGE PATTERN='.*SNOW_.*';
create or replace task PROCESS_FILES_TASK2
	warehouse=VHOL_WH
	after VHOL_ST.PUBLIC.LOAD_TASK
	as copy into CC_TRANS_STAGING from @VHOL_STAGE PATTERN='.*SNOW_.*';
create or replace task REFINE_TASK
	schedule='4 minute'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	COMMENT='2.  ELT Process New Transactions in Landing/Staging Table into a more Normalized/Refined Table (flattens JSON payloads)'
	when SYSTEM$STREAM_HAS_DATA('CC_TRANS_STAGING_VIEW_STREAM')
	as insert into CC_TRANS_ALL (select
card_id, merchant_id, transaction_id, amount, currency, approved, type, timestamp
from CC_TRANS_STAGING_VIEW_STREAM);
create or replace task REFINE_TASK2
	warehouse=VHOL_WH
	after VHOL_ST.PUBLIC.PROCESS_FILES_TASK2
	as insert into CC_TRANS_ALL (select
card_id, merchant_id, transaction_id, amount, currency, approved, type, timestamp
from CC_TRANS_STAGING_VIEW_STREAM);
create or replace task WAIT_TASK
	warehouse=VHOL_WH
	after VHOL_ST.PUBLIC.PROCESS_FILES_TASK2
	as call SYSTEM$wait(1);