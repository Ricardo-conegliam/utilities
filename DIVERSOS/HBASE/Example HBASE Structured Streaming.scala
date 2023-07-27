// Databricks notebook source
//hbase
dbutils.widgets.text("HBASE_SITE_READ", "/local_disk0/hbase/conf/hbase-local-non-kerberos-site.xml", "HBASE-SITE File - Read")
val hbase_site_read = dbutils.widgets.get("HBASE_SITE_READ")

//hbase tables
dbutils.widgets.text("HBASE_CLIENTE", "clientes", "HBASE Clientes Activos Table")
dbutils.widgets.text("HBASE_CLIENTE_CF", "cf", "HBASE Clientes Activos CF")
val hbase_table_clientes = dbutils.widgets.get("HBASE_CLIENTE")
val hbase_table_clientes_cf = dbutils.widgets.get("HBASE_CLIENTE_CF")

//slack
dbutils.widgets.text("SLACK_URL", "https://hooks.slack.com/services/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", "Slack Notification URL") // slack url
val slack_url = dbutils.widgets.get("SLACK_URL")

// COMMAND ----------

// prepare - load nemonicos table and cache it.
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.fs.Path

// load hbase-site for read tables
@transient lazy val hbase_read_conf = HBaseConfiguration.create()
hbase_read_conf.addResource(new Path(hbase_site_read))

// creates hbase context
val hbase_context_read = new HBaseContext(spark.sparkContext, hbase_read_conf)

// COMMAND ----------

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Scan.ReadType
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes

case class ClientOutput(key:String, n1:String, n2:String, n3:String, n4:String, n5:String, n6:String, n7:String, n8:String, n9:String)

// creates hbase connection
@transient lazy val hbase_read_conn = ConnectionFactory.createConnection(hbase_read_conf)
@transient lazy val actCliTable    = hbase_read_conn.getTable(TableName.valueOf(hbase_table_clientes))

def lookup_clientes (accountNumber:String): ClientOutput = {
  try {
    // setup hbase scan with the accout number as a key
    val scan = new Scan(Bytes.toBytes(accountNumber))
    scan.setRowPrefixFilter(Bytes.toBytes(accountNumber))
    val results: ResultScanner = actCliTable.getScanner(scan)
    var r: Result = results.next()  
                            
    if(r == null){
      ClientOutput(accountNumber, "", "", "", "", "", "", "", "", "")
    } else {                
        val n1  = Bytes.toString(r.getValue(Bytes.toBytes(hbase_table_clientes_cf), Bytes.toBytes("ope_cop_orn")))
        val n2  = Bytes.toString(r.getValue(Bytes.toBytes(hbase_table_clientes_cf), Bytes.toBytes("per_nom")))
        val n3  = Bytes.toString(r.getValue(Bytes.toBytes(hbase_table_clientes_cf), Bytes.toBytes("per_ape_ptn")))
        val n4  = Bytes.toString(r.getValue(Bytes.toBytes(hbase_table_clientes_cf), Bytes.toBytes("per_ape_mtn")))
        //... continue here

        ClientOutput(accountNumber, n1, n2, n3, n4, "", "", "", "", "")
  /* TODO - potential risk
  
     original code = 2 levels of lists:
       iterator (list of rows), our code is a udf -> operates line by line.
       resultset from hbase can, our takes the first element.
         - if the hbase record set has more than one, we'll be discard lines.
         - to fix, we need to output an array of JNLOutput objects.
  */
    }
  } catch {
    case e:java.io.FileNotFoundException => null
  }
}

val lookup_clientesUDF = spark.udf.register("lookup_clientes", lookup_clientes _)

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def fraude_lookup_clientes(df: DataFrame): DataFrame = {
  df.withColumn("enrichedEvent", lookup_clientesUDF('columna_de_client_id))
}

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

/* streaming parameters */
// set if running concurrently - see https://docs.databricks.com/spark/latest/structured-streaming/production.html
// spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
val trigger = Trigger.ProcessingTime("1 second")
val query_name = "eq2_basic_jnl"
val checkpoint_location = "dbfs:/tmp/eq2_checkpoint"
dbutils.fs.rm(checkpoint_location, true)

val stream = spark
  .readStream
  .format("delta")
  //.table("")
  .load()
  //.transform(format_output)
  .transform(fraude_lookup_clientes)
  .writeStream
  .format("noop")
  //.foreachBatch(fraude_send_to_slack _)
  .option("checkpointLocation", checkpoint_location)
  .trigger(trigger)
  .queryName(query_name)
  .start()

