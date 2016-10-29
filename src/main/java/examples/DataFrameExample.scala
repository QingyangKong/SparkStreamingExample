package examples
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType

/*
 * This is to get HBase Table using mapreduce in spark
 * method: newAPIHadoopRDD
 * 
 * */
object DataFrameExample {
  
  private val NPARAMS = 3
  
  def main(args: Array[String]): Unit = {
    
    parseArgs(args)
    
    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", args(0))
    conf.set("hbase.zookeeper.quorum", args(1))
    conf.set("hbase.master", args(2))

    val tableName = "frankTest"

    
    val resDF = getRDDFromHBaseTable(sc, sqlContext, conf, tableName)
    resDF.show()
  }

  def getRDDFromHBaseTable(
    sc: SparkContext,
    sqlContext: SQLContext,
    conf: Configuration,
    tableName: String): DataFrame = {

    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    
    val rawRDD = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val tupleRDD = rawRDD.map(tuple => (
      Bytes.toString(tuple._1.get),
      Bytes.toString(tuple._2.getValue("cf1".getBytes, "Id".getBytes)),
      Bytes.toString(tuple._2.getValue("cf1".getBytes, "Name".getBytes)),
      Bytes.toString(tuple._2.getValue("cf1".getBytes, "TNum".getBytes)),
      Bytes.toString(tuple._2.getValue("cf1".getBytes, "Type".getBytes)),
      Bytes.toString(tuple._2.getValue("cf1".getBytes, "Value".getBytes))))
      
    val rowRDD = tupleRDD.map(tuple => Row(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, tuple._6))
    
    val rowKey = StructField("RK", StringType)
    val colId = StructField("ID", StringType)
    val colName = StructField("Name", StringType)
    val colTNum = StructField("TNum", StringType)
    val colType = StructField("Type", StringType)
    val colvalue = StructField("Value", StringType)
    
    val tableStruct = StructType(Seq(rowKey, colId, colName, colTNum, colType, colvalue))
    
    val df = sqlContext.createDataFrame(rowRDD, tableStruct)
    df
  }
  
  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      printUsage
      System.exit(1)
    }
  }
  
  private def printUsage(): Unit = {
    val usage: String = "HTable Example\n" +
      "\n" +
      "Usage: HTableExample\n" +
      "\n" +
      "args-1: zookeeper.znode.parent (string)\n" +
      "args-2: hbase.zookeeper.quorum (string)\n"+
      "args-3: hbase.master - (string)\n"+
      "they can be found in configuration file: hbase-site.xml"

    println(usage)
  }
}