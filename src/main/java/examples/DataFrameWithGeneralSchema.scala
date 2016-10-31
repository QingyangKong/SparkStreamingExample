package examples
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.HBaseConfiguration

object DataFrameWithGeneralSchema {
  
  val NPARAMS = 3

  val TableSchema_Test: Array[(String, String, DataType)] = Array(
    ("cf1", "ID", StringType),
    ("cf1", "Name", StringType),
    ("cf1", "TNum", IntegerType),
    ("cf1", "Type", StringType),
    ("cf1", "Value", DoubleType))

  def main(args: Array[String]): Unit = {
    
    parseArgs(args)
    
    val sparkConf = new SparkConf
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val conf = HBaseConfiguration.create
    conf.set("zookeeper.znode.parent", args(0))
    conf.set("hbase.zookeeper.quorum", args(1))
    conf.set("hbase.master", args(2))

    val tableName = "frankTest"

    val df = getDataFrameByTableName(sc, sqlContext, conf, tableName, TableSchema_Test)
    df.show()
  }

  //fetch data form HBase as RDD[(ImmutableBytesWritable, Result)]
  private def getResultRDD(sc: SparkContext, sqlContext: SQLContext, conf: Configuration, tableName: String) = {
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    val rawFormat =
      sc.newAPIHadoopRDD(
        conf,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result])
    rawFormat
  }

  //convert RDD[(ImmutableBytesWritable, Result)] to RDD[Row]
  private def formatRDD(rdd: RDD[(ImmutableBytesWritable, Result)], tableSchema: Array[(String, String, DataType)]): RDD[Row] = {
    val res = rdd.map {
      tuple =>
        (
          Bytes.toString(tuple._1.get),
          tableSchema.map(cfCol => cfCol._3 match {
            case StringType  => Bytes.toString(tuple._2.getValue(cfCol._1.getBytes, cfCol._2.getBytes))
            case IntegerType => (Bytes.toString(tuple._2.getValue(cfCol._1.getBytes, cfCol._2.getBytes))).toInt
            case DoubleType  => (Bytes.toString(tuple._2.getValue(cfCol._1.getBytes, cfCol._2.getBytes))).toDouble
            case _           => null
          }))
    }.map {
      tuple => Array(tuple._1) ++ (tuple._2)
    }.map { arr => Row.fromSeq(arr) }
    res
  }

  //Create DataFrame with schema
  private def applySchemaOnRDD(sqlContext: SQLContext, rdd: RDD[Row], schema: Array[(String, String, DataType)]): DataFrame = {
    val fields = StructField("rk", StringType) +: schema.map(trasferToStructField)
    val struct = StructType(Seq(fields: _*))
    val formattedDF = sqlContext.createDataFrame(rdd, struct)
    formattedDF
  }

  private def trasferToStructField(tuple: (String, String, DataType)): StructField = {
    StructField(tuple._2, tuple._3)
  }

  protected def getDataFrameByTableName(
    sc: SparkContext,
    sqlContext: SQLContext,
    conf: Configuration,
    tableName: String,
    tableSchema: Array[(String, String, DataType)]): DataFrame = {
    val rawRDD = getResultRDD(sc, sqlContext, conf, tableName)
    val formattedRDD = formatRDD(rawRDD, tableSchema)
    val formattedDF = applySchemaOnRDD(sqlContext, formattedRDD, tableSchema)
    formattedDF
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