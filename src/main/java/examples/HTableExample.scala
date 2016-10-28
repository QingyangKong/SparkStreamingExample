package examples

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin

/*
 * This is an example testing basic manipulations of HBase in Scala API
 * 
 * A table named 'frankTest' with column family 'cf1' will be checked and created.
 * A record with rowkey 'existed' value 'old_value' will be inserted into the table
 * 
 * compile and run 
 * maven package
 * java -cp {scala-path}:{hbase-path}:jarFilePath examples.HTableExample
 * 
 * 
 * */
object HTableExample {

  private val NPARAMS = 3

  def main(args: Array[String]): Unit = {
    //this is to just certify that record can be read and inserted through htable api
    //api in spark and spark streaming is very similar with methods used in this program
    parseArgs(args)
    //create a hbase configuration and set 3 attributes
    val conf = HBaseConfiguration.create()
    //three params can be found in hbase/conf/core-site.xml and hbase-site.xml
    conf.set("zookeeper.znode.parent", args(0))
    conf.set("hbase.zookeeper.quorum", args(1))
    conf.set("hbase.master", args(2))
    
    val tableName = "frankTest"
    val cf = "cf1"
    val rowKey = "existed"
    val qualifier = "test"
    val value ="qingyangkong_1"
    
    if(checkTable(conf, tableName)){
      dropTable(conf, tableName)
    }
    createTable(conf, tableName, cf)
    putRecord(conf, tableName, rowKey, cf, qualifier, value)
    println(HTableExample.getRecord(conf, tableName, rowKey, cf, qualifier))
  }
  
  def checkTable(conf: Configuration, tableName: String): Boolean = {
    //check if the table exists or not
    val admin = new HBaseAdmin(conf)
    if(admin.tableExists(tableName)){
      admin.close()
      println(tableName + " already exists.")
      return true
    }else{
      println(tableName + " does not exist.")
      admin.close()
      return false
    }
  }
  
  def dropTable(conf: Configuration, tableName: String): Unit = {
    val admin = new HBaseAdmin(conf)
    admin.disableTable(tableName)
    println(tableName + " disabled.")
    admin.deleteTable(tableName)
    println(tableName + " deleted.")
    admin.close()
  }
  
  
  def createTable(conf: Configuration, tableName: String, cf: String): Unit = {
    val htable = new HTableDescriptor(tableName)
    htable.addFamily(new HColumnDescriptor(cf))
    val admin = new HBaseAdmin(conf)
    admin.createTable(htable)
    admin.close()
    println(tableName + " created.")
  }

  //find a record with rowkey, cf and qualifier
  def getRecord(conf: Configuration, tableName: String, rowKey: String, cf: String, qualifier: String): String = {
    val table = new HTable(conf, tableName.getBytes)
    val getOnce = new Get(rowKey.getBytes)
    val res: Result = table.get(getOnce)
    val resBytes: Array[Byte] = res.getValue(cf.getBytes, qualifier.getBytes)
    val resStr: String = Bytes.toString(resBytes)
    table.close()
    return resStr
  }

  //put a record into hbase with rk, cf and qualifier.
  def putRecord(conf: Configuration, tableName: String, rowKey: String, cf: String, qualifier: String, value: String): Unit = {
    val table = new HTable(conf, tableName.getBytes)
    val putOnce = new Put(rowKey.getBytes)
    putOnce.addColumn(cf.getBytes, qualifier.getBytes, value.getBytes)
    table.put(putOnce)
    table.close()
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
