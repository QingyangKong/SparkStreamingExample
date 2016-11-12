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
 * This is example of Scala API for basic manipulations of Hbase table.
 * This example contains sample codes to Create, disable and Drop hbase tables by hbaseAdmin.
 * In addition, Get and Put records into the Hbase Table using HTable API.
 * */

object HTableExample_1 {
  private val NPARAMS = 3

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    
    //create a hbase configuration and set 3 attributes
    val conf = HBaseConfiguration.create()
    conf.set("zookeeper.znode.parent", args(0))
    conf.set("hbase.zookeeper.quorum", args(1))
    conf.set("hbase.master", args(2))
    
    val tableName = "frankTest"
    val cf = "cf1"
    
    //drop table if the table exists
    if(checkTable(conf, tableName)){
      dropTable(conf, tableName)
    }
    
    //create the table
    createTable(conf, tableName, cf)
    
    /* 
     * Put 3 records into the table created before and put 3 records.
     * After put , the table should be like:
     * 
     * record_1 {ID => "1", Name => "recordName_1", TNum => "111", Type => "TYPE1", Value => "111Value"}
     * record_2 {ID => "2", Name => "recordName_2", TNum => "222", Type => "TYPE2", Value => "222Value"}
     * record_3 {ID => "3", Name => "recordName_3", TNum => "333", Type => "TYPE3", Value => "333Value"}
     * */
    putRecord(conf, tableName, "record_11", cf, "ID", "1")
    putRecord(conf, tableName, "record_11", cf, "Name", "recordName_1")
    putRecord(conf, tableName, "record_11", cf, "TNum", "111")
    putRecord(conf, tableName, "record_11", cf, "Type", "TYPE1")
    putRecord(conf, tableName, "record_11", cf, "Value", "111Value")
    
    putRecord(conf, tableName, "record_12", cf, "ID", "2")
    putRecord(conf, tableName, "record_12", cf, "Name", "recordName_2")
    putRecord(conf, tableName, "record_12", cf, "TNum", "222")
    putRecord(conf, tableName, "record_12", cf, "Type", "TYPE2")
    putRecord(conf, tableName, "record_12", cf, "Value", "222Value")
    
    putRecord(conf, tableName, "record_13", cf, "ID", "3")
    putRecord(conf, tableName, "record_13", cf, "Name", "recordName_3")
    putRecord(conf, tableName, "record_13", cf, "TNum", "333")
    putRecord(conf, tableName, "record_13", cf, "Type", "TYPE3")
    putRecord(conf, tableName, "record_13", cf, "Value", "333Value")
    
    //get the record with tableName, rowkey and cf:qualifier and the result should be 1
    println("ID with rowKey record_11 is: " + HTableExample.getRecord(conf, tableName, "record_11", cf, "ID"))
    
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