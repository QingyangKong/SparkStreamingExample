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

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter

/*
 * This is to test write and read hbase record from a Scala API
 * 
 * a table named 'frankTest' needs to be created in HBase before
 * command to create table: create 'frankTest', 'cf1'
 * 
 * compile and run 
 * java -cp {scala-path}:{hbase-path}:jarFilePath examples.HTableExample
 * */
object HTableExample {

  private val NPARAMS = 3

  def main(args: Array[String]): Unit = {
    //this is to just certify that record can be read and inserted through htable api
    //api in spark and spark streaming is very similar with methods used in this program
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
    
    //scan the HTable with custom filters
    //select qualifier when (ID = 1 && Name = recordName_1), and result should be  111Value
    scanRecordsWithFilter(conf, "frankTest")
    
    //scan the HTable with a range
    //select qualifier when rowkey >= "record_11" && rowkey < "record_12"
    scanRecordsWithStartingAndEndingRecords(conf, "frankTest", "record_11", "record_12")
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
  
  /*
   * In this method, singleColumnValueFilter is used to filter scan result by multiple qualifiers
   * But this method is very slow. If only used for once, it is OK but it is not a good idea to use this method on massive records
   * */
  def scanRecordsWithFilter(conf: Configuration, tableName: String): Unit = {
    //create filters
    val f1 = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("ID"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1"))
    val f2 = new SingleColumnValueFilter(Bytes.toBytes("cf1"), Bytes.toBytes("Name"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("recordName_1"))
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filterList.addFilter(f1)
    filterList.addFilter(f2)
    
    val scanOnce = new Scan
    scanOnce.setFilter(filterList)
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ID"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Name"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("TNum"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Type"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Value"))
    
    //create HTable
    val table = new HTable(conf: Configuration, tableName.getBytes)
    
    val resultScanner = table.getScanner(scanOnce)
    var result = resultScanner.next()
    
    while(result != null){
      println("valuw with ID=1 and Name=recordName_1 is: " + Bytes.toString(result.getValue("cf1".getBytes, "Value".getBytes)))
      result = resultScanner.next()
    }
    resultScanner.close()
    table.close()
  }
  
  /*
   * In this method, staring and ending point of the scan are defined in this Scan instantiation
   * Because rowkeys in the hbase table are saved lexicographically, by using starting and ending point, performance can be improved greatly.
   * When process large scale of data, this method is better. 
   * */
  def scanRecordsWithStartingAndEndingRecords(conf: Configuration, tableName: String, startRecord: String, endRecord: String): Unit = {
    val scanOnce = new Scan(startRecord.getBytes, endRecord.getBytes)
    //please be notified, startRecord is included while endRecord is not
    
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ID"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Name"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("TNum"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Type"))
    scanOnce.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("Value"))
    val table = new HTable(conf: Configuration, tableName.getBytes)
    
    val resultScanner = table.getScanner(scanOnce)
    var result = resultScanner.next()
    
    while(result != null){
      println(s"value with rowkey from $startRecord to $endRecord is: " + Bytes.toString(result.getValue("cf1".getBytes, "Value".getBytes)))
      result = resultScanner.next()
    }
    resultScanner.close()
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
