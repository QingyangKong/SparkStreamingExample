package examples
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.CompareFilter

object HTableExample_2 {
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
    
    //scan the HTable with custom filters
    //select qualifier when (ID = 1 && Name = recordName_1), and result should be  111Value
    val colsList_1 = List(("cf1", "ID"), ("cf1", "Name"), ("cf1", "TNum"), ("cf1", "Type"), ("cf1", "Value"))
    val conditionList = List(("cf1", "ID", "1"), ("cf1", "Name", "recordName_1"))
    scanRecordsWithFilter(conf, tableName, colsList_1, conditionList)
    
    //scan the HTable with a range
    //select qualifier when rowkey >= "record_11" && rowkey < "record_12"
    val colsList_2 = List(("cf1", "ID"), ("cf1", "Name"), ("cf1", "TNum"), ("cf1", "Type"), ("cf1", "Value"))
    scanRecordsWithStartingAndEndingRecords(conf, tableName, colsList_2, "record_11", "record_12")
  }
  
  
  /*
   * In this method, singleColumnValueFilter is used to filter scan result by multiple qualifiers
   * Although the method is flexible, it is very slow. 
   * If only used for once, it is OK but it may not efficient to apply the filter a large scale of data.
   * */
  def scanRecordsWithFilter(conf: Configuration, 
      tableName: String,
      colsList: List[(String, String)],
      conditionsList: List[(String, String, String)]): Unit = {
    //create filters
    
    val filters = conditionsList.map{case (cf: String, qualifier: String, value: String) => {
      new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(qualifier), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value))
    }}
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filters.foreach { filterList.addFilter }
    
    val scanOnce = new Scan
    scanOnce.setFilter(filterList)
    colsList.foreach{case(cf: String, qualifier: String) => scanOnce.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier))}

    //create HTable
    val table = new HTable(conf: Configuration, tableName.getBytes)
    
    val resultScanner = table.getScanner(scanOnce)
    var result = resultScanner.next()
    
    println("Results:")
    while(result != null){
      colsList.foreach{
        case(cf: String, value: String)=> print(s"$cf:$value -> " + Bytes.toString(result.getValue(cf.getBytes, value.getBytes)) + "|")
        }
      result = resultScanner.next()
    }
    resultScanner.close()
    table.close()
  }
  
  
  /*
   * In this method, staring and ending point of the scan are defined in this Scan instantiation
   * Because rowkeys in the hbase table are saved lexicographically, performance can be improved greatly by using starting and ending point.
   * When process large scale of data, this method is better in my view. 
   * */
  def scanRecordsWithStartingAndEndingRecords(
      conf: Configuration, 
      tableName: String,
      colsList: List[(String, String)],
      startRecord: String, 
      endRecord: String): Unit = {
    val scanOnce = new Scan(startRecord.getBytes, endRecord.getBytes)
    //please be notified, startRecord is included while endRecord is not
    
    colsList.map{case(cf: String, qualifier: String) => scanOnce.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier))}
    
    val table = new HTable(conf: Configuration, tableName.getBytes)
    
    val resultScanner = table.getScanner(scanOnce)
    var result = resultScanner.next()
    
    println("results: ")
    while(result != null){
      colsList.foreach{
        case (cf: String, value: String) => print(s"$cf:$value ->" + Bytes.toString(result.getValue(cf.getBytes, value.getBytes)) + "|")
        }
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