package examples
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.SubstringComparator
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.filter.CompareFilter


/* This example shows 3 types of filters that can be used in HBase scan
 * 1. (FilterList + singleColumnValueFilter): to specify values for qualifiers, this kind of filter can be used to filter records by qualifiers 
 * 2. (Filterlist + RowFilter): to filter records sharing the same pattern as part of rowKey
 * 3. scan(startRow, endRow): to filter records sharing the same pattern but nothing can exist before pattern in rowkey.
 * */

object HTableExample_2 {
  
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
    val colsList = List(("cf1", "ID"), ("cf1", "Name"), ("cf1", "TNum"), ("cf1", "Type"), ("cf1", "Value"))
    
    /*
     * In this example, records saved in the hbase table:
     *  cf1:ID -> 1|cf1:Name -> recordName_1|cf1:TNum -> 111|cf1:Type -> TYPE1|cf1:Value -> 111Value|
		 * 	cf1:ID -> 2|cf1:Name -> recordName_2|cf1:TNum -> 222|cf1:Type -> TYPE2|cf1:Value -> 222Value|
		 * 	cf1:ID -> 3|cf1:Name -> recordName_3|cf1:TNum -> 333|cf1:Type -> TYPE3|cf1:Value -> 333Value|
     * */
    
    
    /* filter 1:
     * select qualifier when (ID = 1 && Name = recordName_1), so only first one would be returned 
     * result: 
     * 	cf1:ID -> 1|cf1:Name -> recordName_1|cf1:TNum -> 111|cf1:Type -> TYPE1|cf1:Value -> 111Value|
     * 
     * */    
    val conditionList = List(("cf1", "ID", "1"), ("cf1", "Name", "recordName_1"))
    scanWithColumnFilter(conf, tableName, colsList, conditionList)
    
    /* filter 2:
     * select records whose rowkey contains pattern "record_1", so all of 3 records would be returned
     * result:
     * 	cf1:ID -> 1|cf1:Name -> recordName_1|cf1:TNum -> 111|cf1:Type -> TYPE1|cf1:Value -> 111Value|
		 * 	cf1:ID -> 2|cf1:Name -> recordName_2|cf1:TNum -> 222|cf1:Type -> TYPE2|cf1:Value -> 222Value|
		 * 	cf1:ID -> 3|cf1:Name -> recordName_3|cf1:TNum -> 333|cf1:Type -> TYPE3|cf1:Value -> 333Value|
     * */
    scanWithRowFilter(conf, tableName, colsList, "record_1")
    
    /* filter 3:
     * select records whose rowkey from "record_11" to "record_13"(record_13 is not included), so first 2 records would be returned
     * result:
     * 	cf1:ID -> 1|cf1:Name -> recordName_1|cf1:TNum -> 111|cf1:Type -> TYPE1|cf1:Value -> 111Value|
		 * 	cf1:ID -> 2|cf1:Name -> recordName_2|cf1:TNum -> 222|cf1:Type -> TYPE2|cf1:Value -> 222Value|
     * 
     * */
    scanWithRowkeyRange(conf, tableName, colsList, "record_11", "record_13")
  }
  
  
  /* In this method, singleColumnValueFilter is used to filter scan result by multiple qualifiers
   * Although the method is flexible, it is very slow. 
   * If only used for once, it is OK but it may not efficient to apply the filter a large scale of data.
   * */
  def scanWithColumnFilter(conf: Configuration, 
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
    val table = new HTable(conf, tableName.getBytes)
    
    val resultScanner = table.getScanner(scanOnce)
    var result = resultScanner.next()
    
    println("Results:")
    while(result != null){
      colsList.foreach{
        case(cf: String, qualifier: String)=> print(s"$cf:$qualifier -> " + Bytes.toString(result.getValue(cf.getBytes, qualifier.getBytes)) + "|")
        }
      println
      result = resultScanner.next()
    }
    resultScanner.close()
    table.close()
  }
  
  /* This way is much better because filtering by rowkey.
   * But performance is not good enough. 
   * Because the pattern cannot be promised at start of rowkeys, I guess logics nuder hood still search all records in Hbase table
   * */  
  def scanWithRowFilter(conf: Configuration, 
      tableName: String, 
      colsList: List[(String, String)],
      pattern: String): Unit = {
    
    val rowKeyFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(pattern))
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    filterList.addFilter(rowKeyFilter)
    
    
    val scanOnce = new Scan
    scanOnce.setFilter(filterList)
    colsList.map{case(cf:String, qualifier: String) => 
      scanOnce.addColumn(cf.getBytes, qualifier.getBytes)
      }
    
    val table = new HTable(conf, tableName.getBytes)
    
    val resultScanner = table.getScanner(scanOnce)
    var result = resultScanner.next()
    
    println("Result:")
    while(result != null){
      colsList.foreach{
        case(cf: String, qualifier: String) => print(s"$cf:$qualifier -> " + Bytes.toString(result.getValue(cf.getBytes, qualifier.getBytes)) + "|")
      }
      println
      result = resultScanner.next()
    }
  }
  
  
  /*
   * In this method, staring and ending point of the scan are defined in this Scan instantiation
   * Because rowkeys in the hbase table are saved lexicographically, performance can be improved greatly by using starting and ending point.
   * When process large scale of data, this method is the best in my view. 
   * */
  
  /*
   * Order of rowkeys:
   * rule: rowKey will be compared character by character and underscore is greater than numbers
   * 			 I guess they are compared by ASCII
   * eg: 1 < 1_1 < 1__
   * */
  def scanWithRowkeyRange(
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
        case (cf: String, value: String) => print(s"$cf:$value -> " + Bytes.toString(result.getValue(cf.getBytes, value.getBytes)) + "|")
        }
      println
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