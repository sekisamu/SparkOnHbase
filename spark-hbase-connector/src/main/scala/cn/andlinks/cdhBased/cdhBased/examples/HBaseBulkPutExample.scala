package cn.andlinks.cdhBased.cdhBased.examples

/**
  * Created by hammer on 2017/5/16.
  */

import java.util.ArrayList

import cn.andlinks.cdhBased.cdhBased.hbasetwo.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.conf._
import org.apache.hadoop.hbase.client.HTable._
import scala.collection.mutable.ArrayBuffer

object HBaseBulkPutExample {

  def generateArray(number:Int,columnFamily:String):
  Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] ={

    val list = for( i <- 1 to number) yield (Bytes.toBytes(i.toString),
                                               Array((Bytes.toBytes(columnFamily),
                                                      Bytes.toBytes(i.toString),
                                                      Bytes.toBytes(i.toString))))
    list.toArray

  }


  def main(args: Array[String]) {
//    if (args.length == 0) {
//      System.out.println("HBaseBulkPutExample {tableName} {columnFamily}");
//      return;
//    }

    val tablename = "testcdh"
    val columnFamily = "info"

    val sparkConf = new SparkConf()
      .setAppName("HBaseBulkPutExample " + tablename + " " + columnFamily).setMaster("local")
    val sc = new SparkContext(sparkConf)

    try {
      //[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])]
      val rdd = sc.parallelize(Array(
        (Bytes.toBytes("1"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
        (Bytes.toBytes("2"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
        (Bytes.toBytes("3"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
        (Bytes.toBytes("4"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
        (Bytes.toBytes("5"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))
      ))

      val conf1 = new Configuration();
      conf1.set("hbase.zookeeper.quorum","andlinksmaster,andlinksslave1,andlinksslave2")
      conf1.set("hbase.zookeeper.property.clientPort","2181")
      //conf1.set("hbase.master", "andlinksslave2:60000")
      conf1.set("zookeeper.znode.parent", "/hbase-unsecure")

      val conf = HBaseConfiguration.create(conf1)


      val hbaseContext = new HBaseContext(sc, conf)
      hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
        TableName.valueOf(tablename),
        (putRecord) => {
          val put = new Put(putRecord._1)
          putRecord._2.foreach((putValue) =>
            put.addColumn(putValue._1, putValue._2, putValue._3))
          put
        })

    } finally {

      sc.stop()
    }
  }
}