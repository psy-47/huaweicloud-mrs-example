/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.bigdata.spark.examples.hbasecontext

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}


/**
 * This is a simple example of using the foreachPartition
 * method with a HBase connection
 */

object HBaseForEachPartitionExample {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("HBaseForeachPartitionExample {tableName} {columnFamily} are missing an arguments")
      return
    }

    val tableName = args(0)
    val columnFamily = args(1)

    val sparkConf = new SparkConf().setAppName("HBaseForeachPartitionExample " +
      tableName + " " + columnFamily)
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

      val conf = HBaseConfiguration.create()

      val hbaseContext = new HBaseContext(sc, conf)


      rdd.hbaseForeachPartition(hbaseContext,
        (it, connection) => {
          val m = connection.getBufferedMutator(TableName.valueOf(tableName))

          it.foreach(r => {
            val put = new Put(r._1)
            r._2.foreach((putValue) =>
              put.addColumn(putValue._1, putValue._2, putValue._3))
            m.mutate(put)
          })
          m.flush()
          m.close()
        })

    } finally {
      sc.stop()
    }
  }
}
