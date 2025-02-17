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
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}


/**
 * This is a simple example of using the mapPartitions
 * method with a HBase connection
 */

object HBaseMapPartitionExample {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("HBaseMapPartitionExample {tableName} is missing an argument")
      return
    }

    val tableName = args(0)

    val sparkConf = new SparkConf().setAppName("HBaseMapPartitionExample " + tableName)
    val sc = new SparkContext(sparkConf)

    try {

      //[(Array[Byte])]
      val rdd = sc.parallelize(Array(
        Bytes.toBytes("1"),
        Bytes.toBytes("2"),
        Bytes.toBytes("3"),
        Bytes.toBytes("4"),
        Bytes.toBytes("5")))

      val conf = HBaseConfiguration.create()

      val hbaseContext = new HBaseContext(sc, conf)

      val b = new StringBuilder

      val getRdd = rdd.hbaseMapPartitions[String](hbaseContext, (it, connection) => {
        val table = connection.getTable(TableName.valueOf(tableName))
        it.map{r =>
          //batching would be faster.  This is just an example
          val result = table.get(new Get(r))

          val it = result.listCells().iterator()

          b.append(Bytes.toString(result.getRow) + ":")

          while (it.hasNext) {
            val cell = it.next()
            val q = Bytes.toString(cell.getQualifierArray)
            if (q.equals("counter")) {
              b.append("(" + q + "," + Bytes.toLong(cell.getValueArray) + ")")
            } else {
              b.append("(" + q + "," + Bytes.toString(cell.getValueArray) + ")")
            }
          }
          b.toString()
        }
      })

      getRdd.collect().foreach(v => println(v))

    } finally {
      sc.stop()
    }
  }
}
