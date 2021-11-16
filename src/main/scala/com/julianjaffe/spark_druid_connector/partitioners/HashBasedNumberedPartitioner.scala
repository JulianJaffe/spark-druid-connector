/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.julianjaffe.spark_druid_connector.partitioners

import com.google.common.hash.Hashing
import com.julianjaffe.spark_druid_connector.partitioners.HashBasedNumberedPartitioner.generatePartitionMap
import com.julianjaffe.spark_druid_connector.MAPPER
import org.apache.druid.java.util.common.ISE
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, greatest, lit, max, row_number}
import org.apache.spark.sql.types.IntegerType

import scala.collection.JavaConverters.seqAsJavaListConverter

class HashBasedNumberedPartitioner(df: DataFrame) extends PartitionMapProvider with Serializable {
  private var partitionMap = Map[Int, Map[String, String]]()
  private val HashFunction = Hashing.murmur3_32()

  def partition(
                 tsCol: String,
                 tsFormat: String,
                 segmentGranularity: String,
                 rowsPerPartition: Long,
                 partitionColsOpt: Option[Seq[String]],
                 shouldRollUp: Boolean = true
               ): DataFrame = {
    // If partition dimensions are not provided, assume all columns except __time are dimensions
    val partitionColumns = partitionColsOpt.getOrElse(df.schema.fieldNames.toSeq.filterNot(_ == tsCol))
    val partitionColsString = partitionColsOpt.map(_.mkString(","))

    val rankColWindow = if (shouldRollUp) {
      dense_rank().over(Window.partitionBy(_timeBucketCol).orderBy(partitionColumns.map(col): _*))
    } else {
      row_number().over(timeBucketWindowSpec(tsCol))
    }
    val bucketedDf = df
      .withColumn(_timeBucketCol, SparkUdfs.bucketRow(col(tsCol), lit(tsFormat), lit(segmentGranularity)))
      .withColumn(_rankCol, rankColWindow)
      .withColumn(_partitionNumCol,
        greatest( // Taking the max of partitionNum and 1 to handle the case where numRows < rowsPerPartition
          max(_rankCol)
            .over(timeBucketWindowSpec(tsCol)
              .rangeBetween(Window.currentRow, Window.unboundedFollowing))
            .divide(lit(rowsPerPartition))
            .cast(IntegerType),
          lit(1)))

    val schemaWithHashKey = bucketedDf.schema.add(_partitionKeyCol, IntegerType, nullable = false)
    val bucketedDfWithKeys = bucketedDf.map({ row: Row =>
      val groupKey = MAPPER.writeValueAsBytes(row.getValuesMap(partitionColumns).values.toSeq.asJava)
      val hashKey = Math.abs(HashFunction.hashBytes(groupKey).asInt() % row.getInt(row.fieldIndex(_partitionNumCol)))

      Row.fromSeq(row.toSeq :+ hashKey)
    }, RowEncoder(schemaWithHashKey))

    val partitionedDf = bucketedDfWithKeys.repartition(col(_timeBucketCol), col(_partitionKeyCol))
    partitionMap = generatePartitionMap(partitionedDf, partitionColsString)
    partitionedDf.drop(_timeBucketCol, _rankCol, _partitionNumCol, _partitionKeyCol)
  }

  override def getPartitionMap: Map[Int, Map[String, String]] = {
    if (partitionMap.isEmpty) {
      throw new ISE("Must call rangePartition() to partition the dataframe before calling getPartitionMap!")
    }
    partitionMap
  }
}

object HashBasedNumberedPartitioner {
  def generatePartitionMap(
                            partitionedDf: DataFrame,
                            partitionCols: Option[String]
                          ): Map[Int, Map[String, String]] = {
    val bucketMapping = partitionedDf.rdd.mapPartitions { rowIterator =>
      if (rowIterator.hasNext) {
        val sparkPartitionId = TaskContext.getPartitionId()
        val head = rowIterator.next()
        val timeBucket = head.getLong(head.fieldIndex(_timeBucketCol))
        val druidPartitionId = head.getInt(head.fieldIndex(_partitionKeyCol))
        val numPartitions = head.getInt(head.fieldIndex(_partitionNumCol))
        Iterator(HashBasedNumberedPartitionInfo(timeBucket, druidPartitionId, sparkPartitionId, numPartitions))
      } else {
        Iterator.empty
      }
    }.collect().groupBy(_.timeBucket)

    // We need to "pack" the partition ids and total partitions to handle the case where we have gaps in our
    // populated partitions.
    bucketMapping.values.flatMap{partitions =>
      val numPopulatedPartitions = partitions.length.toString
      partitions.sortBy(_.druidPartition).zipWithIndex.map{case(partition, index) =>
        val partitionColsMap = Seq(partitionCols.map("partitionDimension" -> _)).flatten.toMap

        partition.sparkPartition -> (Map[String, String](
          "partitionId" -> index.toString,
          "numPartitions" -> numPopulatedPartitions,
          "bucketId" -> partition.druidPartition.toString,
          "numBuckets" -> partition.numPartitions.toString
        ) ++ partitionColsMap)
      }
    }.toMap
  }

  private case class HashBasedNumberedPartitionInfo(
                                                     timeBucket: Long,
                                                     druidPartition: Int,
                                                     sparkPartition: Int,
                                                     numPartitions: Int
                                                   )
}
