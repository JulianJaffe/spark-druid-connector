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

package com.julianjaffe.spark_druid_connector.v2.reader

import com.julianjaffe.spark_druid_connector.SparkFunSuite
import com.julianjaffe.spark_druid_connector.configuration.SerializableHadoopConfiguration
import org.apache.spark.sql.catalyst.InternalRow

class DruidInputPartitionReaderSuite extends SparkFunSuite with InputPartitionReaderBehaviors[InternalRow] {

  private val hadoopConf =
    () => sparkContext.broadcast(new SerializableHadoopConfiguration(sparkContext.hadoopConfiguration))
  private val sutName = "DruidInputPartitionReader"

  // Run InputPartitionReader tests for DruidInputPartitionReader
  testsFor(inputPartitionReader(
    sutName,
    hadoopConf,
    new DruidInputPartitionReader(_, _, _, _, _, _, _, _, _),
    partitionReaderToSeq
  ))
}
