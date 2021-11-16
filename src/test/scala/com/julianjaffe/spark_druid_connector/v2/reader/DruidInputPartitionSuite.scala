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
import com.julianjaffe.spark_druid_connector.configuration.Configuration
import com.julianjaffe.spark_druid_connector.v2.DruidDataSourceV2TestUtils
import org.scalatest.matchers.should.Matchers


class DruidInputPartitionSuite extends SparkFunSuite with Matchers with DruidDataSourceV2TestUtils {
  test("DruidInputPartition should correctly serialize tasks") {
    val reader = new DruidInputPartition(
      firstSegment, schema, None, None, Configuration.EMPTY_CONF, false, false, false
    ).createPartitionReader()
    reader.next() shouldBe true
    reader.close()
  }

}
