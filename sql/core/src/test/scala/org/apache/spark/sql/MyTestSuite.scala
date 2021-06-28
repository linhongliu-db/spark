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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class MyTestSuite extends QueryTest with SQLTestUtils with SharedSparkSession {
  test("schema change due to spark upgrade, error: incompatible schema change") {
    // scalastyle:off
    val catalog = spark.sessionState.catalog
    withTable("t") {
      withView("v") {
        sql("CREATE TABLE t USING json AS SELECT '1' as a, '20210420' as b")
        sql("CREATE OR REPLACE VIEW v AS SELECT CAST(a AS integer), to_date(b, 'yyyyMMdd') FROM t")
        val meta = catalog.getTableRawMetadata(TableIdentifier("v", Some("default")))
        val prop = meta.properties
        val newProp = prop.map {
          case (key, value) =>
            if (key == "view.query.out.col.1") {
              (key -> "to_date(`t.b`, yyyyMMdd)")
            } else {
              (key -> value)
            }
        }
        val newMeta = meta.copy(properties = newProp)
        catalog.dropTable(TableIdentifier("v", Some("default")), ignoreIfNotExists = false, purge = false)
        catalog.createTable(newMeta, ignoreIfExists = false)
        sql("SELECT * FROM v").show()
      }
    }
  }

  test("schema chagne due to column rename, error: cannot resolve column") {
    withTable("t") {
      withView("v") {
        sql("CREATE TABLE t USING json AS SELECT '1' as a, '20210420' as b")
        sql("CREATE OR REPLACE VIEW v AS SELECT CAST(a AS integer), to_date(b, 'yyyyMMdd') FROM t")
        sql("DROP TABLE t")
        sql("CREATE TABLE t USING json AS SELECT '1' as x, '20210420' as b")
        sql("SELECT * FROM v").show()
      }
    }
  }
}
