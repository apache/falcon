/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.example.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Spark SQL Example.
 */

public final class SparkSQLProcessTable {

    private SparkSQLProcessTable() {
    }
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Arguments must contain details for input or output table");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("SparkSQL example");
        SparkContext sc = new SparkContext(conf);
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

        String sqlQuery = "FROM " +args[2]+"."+args[1]+ " INSERT OVERWRITE TABLE " +args[5]+"."+args[4]
                +" PARTITION("+args[3]+")  SELECT word, SUM(cnt) AS cnt WHERE "+args[0]+" GROUP BY word";

        DataFrame df = sqlContext.sql(sqlQuery);
        df.show();
    }
}

