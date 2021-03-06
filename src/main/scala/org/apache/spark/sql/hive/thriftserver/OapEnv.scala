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

package org.apache.spark.sql.hive.thriftserver

import java.io.PrintStream

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.oap.listener.OapListener
import org.apache.spark.sql.oap.ui.OapTab
import org.apache.spark.util.Utils

/**
 * Most of the code in init() are copied from SparkSQLEnv. Please include code from the
 * corresponding Spark version.
 */
private[spark] object OapEnv extends Logging {
  logDebug("Initializing Oap Env")

  var sqlContext: SQLContext = _
  var sparkContext: SparkContext = _
  var sparkSession: SparkSession = _

  var initialized: Boolean = false

  def init(): Unit = synchronized {
    if (!initialized && !Utils.isTesting) {
      if (sqlContext == null) {
        val sparkConf = new SparkConf(loadDefaults = true)
        // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
        // the default appName [SparkSQLCLIDriver] in cli or beeline.
        val maybeAppName = sparkConf
          .getOption("spark.app.name")
          .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
          .filterNot(_ == classOf[HiveThriftServer2].getName)

        sparkConf.setAppName(maybeAppName.getOrElse(s"SparkSQL::${Utils.localHostName()}"))

        val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
        sparkContext = sparkSession.sparkContext
        sqlContext = sparkSession.sqlContext

        val metadataHive = sparkSession
          .sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]
          .client.newSession()
        metadataHive.setOut(new PrintStream(System.out, true, "UTF-8"))
        metadataHive.setInfo(new PrintStream(System.err, true, "UTF-8"))
        metadataHive.setError(new PrintStream(System.err, true, "UTF-8"))
        sparkSession.conf.set("spark.sql.hive.version", HiveUtils.builtinHiveVersion)
      }

      sparkContext.addSparkListener(new OapListener)

      SparkSQLEnv.sparkContext = sparkContext
      SparkSQLEnv.sqlContext = sqlContext
      this.sparkSession = sqlContext.sparkSession

      sparkContext.ui.foreach(new OapTab(_))
      initialized = true
    }
  }

  // This is to enable certain OAP features, like UI, in non-Spark SQL CLI/ThriftServer conditions
  def initWithoutCreatingOapSession(): Unit = synchronized {
    if (!initialized && !Utils.isTesting) {
      val sc = SparkContext.getOrCreate()
      sc.addSparkListener(new OapListener)
      this.sparkSession = SparkSession.getActiveSession.get
      sc.ui.foreach(new OapTab(_))
      initialized = true
    }
  }

  /** Cleans up and shuts down the Spark SQL environments. */
  def stop() {
    SparkSQLEnv.stop()
  }
}
