package org.tribbloid.ispark.display

import org.apache.spark.sql.DataFrame
import org.tribbloid.ispark.display.dsl.Display.Table

/**
 * Created by peng on 4/9/15.
 */
package object dsl {

  implicit class DataFrameView(self: DataFrame) {
    def displayTable = Table(self)
  }
}