package com.hcdlearning.common.steps

import org.apache.spark.sql._

abstract class BaseStep() {
    def execute(): DataFrame
}