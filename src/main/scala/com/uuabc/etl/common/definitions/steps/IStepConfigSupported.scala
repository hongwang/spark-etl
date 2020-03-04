package com.uuabc.etl.common.definitions.steps

import com.typesafe.config.Config

trait IStepConfigSupported {
    def use(config: Config): BaseStep
}
