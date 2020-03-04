package com.uuabc.etl.common.util

import scala.reflect.runtime.universe

object ReflectUtils {
  private lazy val universeMirror = universe.runtimeMirror(getClass.getClassLoader)

  def getCompanion[T](className: String): T = {
    val module = universeMirror.staticModule(className + "$")
    val obj = universeMirror.reflectModule(module)
    obj.instance.asInstanceOf[T]
  }

//  def getCompanion(className: String): IConfigSupported = {
//    val constructors = Class.forName(className + "$").getDeclaredConstructors
//    constructors(0).setAccessible(true)
//    constructors(0).newInstance().asInstanceOf[IConfigSupported]
//  }
}
