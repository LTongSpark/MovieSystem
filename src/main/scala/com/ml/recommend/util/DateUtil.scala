package com.ml.recommend.util

import org.apache.commons.lang3.time.FastDateFormat

/**
  * @author LTong
  * @date 2019-06-18 下午 1:53
  */
object DateUtil {

  val TARGET_TIME_FORMAT = FastDateFormat.getInstance("yyyyMM")

  def parse(timestamp:Int) ={
      TARGET_TIME_FORMAT.format((timestamp.toLong)*1000l)
  }


}
