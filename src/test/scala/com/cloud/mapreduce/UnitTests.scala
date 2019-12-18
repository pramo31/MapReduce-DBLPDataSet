package com.cloud.mapreduce

import com.cloud.mapreduce.config.MapReduceConfig
import com.cloud.mapreduce.utils.{ConfigReader, ParserUtils, ReadWriteUtils}
import com.typesafe.config.{Config, ConfigException}
import org.scalatest._

import scala.collection.mutable.ArrayBuffer

class UnitTests extends FunSuite {

  test("The config reader reads the value from config file") {
    val config: Config = ConfigReader.readConfig("test")
    val actual = config.getString("TestConfig")
    val expected = "TestValue"
    assert(actual == expected)
  }
  test("Invalid configName throws exceptions") {
    val config: Config = ConfigReader.readConfig("test")
    assertThrows[ConfigException.Missing] {
      config.getString("InvalidConfigName")
    }
  }
  test("Utility function to create each csv row") {
    val a = new ArrayBuffer[String]()
    a.addOne("Key1")
    a.addOne("Key2")
    val b = new ArrayBuffer[String]()
    b.addOne("Value1")
    b.addOne("Value2")
    val actual = ParserUtils.formatCsv(Array("Column1", "Column2"), Array(a, b))
    val expected = new ArrayBuffer[String]()
    expected.addOne("Column1,Column2\n")
    expected.addOne("Key1,Value1\n")
    expected.addOne("Key2,Value2\n")
    (actual zip expected).map { case (actual, expected) =>
      assert(actual == expected)
    }
  }
  test("Pre-formatting the xml before parsing") {
    val rawXml = "<proceedings mdate=\"2019-05-14\" key=\"journals/tlsdkcs/2013-9\">\n<editor>Abdelkader & Hameurlain</editor>\n<editor>Josef K&uuml;ng</editor>\n<editor>Roland R. Wagner</editor>\n<title>Transactions on Large-Scale Data- and Knowledge-Centered Systems IX</title>\n<year>2013</year>\n<publisher>Springer</publisher>\n<series href=\"db/series/lncs/index.html\">Lecture Notes in Computer Science</series>\n<volume>7980</volume>\n<ee>https://doi.org/10.1007/978-3-642-40069-8</ee>\n<isbn>978-3-<642-400>68-1</isbn>\n<booktitle>Trans. Large-Scale Data- and Knowledge-Centered Systems</booktitle>\n<url>db/journals/tlsdkcs/tlsdkcs9.html</url>\n</proceedings>"
    val expected = "<proceedings mdate=\"2019-05-14\" key=\"journals/tlsdkcs/2013-9\"><editor>Abdelkader &amp; Hameurlain</editor><editor>Josef K&amp;uuml;ng</editor><editor>Roland R. Wagner</editor><title>Transactions on Large-Scale Data- and Knowledge-Centered Systems IX</title><year>2013</year><publisher>Springer</publisher><series href=\"db/series/lncs/index.html\">Lecture Notes in Computer Science</series><volume>7980</volume><ee>https://doi.org/10.1007/978-3-642-40069-8</ee><isbn>978-3-&lt;642-400&gt;68-1</isbn><booktitle>Trans. Large-Scale Data- and Knowledge-Centered Systems</booktitle><url>db/journals/tlsdkcs/tlsdkcs9.html</url></proceedings>"
    val actual = ParserUtils.preFormatXml(rawXml)
    assert(actual == expected)
  }
  test("To check the venue Map created from config file") {
    val configObj = new MapReduceConfig
    val venueMap = configObj.venueMap
    val values = Array("journal", "book", "school", "webpage")
    assert(venueMap.size == 8)
    venueMap.values.foreach(value => {
      assert(values.contains(value))
    })
  }
}