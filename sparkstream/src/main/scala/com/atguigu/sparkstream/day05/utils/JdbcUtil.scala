package com.atguigu.sparkstream.day05.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

import scala.collection.mutable.ListBuffer

object JdbcUtil {

  private val datasource: DataSource = init()

  def init(): DataSource = {
    val config: Properties = PropertiesUtil.load("wordcount.properties")
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }

  def getconnection: Connection ={
    datasource.getConnection
  }

  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Unit = {
    var preparedStatement: PreparedStatement = null
    try {

      preparedStatement = connection.prepareStatement(sql)

      for (i <- params.indices) {
        preparedStatement.setObject(i + 1, params(i))
      }
      preparedStatement.executeUpdate()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {
    var result: Long = 0L
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  def getBlackList(connection: Connection): List[String] = {


    val blackList = new ListBuffer[String]

    val statement: PreparedStatement = connection.prepareStatement("select userid from black_list")

    val resultSet: ResultSet = statement.executeQuery()

    while (resultSet.next()) {
      blackList += resultSet.getString(1)
    }
    resultSet.close()
    statement.close()

    blackList.toList
  }
}
