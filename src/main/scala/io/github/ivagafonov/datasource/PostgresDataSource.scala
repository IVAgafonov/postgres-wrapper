package io.github.ivagafonov.datasource

import io.github.ivagafonov.config.PostgresConfig
import org.apache.commons.dbcp2.BasicDataSource

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal


trait PostgresDataSource {

  protected val config: PostgresConfig

  private val ds = new BasicDataSource()

  ds.setUrl(s"jdbc:postgresql://${config.host}:${config.port}/${config.database}")
  ds.setUsername(config.user)
  ds.setPassword(config.password)

  ds.setInitialSize(config.poolInitialSize)
  ds.setMinIdle(config.poolInitialSize)
  ds.setMaxIdle(config.poolInitialSize)
  ds.setMaxTotal(config.poolMaxSize)

  protected def getArrayAndMap[T](query: String)(rs: ResultSet => T): Seq[T] = {
    withConnection(c => {
      val stmt = c.createStatement()
      stmt.setFetchSize(1000)
      processResultSet(stmt.executeQuery(query))(rs)
    })
  }

  protected def getArrayWithParamsAndMap[T](query: String)(p: PreparedStatement => Unit)(rs: ResultSet => T): Seq[T] = {
    withConnection(c => {
      val stmt = c.prepareStatement(query)
      stmt.setFetchSize(1000)
      p(stmt)
      processResultSet(stmt.executeQuery)(rs)
    })
  }

  protected def updateWithParams(query: String)(p: PreparedStatement => Unit): Int = {
    withConnection(c => {
      val stmt = c.prepareStatement(query)
      p(stmt)
      stmt.executeUpdate()
    })
  }

  protected def updateWithParamsAndMap[T](query: String)(p: PreparedStatement => Unit)(rs: ResultSet => T = null): Seq[T] = {
    withConnection(c => {
      val stmt = c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)
      p(stmt)
      val affectedRows = stmt.executeUpdate()
      if (affectedRows > 0 && rs != null) {
        processResultSet(stmt.getGeneratedKeys)(rs)
      } else {
        Seq.empty
      }
    })
  }

  private def withConnection[T](connection: Connection => Seq[T]): Seq[T] = {
    withConnectionInternal(connection).asInstanceOf[Seq[T]]
  }

  private def withConnection(connection: Connection => Int): Int = {
    withConnectionInternal(connection).asInstanceOf[Int]
  }

  private def withConnectionInternal(connection: Connection => Any): Any = {
    var c: Connection = null;

    try {
      c = ds.getConnection
      connection(c)
    } catch {
      case NonFatal(e) =>
        throw e
    } finally {
      closeConnection(c)
    }
  }

  private def processResultSet[T](rs: ResultSet)(m: ResultSet => T): Seq[T] = {
    val res = new ArrayBuffer[T]()

    while (rs.next()) {
      res.addOne(m(rs))
    }
    res.toSeq
  }

  private def closeConnection(c: Connection): Unit = {
    if (c != null) {
      c.close()
    }
  }
}
