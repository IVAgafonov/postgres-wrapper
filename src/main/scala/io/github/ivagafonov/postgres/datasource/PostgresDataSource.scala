package io.github.ivagafonov.postgres.datasource

import io.github.ivagafonov.postgres.config.PostgresConfig
import org.apache.commons.dbcp2.BasicDataSource

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

case class ResultAndTotal[T](result: Seq[T], total: Long)

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
    getArrayWithParamsAndMap(query)(_ => ())(rs)
  }

  protected def getArrayWithParamsAndMap[T](query: String, fetchSize: Int = 1000)(p: PreparedStatement => Unit)(rs: ResultSet => T): Seq[T] = {
    withConnection(c => {
      val stmt = c.prepareStatement(query)
      stmt.setFetchSize(fetchSize)
      p(stmt)
      processResultSet(stmt.executeQuery)(rs)
    })
  }

  protected def getArrayAndTotalWithParamsAndMap[T](query: String, totalRowName: String, fetchSize: Int = 1000)(p: PreparedStatement => Unit)(rs: ResultSet => T): ResultAndTotal[T] = {
    withConnection(c => {
      val stmt = c.prepareStatement(query)
      stmt.setFetchSize(fetchSize)
      p(stmt)
      processResultSetAndTotal(stmt.executeQuery, totalRowName)(rs)
    })
  }

  protected def getStreamWithParamsAndMap[T](query: String, chunkSize: Int = 100, from: Long = 0)(p: PreparedStatement => Unit)(rs: ResultSet => T): LazyList[Seq[T]] = {
    def getSeq(query: String, chunkSize: Int = 100, from: Long = 0): Seq[T] = {
      withConnection(c => {
        val stmt = c.prepareStatement(query + s" LIMIT $chunkSize OFFSET $from")
        stmt.setFetchSize(chunkSize)
        p(stmt)
        processResultSet(stmt.executeQuery)(rs)
      })
    }
    LazyList.unfold[Seq[T], (Long, Int)](from -> 0) { case (page, lastSelectCount: Int) =>
      if (page != 0 && lastSelectCount < chunkSize) {
        None
      } else {
        val seq = getSeq(query, chunkSize, page * chunkSize)
        Some(seq, page + 1 -> seq.size)
      }
    }
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

  private def withConnection[T](connection: Connection => ResultAndTotal[T]): ResultAndTotal[T] = {
    withConnectionInternal(connection).asInstanceOf[ResultAndTotal[T]]
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
      if (c != null) {
        c.close()
      }
    }
  }

  private def processResultSet[T](rs: ResultSet)(m: ResultSet => T): Seq[T] = {
    val res = new ArrayBuffer[T]()

    while (rs.next()) {
      res.addOne(m(rs))
    }
    res.toSeq
  }

  private def processResultSetAndTotal[T](rs: ResultSet, totalRowName: String)(m: ResultSet => T): ResultAndTotal[T] = {
    val res = new ArrayBuffer[T]()

    var total: Long = 0
    while (rs.next()) {
      if (total == 0) {
        total = rs.getLong(totalRowName)
      }
      res.addOne(m(rs))
    }

    ResultAndTotal(res.toSeq, total)
  }
}
