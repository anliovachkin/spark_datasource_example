package org.example.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.example.datasource.postgres.ConnectionProperties.connectionProperties

import java.sql.DriverManager
import java.util
import scala.collection.JavaConverters._
import scala.math.min


class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = new PostgresTable(properties.get("tableName")) // TODO: Error handling
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("user_id", LongType)
}

case class ConnectionProperties(url: String, user: String, password: String, tableName: String,
                                partitionSize: Long)

object ConnectionProperties {
  def connectionProperties(options: CaseInsensitiveStringMap): ConnectionProperties =
    ConnectionProperties(options.get("url"), options.get("user"), options.get("password"),
      options.get("tableName"), options.getOrDefault("partitionSize", "0").toLong)
}

/** Read */

class PostgresScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new PostgresScan(connectionProperties(options))
}

case class PostgresPartition(from: Long, to: Long) extends InputPartition

class PostgresScan(connectionProperties: ConnectionProperties) extends Scan with Batch {
  override def readSchema(): StructType = PostgresTable.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val connection = DriverManager.getConnection(
      connectionProperties.url, connectionProperties.user, connectionProperties.password
    )
    val statement = connection.createStatement()
    val resultSet = statement
      .executeQuery(s"SELECT COUNT(*) FROM ${connectionProperties.tableName}")
    resultSet.next()
    val count = resultSet.getLong("count")
    val partitionSize = connectionProperties.partitionSize
    for (i <- (0L until count by partitionSize).toArray)
      yield PostgresPartition(i, min(i + partitionSize, count))
  }

  override def createReaderFactory(): PartitionReaderFactory = new PostgresPartitionReaderFactory(connectionProperties)
}

class PostgresPartitionReaderFactory(connectionProperties: ConnectionProperties)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val postgresPartition = partition.asInstanceOf[PostgresPartition]
    new PostgresPartitionReader(connectionProperties, postgresPartition.from, postgresPartition.to)
  }
}

class PostgresPartitionReader(connectionProperties: ConnectionProperties, offset: Long, limit: Long) extends PartitionReader[InternalRow] {
  private val connection = DriverManager.getConnection(
    connectionProperties.url, connectionProperties.user, connectionProperties.password
  )
  private val statement = connection.createStatement()
  private val resultSet = statement.executeQuery(s"SELECT * FROM ${connectionProperties.tableName} ORDER BY user_id LIMIT $limit OFFSET $offset")

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong(1))

  override def close(): Unit = connection.close()
}

/** Write */

class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(connectionProperties(options))
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}

