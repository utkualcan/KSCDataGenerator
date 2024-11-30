import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector._
import com.example.kscdatagenerator.repository.EmployeeRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired 1

object KafkaSparkStreaming {

  @Autowired
  private val employeeRepository: EmployeeRepository = null

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("KafkaSparkStreaming")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "my-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = employeeRepository.findAll().map(employee => "user-" + employee.getEmpno()).toArray

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val cassandraSession = CqlSession.builder()
      .withKeyspace("expenses_ks")
      .build()

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val processedRDD = rdd.map { record: ConsumerRecord[String, String] =>
          val Array(userId, dateTime, description, expenseType, count, payment) = record.value().split(", ")
          (userId.toInt, dateTime, description, expenseType, count.toInt, payment.toDouble)
        }

        processedRDD.saveToCassandra(
          "expenses_ks",
          "expenses",
          SomeColumns("user_id", "date_time", "description", "expense_type", "count", "payment")
        )

        val totalExpenses = processedRDD.groupBy(record => record._1)
          .mapValues(records => records.map(_._6).sum)

        totalExpenses.foreach { case (userId, totalExpense) =>
          println(s"Kullanıcı ID: $userId, Toplam Harcama: $totalExpense")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}