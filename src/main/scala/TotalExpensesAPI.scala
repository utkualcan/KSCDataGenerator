import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector._
import com.example.kscdatagenerator.repository.EmployeeRepository
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired

import scala.collection.JavaConverters._

object TotalExpensesAPI {

  @Autowired
  private val employeeRepository: EmployeeRepository = null

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("TotalExpensesAPI")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "total-expenses-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = employeeRepository.findAll().asScala.map(employee => "user-" + employee.getEmpno()).toArray

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val cassandraSession = CqlSession.builder()
      .withKeyspace("expenses_ks")
      .build()

    var totalExpenses: Map[Int, Double] = Map()

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

        totalExpenses = processedRDD.groupBy(record => record._1)
          .mapValues(records => records.map(_._6).sum)
          .collectAsMap()
          .toMap

        totalExpenses.foreach { case (userId, totalExpense) =>
          println(s"Kullanıcı ID: $userId, Toplam Harcama: $totalExpense")
        }
      }
    }

    ssc.sparkContext.parallelize(Seq("")).foreach { _ =>
      val thread = new Thread {
        override def run(): Unit = {
          unfiltered.jetty.Http(8081)
            .filter(unfiltered.filter.Planify {
              case unfiltered.request.GET(unfiltered.request.Path(Seg("total_expenses" :: Nil))) & unfiltered.request.Params(params) =>
                val userId = params("userId").headOption.getOrElse("1").toInt
                val userTotalExpense = totalExpenses.getOrElse(userId, 0.0)
                unfiltered.response.JsonContent ~> unfiltered.response.Ok ~> unfiltered.response.ResponseString(s"""{"userId": $userId, "totalExpense": $userTotalExpense}""")
            })
            .run()
        }
      }
      thread.start()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}