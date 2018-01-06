
package pipeline
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.UUID

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import io.circe.generic.auto._
import io.circe.parser._
import com.datastax.spark.connector._



case class Event(body: String, user: UUID, emitted: Long)


object StreamRunner {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "group.id" -> "1",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("events")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        rdd.map {
          record =>
            val e = decode[Event](record.value)
            e match {
              case Right(event) =>
                val emittedDate =  Instant.ofEpochMilli(event.emitted)
                val zonedDateTimeUtc = ZonedDateTime.ofInstant(emittedDate, ZoneId.of("UTC"))

                val year = zonedDateTimeUtc.getYear
                val month = zonedDateTimeUtc.getMonthValue
                val day = zonedDateTimeUtc.getDayOfMonth
                val hour = zonedDateTimeUtc.getHour
                val minute = zonedDateTimeUtc.getMinute
                (record.offset, record.key, event.body, event.user, event.emitted, year, month, day, hour, minute)

            }

        }.saveToCassandra("events", "events",  SomeColumns("offset", "key", "body", "user", "emitted", "year", "month", "day", "hour", "minute"))

      }

    }


    ssc.start()
    ssc.awaitTermination()


  }


}