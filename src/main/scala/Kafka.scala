import Library.BookEvent
import cats.effect._
import cats.effect.unsafe.implicits.global
import fs2.kafka._

object Kafka {
  val headerName = "event-type"
  val deserializer: Deserializer[IO, Either[String, BookEvent]] =
    Deserializer.instance[IO, Either[String, BookEvent]] { case (_, _, payload) =>
      IO.pure(Option(BookEvent.parseFrom(payload)).toRight("Error"))
    }
  val consumerSettings: ConsumerSettings[IO, Unit, Either[String, BookEvent]] =
    ConsumerSettings[IO, Unit, Either[String, BookEvent]](Deserializer.unit[IO],deserializer)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group")

  val stream: fs2.Stream[IO, BookEvent] =
    KafkaConsumer.stream(consumerSettings)
      .subscribeTo("topic")
      .records.map(_.record.value.getOrElse(BookEvent()))

  val getBooks: List[BookEvent] = stream.compile.toList.unsafeRunSync()
}
