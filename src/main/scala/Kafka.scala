import Kafka.{getBooks, stateUpdate}
import Library.BookEvent
import cats.effect._
import cats.effect.unsafe.implicits.global
import fs2.kafka._

object Kafka{
  val deserializer: Deserializer[IO, Either[String, BookEvent]] =
    Deserializer.instance[IO, Either[String, BookEvent]] { case (_, _, payload) =>
      IO.pure(Option(BookEvent.parseFrom(payload)).toRight("Error"))
    }
  val consumerSettings: ConsumerSettings[IO, Unit, Either[String, BookEvent]] =
    ConsumerSettings[IO, Unit, Either[String, BookEvent]](Deserializer.unit[IO],deserializer)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:9092").withGroupId("group")
  val stateIo: IO[Ref[IO, Map[String, Book]]] = Ref[IO].of(Map.empty[String,Book])
  val stream: fs2.Stream[IO, BookEvent] =
    KafkaConsumer.stream(consumerSettings)
      .subscribeTo("book-events")
      .records.map(_.record.value.getOrElse(BookEvent()))

  val stateUpdate: fs2.Stream[IO, Unit] = stream.evalMap(event => stateIo.flatMap{ state => state.get.flatMap{ map => map.get(event.id) match {
    case None => state.update(_.+(event.id-> Book(event.id,event.name,event.year,event.year)))
    case Some(value) => state.update{m =>
      val history = m(value.id).history
      m.+(event.id -> Book(event.id, event.name,event.year,history ++ ", "++ event.year))
    }
  }}})
  val getBooks = stateIo.unsafeRunSync().get.unsafeRunSync()
}
object Main extends App {
  stateUpdate.compile.drain.unsafeRunSync()
  println(getBooks)
}
case class Book(id: String, name: String, year: String, history: String)