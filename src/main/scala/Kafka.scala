import Library.BookEvent
import cats.effect._
import cats.effect.unsafe.implicits.global
import fs2.kafka._

class Kafka[F[_]](state: Ref[IO, Map[String, Book]]) {
  val deserializer: Deserializer[IO, Either[String, BookEvent]] =
    Deserializer.instance[IO, Either[String, BookEvent]] {
      case (_, _, payload) =>
        IO.pure(Option(BookEvent.parseFrom(payload)).toRight("Error"))
    }
  val consumerSettings: ConsumerSettings[IO, Unit, Either[String, BookEvent]] =
    ConsumerSettings[IO, Unit, Either[String, BookEvent]](
      Deserializer.unit[IO],
      deserializer
    ).withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:9092")
      .withGroupId("test-consumer-group")

  val kafkaConsumer
      : fs2.Stream[IO, KafkaConsumer[IO, Unit, Either[String, BookEvent]]] =
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo("book-events")

  val stateUpdate: BookEvent => IO[Unit] = event =>
    state.get.flatMap { map =>
      map.get(event.id) match {
        case None =>
          state.update(
            _.+(
              event.id -> Book(
                event.id,
                event.name,
                event.year,
                event.year
              )
            )
          )
        case Some(value) =>
          state.update { m =>
            val history = m(value.id).history
            m.+(
              event.id -> Book(
                event.id,
                event.name,
                event.year,
                history ++ ", " ++ event.year
              )
            )
          }
      }
    }
  val stream =
    kafkaConsumer.records
      .map(_.record.value.getOrElse(BookEvent()))
      .evalMap(stateUpdate)
}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val stateIo: IO[Ref[IO, Map[String, Book]]] =
      Ref[IO].of(Map.empty[String, Book])
    for {
      ref <- stateIo
      _ <- new Kafka[IO](ref).stream.compile.drain
    } yield (ExitCode.Success)
  }
}
case class Book(id: String, name: String, year: String, history: String)
