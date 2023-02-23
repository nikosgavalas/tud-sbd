import java.time.Duration
import java.util.Properties
import io.github.azhur.kafkaserdecirce.CirceSupport
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore, Stores}


// The case classes used for serialization of the input and the output of the stream
case class Input(timestamp: Long, city_id: Int, city_name: String, refugees: Int)
case class Output(city_id: Int, city_name: String, refugees: Integer, change: Integer)

object Transformer extends App with CirceSupport {
  import io.circe.generic.auto._
  import org.apache.kafka.streams.scala.serialization.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._

  // Parse window length in seconds positional argument
  if (args.length != 1) {
    println("Time window length positional argument missing.")
    sys.exit(1)
  }
  val timeWindowLength: Long = args(0).toLong

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "transformer")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server:9092")
    p
  }

  // We can't use a store with whatever types we want, because of Serdes, so we are using three different ones.
  // These are also in-mem stores, because no persistence is needed for this assignment
  val idToRefugeesStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("refugees_store"),
    Serdes.Integer,
    Serdes.Integer
  ).withLoggingDisabled
  val idToNamesStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("names_store"),
    Serdes.Integer,
    Serdes.String
  ).withLoggingDisabled
  val idToChangesStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("changes_store"),
    Serdes.Integer,
    Serdes.Integer
  ).withLoggingDisabled

  // Define the stream
  val builder: StreamsBuilder = new StreamsBuilder
  // Attach the stores
  builder.addStateStore(idToNamesStoreBuilder)
  builder.addStateStore(idToRefugeesStoreBuilder)
  builder.addStateStore(idToChangesStoreBuilder)
  builder.stream[String, Input]("events")
    // Use a transformer to transform the events to be passed on
    .transform(() =>
      new Transformer[String, Input, KeyValue[String, Output]] {
        var context: ProcessorContext = _
        var namesStore: KeyValueStore[Integer, String] = _
        var refugeesStore: KeyValueStore[Integer, Integer] = _
        var changesStore: KeyValueStore[Integer, Integer] = _
        override def init(context: ProcessorContext): Unit = {
          this.context = context
          namesStore = context.getStateStore("names_store").asInstanceOf[KeyValueStore[Integer, String]]
          refugeesStore = context.getStateStore("refugees_store").asInstanceOf[KeyValueStore[Integer, Integer]]
          changesStore = context.getStateStore("changes_store").asInstanceOf[KeyValueStore[Integer, Integer]]
          // When the transfomer starts, schedule a function to run every timeWindowLength seconds.
          context.schedule(Duration.ofSeconds(timeWindowLength), PunctuationType.WALL_CLOCK_TIME, (timestamp: Long) => {
            val iter: KeyValueIterator[Integer, String] = namesStore.all()
            // Iterate through all the cities
            while (iter.hasNext) {
              val next = iter.next()
              val cityId: Integer = next.key
              val cityName: String = next.value
              // Get the total number of refugees and changes for each city from the two storages
              val refugees: Integer = if (Option(refugeesStore.get(cityId)).isEmpty) 0 else refugeesStore.get(cityId)
              val changes: Integer = if (Option(changesStore.get(cityId)).isEmpty) 0 else changesStore.get(cityId)
              // Update the number of refugees
              val updatedRefugees = refugees + changes
              // Create an event with the updated number of refugees and the change and push it down the stream
              context.forward(cityId.toString, Output(cityId, cityName, updatedRefugees, changes))
              // Then update the refugees store and reset the changes store for the next window
              refugeesStore.put(cityId, updatedRefugees)
              changesStore.put(cityId, 0)
            }
          })
          context.commit()
        }
        override def transform(key: String, value: Input): KeyValue[String, Output] = {
          // The transform function is called on each event
          // First save the city from this event with its name in the respective store
          if (Option(namesStore.get(value.city_id)).isEmpty) {
            namesStore.put(value.city_id, value.city_name)
          }
          // Update changes
          val prevChanges: Integer = if (Option(changesStore.get(value.city_id)).isEmpty) 0 else changesStore.get(value.city_id)
          changesStore.put(value.city_id, prevChanges + value.refugees)
          context.commit()
          // Return null so no records are emitted.
          null
        }
        override def close(): Unit = {}
      }, "names_store", "refugees_store", "changes_store")
    .to("updates")

  // Build stream and start it
  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
    val timeout = 10
    streams.close(Duration.ofSeconds(timeout))
  }
}
