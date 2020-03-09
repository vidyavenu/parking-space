package parking.config;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${kafka.input.topic}")
    private String parkingRawTopic;

    @Value("${kafka.output.topic}")
    private String parkingDataTopic;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServer;

    @Bean
    public KStream<String, String> parkingKstream(@Value("${spring.application.name}") String applicationName) {
        Properties streamConfigurations = new Properties();

        streamConfigurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        streamConfigurations.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
        streamConfigurations.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamConfigurations.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        streamConfigurations.put(StreamsConfig.STATE_DIR_CONFIG, "data");
        streamConfigurations.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        streamConfigurations.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        streamConfigurations.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");
        streamConfigurations.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        StreamsBuilder parkingDatastreamsBuilder = new StreamsBuilder();
        final KStream<String, String> parkingDataStream = parkingDatastreamsBuilder.stream(parkingRawTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        parkingDataStream.foreach((key, value) -> System.out.println(key + " => " + value));

        parkingDataStream.flatMap((k, v) ->
             splitAndCreateParkingKeyValueList(v))
        .to(parkingDataTopic,
                Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(parkingDatastreamsBuilder.build(), streamConfigurations);
        streams.start();

        return parkingDataStream;
    }

    private List<KeyValue<String, String>> splitAndCreateParkingKeyValueList(String parkingStreamValue) {

        List<KeyValue<String, String>> parkingKeyValueList = new ArrayList<>();
        JSONParser parser = new JSONParser();
        try {
            JSONArray jsonArray = (JSONArray) parser.parse(parkingStreamValue);
            for (Object obj : jsonArray) {
                JSONObject jsonObject = (JSONObject) obj;

                jsonObject.put("magic_id", 1);
                String parkingKey = jsonObject.get("st_marker_id").toString() + jsonObject.get("st_marker_id").toString();

                parkingKeyValueList.add(new KeyValue(parkingKey, jsonObject.toString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return parkingKeyValueList;
    }
}

