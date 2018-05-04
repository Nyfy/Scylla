package processor;

import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;

import config.KafkaConfig;
import transformer.*;


public class ScyllaProcessor {
    
    private static Logger logger = Logger.getLogger(ScyllaProcessor.class);
    private static StreamsBuilder builder = new StreamsBuilder();
    private static Properties streamsProps;
    
    public static void main(String[] args) throws NoSuchAlgorithmException {
        initializeProperties();
        defineStream();
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        try {
            streams.start();
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            logger.error("An error occured while executing streams application",e);
        } finally {
            streams.close();
            streams.cleanUp();
        }
    }
    
    protected static void initializeProperties() throws NoSuchAlgorithmException {
        streamsProps = new Properties();
        
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.SCYLLA_GROUP_ID);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVER);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }
    
    private static void defineStream() {
        DisplayTransformer displayTransformer = new DisplayTransformer();
        
        KStream<String, String>[] displaysPreValidated = builder
                .stream(KafkaConfig.DISPLAY_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .branch( (key, value) -> displayTransformer.preValidate(value), (key, value) -> true);
        
        KStream<String, String> displaysPreProcessed = displaysPreValidated[0]
                .map( (key, value) -> displayTransformer.generateKey(key, value))
                .mapValues( (value) -> displayTransformer.preProcess(value));
        
        KStream<String,String> displaysPostProcessed = displaysPreProcessed
                .mapValues( (value) -> displayTransformer.normalizeFields(value))
                .mapValues( (value) -> displayTransformer.postProcess(value));
        
        KStream<String, String>[] displaysPostValidated = displaysPostProcessed
                .branch( (key, value) -> displayTransformer.postValidate(value), (key, value) -> true);
        
        displaysPostValidated[0].to(KafkaConfig.DISPLAY_SINK_TOPIC);
        
        displaysPreValidated[1].to(KafkaConfig.DISPLAY_REJECTED_TOPIC);
        displaysPostValidated[1].to(KafkaConfig.DISPLAY_REJECTED_TOPIC);
    }
}
