package scylla.processor;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ScyllaProcessor {
    private static String APPLICATION_ID = "Scylla";
    private static String BOOTSTRAP_SERVER = "localhost:9092";
    
    private static String MONITOR_SOURCE_TOPIC = "Monitors-Raw";
    private static String MONITOR_SINK_TOPIC = "Monitors-Processed";
    private static String MONITOR_REJECTED_TOPIC = "Monitors-Rejected";
    private static String MONITOR_RECOVERY_TOPIC = "Monitors-Recovery";
    
    private static String URL = "URL";
    private static String BRAND = "Brand";
    private static String MODEL = "ModelNumber";
    private static String SCREEN_SIZE = "ScreenSize";
    private static String RESOLUTION = "Resolution";
    private static String RESPONSE_TIME = "ResponseTime";
    private static String REFRESH_RATE = "RefreshRate";
    private static String DUPLICATE = "dup";
    
    private static Logger logger = Logger.getLogger(ScyllaProcessor.class);
    private static StreamsBuilder builder;
    private static ObjectMapper objectMapper;
    private static MessageDigest digester;
    private static Properties streamsProps;
    
    public static void main(String[] args) throws NoSuchAlgorithmException {
        initializeProperties();
        defineTopology();
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        try {
            streams.start();
        } catch (Exception e) {
            logger.error("An error occured while executing streams application",e);
        } finally {
            streams.close();
        }
    }
    
    private static void initializeProperties() throws NoSuchAlgorithmException {
        builder = new StreamsBuilder();
        objectMapper = new ObjectMapper();
        digester = MessageDigest.getInstance("MD5");
        
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }
    
    private static void defineTopology() {
        KTable<String, String> feedbackTable = builder
                .table(MONITOR_SINK_TOPIC, Consumed.with(Serdes.String(),Serdes.String()))
                .mapValues( value -> "");
        
        KTable<String, String> recoveryTable = builder
                .table(MONITOR_RECOVERY_TOPIC, Consumed.with(Serdes.String(),Serdes.String()))
                .mapValues( value -> "");
        
        KTable<String, String> dedupTable = feedbackTable
                .join(recoveryTable, (feedbackValue, recoveryValue) -> recoveryValue);
        
        KStream<String, String> monitorsDedup = builder
                .stream(MONITOR_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .map( (key, value) -> setKeyAsUrlHash(key,value))
                .leftJoin(dedupTable, (leftValue, rightValue) -> tagDuplicates(leftValue, rightValue))
                .filter( (key, value) -> deduplicate(value));
                
        KStream<String, String>[] monitorsFiltered = monitorsDedup
                .branch( (key, value) -> isValidRecord(value));
                
        monitorsFiltered[0].to(MONITOR_SINK_TOPIC);
        monitorsFiltered[1].to(MONITOR_REJECTED_TOPIC);
    }
    
    private static boolean isValidRecord(String value) {
        try {
            JsonNode record = objectMapper.readTree(value);
            
            //We want all of these to be present, else reject the record
            if (record.get(URL).isMissingNode() || record.get(BRAND).isMissingNode() || record.get(MODEL).isMissingNode()) {
                return false;
            }
            
            //We want at least 2 out of 4 of these to be present, else reject the record
            boolean missingScreen = record.get(SCREEN_SIZE).isMissingNode();
            boolean missingRes = record.get(RESOLUTION).isMissingNode();
            boolean missingTime = record.get(RESPONSE_TIME).isMissingNode();
            boolean missingRate = record.get(REFRESH_RATE).isMissingNode();
            
            if (missingScreen && missingRes && missingTime) {
                return false;
            } else if (missingScreen && missingTime && missingRate) {
                return false;
            } else if (missingScreen && missingRes && missingRate) {
                return false;
            } else if (missingRes && missingTime && missingRate) {
                return false;
            }
        } catch (Exception e) {
            logger.error("An error occured while filtering invalid post: "+value,e);
            return false;
        }
        return true;
    }
    
    private static KeyValue<String,String> setKeyAsUrlHash(String key, String value) {
        try {
            JsonNode record = objectMapper.readTree(value);
            
            String hashedUrl = digester.digest(record.get(URL).asText().getBytes()).toString();
            
            return new KeyValue<String,String>(hashedUrl,value);
        } catch (Exception e) {
            logger.error("An error occured while hashing Url: "+value,e);
        }
        return null;
    }
    
    private static String tagDuplicates(String streamValue, String tableValue) {
        if (tableValue == null) {
            return streamValue;
        }
        return DUPLICATE;
    }
    
    private static boolean deduplicate(String value) {
        if (StringUtils.equalsIgnoreCase(value, DUPLICATE)) {
            return false;
        }
        return true;
    }
}
