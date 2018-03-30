package processor;

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

import constant.Fields;
import constant.KafkaConstants;

public class ScyllaProcessor {
    private static String APPLICATION_ID = "Scylla";
    
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
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }
    
    private static void defineTopology() {
        KTable<String, String> feedbackTable = builder
                .table(KafkaConstants.MONITOR_SINK_TOPIC, Consumed.with(Serdes.String(),Serdes.String()))
                .mapValues( value -> "");
        
        KTable<String, String> recoveryTable = builder
                .table(KafkaConstants.MONITOR_RECOVERY_TOPIC, Consumed.with(Serdes.String(),Serdes.String()))
                .mapValues( value -> "");
        
        KTable<String, String> dedupTable = feedbackTable
                .join(recoveryTable, (feedbackValue, recoveryValue) -> recoveryValue == null ? feedbackValue : recoveryValue);
        
        KStream<String, String> monitorsDedup = builder
                .stream(KafkaConstants.MONITOR_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .map( (key, value) -> setKeyAsUrlHash(key,value))
                .leftJoin(dedupTable, (leftValue, rightValue) -> tagDuplicates(leftValue, rightValue))
                .filter( (key, value) -> deduplicate(value));
                
        KStream<String, String>[] monitorsFiltered = monitorsDedup
                .branch( (key, value) -> isValidRecord(value));
                
        monitorsFiltered[0].to(KafkaConstants.MONITOR_SINK_TOPIC);
        monitorsFiltered[1].to(KafkaConstants.MONITOR_REJECTED_TOPIC);
    }
    
    private static boolean isValidRecord(String value) {
        try {
            JsonNode record = objectMapper.readTree(value);
            
            //We want all of these to be present, else reject the record
            if (record.get(Fields.URL).isMissingNode() || record.get(Fields.BRAND).isMissingNode() || record.get(Fields.MODEL).isMissingNode()) {
                return false;
            }
            
            //We want at least 2 out of 4 of these to be present, else reject the record
            boolean missingScreen = record.get(Fields.SCREEN_SIZE).isMissingNode();
            boolean missingRes = record.get(Fields.RESOLUTION).isMissingNode();
            boolean missingTime = record.get(Fields.RESPONSE_TIME).isMissingNode();
            boolean missingRate = record.get(Fields.REFRESH_RATE).isMissingNode();
            
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
            
            String hashedUrl = digester.digest(record.get(Fields.URL).asText().getBytes()).toString();
            
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
        return Fields.DUPLICATE;
    }
    
    private static boolean deduplicate(String value) {
        if (StringUtils.equalsIgnoreCase(value, Fields.DUPLICATE)) {
            return false;
        }
        return true;
    }
}
