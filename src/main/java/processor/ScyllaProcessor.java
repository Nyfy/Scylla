package processor;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import config.KafkaConfig;
import config.Fields;


public class ScyllaProcessor {
    
    private static Logger logger = Logger.getLogger(ScyllaProcessor.class);
    private static StreamsBuilder builder;
    private static StreamsBuilder dedupBuilder;
    private static ObjectMapper objectMapper;
    private static MessageDigest digester;
    private static Properties streamsProps;
    private static Properties dedupProps;
    
    public static void main(String[] args) throws NoSuchAlgorithmException {
        initializeProperties();
        defineStream();
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        KafkaStreams dedupStreams = new KafkaStreams(dedupBuilder.build(), dedupProps);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(dedupStreams::close));
        try {
            streams.start();
            dedupStreams.start();
            
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            logger.error("An error occured while executing streams application",e);
        } finally {
            streams.close();
            dedupStreams.close();
            
            streams.cleanUp();
            dedupStreams.cleanUp();
        }
    }
    
    protected static void initializeProperties() throws NoSuchAlgorithmException {
        builder = new StreamsBuilder();
        dedupBuilder = new StreamsBuilder();
        objectMapper = new ObjectMapper();
        digester = MessageDigest.getInstance("MD5");
        
        streamsProps = new Properties();
        
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.SCYLLA_GROUP_ID);
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVER);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        dedupProps = new Properties();
        
        dedupProps.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.DEDUP_GROUP_ID);
        dedupProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVER);
        dedupProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        dedupProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }
    
    private static void defineStream() {
        KStream<String, String>[] monitorsFiltered = builder
                .stream(KafkaConfig.MONITOR_SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .branch( (key, value) -> isValidRecord(value),
                        (key, value) -> true);
        
        KTable<String, String> dedupTable = builder
                .table(KafkaConfig.MONITOR_DEDUP_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues( (value) -> "");
        
        KStream<String, String> monitorsDedup = monitorsFiltered[0]
                .map( (key, value) -> setKeyAsUrlHash(key,value))
                .leftJoin(dedupTable, (leftValue, rightValue) -> tagDuplicates(leftValue, rightValue))
                .filter( (key, value) -> deduplicate(value));
        
        KStream<String,String> monitorsNormalized = monitorsFiltered[0]
                .mapValues( (value) -> normalizeValues(value));
                
        KStream<String, String>[] monitorsValidated = monitorsNormalized
                .branch( (key, value) -> isValidNormalization(value),
                         (key, value) -> true);
        
        monitorsValidated[0].to(KafkaConfig.MONITOR_SINK_TOPIC);
        
        monitorsValidated[1].to(KafkaConfig.MONITOR_REJECTED_TOPIC);
        monitorsFiltered[1].to(KafkaConfig.MONITOR_REJECTED_TOPIC);
        
        dedupBuilder.stream(KafkaConfig.MONITOR_SINK_TOPIC).to(KafkaConfig.MONITOR_DEDUP_TOPIC);
    }
    
    protected static boolean isValidRecord(String value) {
        try {
            JsonNode record = objectMapper.readTree(value);
            
            //We want all of these to be present, else reject the record
            if (record.get(Fields.URL) == null || record.get(Fields.BRAND) == null || record.get(Fields.MODEL) == null ) {
                return false;
            }
        } catch (Exception e) {
            logger.error("An error occured while filtering invalid post: "+value,e);
            return false;
        }
        return true;
    }
    
    protected static boolean isValidNormalization(String value) {
        try {
            JsonNode record = objectMapper.readTree(value);
            
            //We want at least 2 out of 4 of these to be present, else reject the record
            boolean missingScreen = record.get(Fields.SCREEN_SIZE) == null;
            boolean missingRes = record.get(Fields.RESOLUTION) == null;
            boolean missingTime = record.get(Fields.RESPONSE_TIME) == null;
            boolean missingRate = record.get(Fields.REFRESH_RATE) == null;
            
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
            logger.error("An error occured while filtering invalid normalized post: "+value,e);
            return false;
        }
        return true;
    }
    
    protected static KeyValue<String,String> setKeyAsUrlHash(String key, String value) {
        try {
            JsonNode record = objectMapper.readTree(value);
            
            String hashedUrl = String.format("%032x", new BigInteger(1, digester.digest(record.get(Fields.URL).asText().getBytes())));
            
            return new KeyValue<String,String>(hashedUrl,value);
        } catch (Exception e) {
            logger.error("An error occured while hashing Url: "+value,e);
        }
        return null;
    }
    
    protected static String tagDuplicates(String streamValue, String tableValue) {
        if (tableValue == null) {
            return streamValue;
        }
        return Fields.DUPLICATE;
    }
    
    protected static boolean deduplicate(String value) {
        if (StringUtils.equalsIgnoreCase(value, Fields.DUPLICATE)) {
            return false;
        }
        return true;
    }
    
    protected static String normalizeValues(String value) {
        try {
            Map<String,String> normalizedValues = new HashMap<String,String>();
            
            Map<String,String> values = objectMapper.readValue(value, new TypeReference<HashMap<String,String>>(){});
            for (Entry<String,String> fieldEntry : values.entrySet()) {
                String normalizedValue = normalizeField(fieldEntry);
                
                if (StringUtils.isNotEmpty(normalizedValue)) {
                    normalizedValues.put(fieldEntry.getKey(), normalizedValue);
                }
            }
            
            normalizedValues.put(Fields.CATEGORY, values.get(Fields.CATEGORY));
            normalizedValues.put(Fields.URL, values.get(Fields.URL));
            normalizedValues.put(Fields.FOUNDTIME, values.get(Fields.FOUNDTIME));
            normalizedValues.put(Fields.MODEL, values.get(Fields.MODEL));
            normalizedValues.put(Fields.PRICE, values.get(Fields.PRICE));
            
            return objectMapper.writeValueAsString(normalizedValues);
        } catch (Exception e) {
            logger.error("An error occured while normalizing fields: "+value,e);
        }
        return null;
    }
    
    private static String normalizeField(Entry<String,String> fieldEntry) {
        Fields fields = new Fields();
        HashMap<String, String[]> possibleValues = fields.getFieldValues().get(fieldEntry.getKey());
        
        if (possibleValues != null) {
            return normalizeValue(fieldEntry, possibleValues);
        }
        return "";
    }
    
    private static String normalizeValue(Entry<String,String> fieldEntry, HashMap<String, String[]> possibleValues) {
        String bestValue = "";
        boolean foundTaggedMatch = false;
        for (String possibleValue : possibleValues.keySet()) {
            //If we've tagged this possibleValue, pull a value out of the field instead of matching to a predetermined value
            if (StringUtils.equalsIgnoreCase(possibleValue, Fields.FIND_TAG)) {
                String foundValue = findRawValue(fieldEntry, possibleValue, possibleValues);
                
                if (StringUtils.isNotEmpty(foundValue)) {
                    bestValue = foundValue;
                    foundTaggedMatch = true;
                }
                
            } else if (!foundTaggedMatch) {
                for (String regex : possibleValues.get(possibleValue)) {
                    Matcher valueMatcher = Pattern.compile("(?i)"+regex).matcher(fieldEntry.getValue());
                    
                    if (valueMatcher.find() && (possibleValue.length() > bestValue.length())) {
                        bestValue = possibleValue;
                    }
                }
            }
        }
        return bestValue;
    }
    
    private static String findRawValue(Entry<String,String> fieldEntry, String possibleValue, HashMap<String, String[]> possibleValues) {
        String rawValue = "";
        Matcher patternMatcher;
        try {
            String[] possibleMatchers = possibleValues.get(possibleValue);
            for (String matcher : possibleMatchers) {
                patternMatcher = Pattern.compile("(?i)"+matcher).matcher(fieldEntry.getValue());
                
                if (patternMatcher.matches()) {
                    rawValue = patternMatcher.group(1);
                }
            }
        } catch (IllegalStateException e) {
            logger.error("Unable to match value: "+possibleValues.get(possibleValue)[0]+" in "+fieldEntry.getValue(), e);
        }
        return rawValue;
    }
}
