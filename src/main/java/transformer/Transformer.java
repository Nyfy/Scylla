package transformer;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fields.Fields;
import processor.ScyllaProcessor;

public abstract class Transformer {
    private static Logger logger = Logger.getLogger(Transformer.class);
    
    public abstract boolean preValidate(String value);
    public abstract boolean postValidate(String value);
    
    public abstract String preProcess(String value);
    public abstract String postProcess(String value);
    
    /*
     *  Here we set the key of the record to an MD5 hash of the URL, for the purposes of creating a unique identifier. 
     */
    public static KeyValue<String,String> generateKey(String key, String value) {
        try {
            MessageDigest digester = MessageDigest.getInstance("MD5");
            ObjectMapper objectMapper = new ObjectMapper();
            
            JsonNode record = objectMapper.readTree(value);
            
            String hashedUrl = String.format("%032x", new BigInteger(1, digester.digest(record.get(Fields.URL).asText().getBytes())));
            
            return new KeyValue<String,String>(hashedUrl,value);
        } catch (Exception e) {
            logger.error("An error occured while hashing Url: "+value,e);
        }
        return null;
    }
    
    /* 
     * Here we normalize each field, excluding fields that cannot be resolved to a value. 
     */
    public static String normalizeFields(String value) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String,String> normalizedValues = new HashMap<String,String>();
            
            Map<String,String> values = objectMapper.readValue(value, new TypeReference<HashMap<String,String>>(){});
            for (Entry<String,String> fieldEntry : values.entrySet()) {
                String normalizedValue = normalizeField(fieldEntry);
                
                if (StringUtils.isNotEmpty(normalizedValue)) {
                    normalizedValues.put(fieldEntry.getKey(), normalizedValue);
                }
            }
            
            // We don't normalize these fields
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
    
    /*
     * Here we normalize the value of the field, by attempting to match the raw value to one of many possible values.
     */
    private static String normalizeField(Entry<String,String> fieldEntry) {
        Fields fields = new Fields();
        HashMap<String, String[]> possibleValues = fields.getFieldValues().get(fieldEntry.getKey());
        
        String bestValue = "";
        if (possibleValues != null) {
            boolean foundTaggedMatch = false;
            for (String possibleValue : possibleValues.keySet()) {
                
                // The FIND_TAG indicates we try to find a raw value in the data before matching a predefined value
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
        }
        return bestValue;
    }
    
    /*
     * Here we attempt to find the normalized value within the raw value, by matching one of many regular expressions.
     */
    private static String findRawValue(Entry<String,String> fieldEntry, String possibleValue, HashMap<String, String[]> possibleValues) {
        String rawValue = "";
        Matcher patternMatcher;
        try {
            String[] possibleMatchers = possibleValues.get(possibleValue);
            for (String matcher : possibleMatchers) {
                String matcherValue = "";
                patternMatcher = Pattern.compile("(?i)"+matcher).matcher(fieldEntry.getValue());
                
                if (patternMatcher.matches()) {
                    for (int i = 1; i <= patternMatcher.groupCount(); i++) {
                        matcherValue += " " + patternMatcher.group(i);
                    }
                }
                if (StringUtils.isNotEmpty(matcherValue)) {
                    rawValue = matcherValue;
                }
            }
        } catch (IllegalStateException e) {
            logger.error("Unable to match value: "+possibleValues.get(possibleValue)[0]+" in "+fieldEntry.getValue(), e);
        }
        return rawValue.trim();
    }
}
