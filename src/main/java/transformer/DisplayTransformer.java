package transformer;

import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fields.Fields;

public class DisplayTransformer extends Transformer {
    private static Logger logger = Logger.getLogger(DisplayTransformer.class);
    
    /*
     * Here we validate that the record arrives with all mandatory fields (Url, Brand, Model and FoundTime)
     */
    @Override
    public boolean preValidate(String value) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode record = objectMapper.readTree(value);
            
            if (record.get(Fields.URL) == null || record.get(Fields.BRAND) == null || record.get(Fields.MODEL) == null || record.get(Fields.FOUNDTIME) == null) {
                return false;
            }
        } catch (Exception e) {
            logger.error("An error occured while filtering invalid post: "+value,e);
            return false;
        }
        return true;
    }
    
    /*
     * Here we validate that at least 2 out of 4 important fields have been normalized successfully
     */
    @Override
    public boolean postValidate(String value) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode record = objectMapper.readTree(value);
            
            int fieldCount = 4;
            if (record.get(Fields.SCREEN_SIZE) == null) {
                fieldCount--;
            }
            if (record.get(Fields.RESOLUTION) == null) {
                fieldCount--;
            }
            if (record.get(Fields.RESPONSE_TIME) == null) {
                fieldCount--;
            }
            if (record.get(Fields.REFRESH_RATE) == null) {
                fieldCount--;
            }
            
            if (fieldCount < 2) {
                return false;
            } 
        } catch (Exception e) {
            logger.error("An error occured while filtering invalid normalized post: "+value,e);
            return false;
        }
        return true;
    }
    
    /*
     * Here we expand aggregated fields in preparation to normalize them individually
     */
    @Override
    public String preProcess(String value) {
        //TODO explode combined fields
        return value;
    }
    
    /*
     * Here we restructure the final record so that categorized fields (e.g Ergonomics, Connectivity) are nested together in a non-flat structure
     */
    @Override
    public String postProcess(String value) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Fields fields = new Fields();
            JsonNode record = objectMapper.readTree(value);
            
            for (Entry<String, List<String>> fieldCategory : fields.CATEGORIZED_FIELDS.entrySet()) {
                ObjectNode explodedNode = objectMapper.createObjectNode();
                
                for (String field : fieldCategory.getValue()) {
                    if (record.has(field)) {
                        explodedNode.put(field, record.get(field).asText());
                        ((ObjectNode) record).remove(field);
                    }
                }
                ((ObjectNode) record).set(fieldCategory.getKey(), explodedNode);
            }
            return objectMapper.writeValueAsString(record);
        } catch (Exception e) {
            logger.error("Unexpected error occured while exploding structure.", e);
        }
        return null;
    }

}
