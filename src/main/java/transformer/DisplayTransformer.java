package transformer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
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
            
            if (record.get(Fields.URL) != null && record.get(Fields.BRAND) != null && record.get(Fields.MODEL) != null && record.get(Fields.FOUNDTIME) != null) {
                return true;
            }
        } catch (Exception e) {
            logger.error("An error occured while filtering invalid post: "+value,e);
            return false;
        }
        return false;
    }
    
    /*
     * Here we validate that at least 2 out of 4 important fields have been normalized successfully
     */
    @Override
    public boolean postValidate(String value) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode record = objectMapper.readTree(value);
            
            int fieldCount = 0;
            if (record.get(Fields.SCREEN_SIZE) != null) {
                fieldCount++;
            }
            if (record.get(Fields.RESOLUTION) != null) {
                fieldCount++;
            }
            if (record.get(Fields.RESPONSE_TIME) != null) {
                fieldCount++;
            }
            if (record.get(Fields.REFRESH_RATE) != null) {
                fieldCount++;
            }
            
            if (fieldCount >= 2) {
                return true;
            } 
        } catch (Exception e) {
            logger.error("An error occured while filtering invalid normalized post: "+value,e);
            return false;
        }
        return false;
    }
    
    /*
     * Here we expand aggregated fields in preparation to normalize them individually
     */
    @Override
    public String preProcess(String value) {
        try {
        	Matcher patternMatcher;
            ObjectMapper objectMapper = new ObjectMapper();
            Fields fields = new Fields();
            
            Map<String,String> values = objectMapper.readValue(value, new TypeReference<HashMap<String,String>>(){});
            
            //Expand the aggregated ergonomics field
            String ergonomics = values.get(Fields.ERGONOMICS);
            if (ergonomics != null) {
                Map<String,String> ergonomicAdjustments = fields.getErgonomicAdjustments();
                values.remove(Fields.ERGONOMICS);
                
                String[] ergonomicValues = StringUtils.split(ergonomics, '\n');
                for (String ergonomicValue : ergonomicValues) {
                    for (String ergonomicAdjustment : ergonomicAdjustments.keySet()) {
                    patternMatcher = Pattern.compile("(?i)"+ergonomicAdjustment).matcher(ergonomicValue);
                        if (patternMatcher.find()) {
                            values.put(ergonomicAdjustments.get(ergonomicAdjustment), ergonomicValue);
                        }
                    }
                }
            }
            
            //Expand the aggregated connectors field
            String connectors = values.get(Fields.CONNECTORS);
            if (connectors != null) {
                Map<String,String> connectorTypes = fields.getConnectorTypes();
                values.remove(Fields.CONNECTORS);
                
                String[] connectorRows = StringUtils.split(connectors, '\n');
                List<String> connectorValues = new ArrayList<String>();
                for (String connectorRow : connectorRows) {
                    connectorValues.addAll(Arrays.asList(StringUtils.split(connectorRow,',')));
                }
                
                for (String connectorValue : connectorValues) {
                    for (String connectorType : connectorTypes.keySet()) {
                        patternMatcher = Pattern.compile("(?i)"+connectorType).matcher(connectorValue);
                        if (patternMatcher.find()) {
                            values.put(connectorTypes.get(connectorType), connectorValue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Unexpected error occured while expanding aggregated fields.", e);
        }
        return null;
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
            
            for (Entry<String, List<String>> fieldCategory : fields.getCategorizedFields().entrySet()) {
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
