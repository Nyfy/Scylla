package processor;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import constant.Fields;

public class ScyllaProcessorTest {
    
    private String validRecordA = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
            + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
            + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
            + "\"PanelType\":\"IPS panel\",\"ScreenSize\":\"22in\",\"Resolution\":\"1920 x 1080\","
            + "\"ResponseTime\":\"5ms\",\"RefreshRate\":\"60 hertz\",\"VGA\":\"2\"," 
            + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
            + "\"VesaMount\":\"100mm x 100mm\"}";
    
    @Before
    public void setup() throws NoSuchAlgorithmException {
        ScyllaProcessor.initializeProperties();
    }
    
    @Test
    public void testIsValidRecord() {
        //Missing required field (URL)
        String invalidRecordA = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"ScreenSize\":\"22in\",\"Resolution\":\"1920 x 1080\","
                + "\"ResponseTime\":\"5ms\",\"RefreshRate\":\"60 hertz\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
      //Missing required field (Brand)
        String invalidRecordB = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"ScreenSize\":\"22in\",\"Resolution\":\"1920 x 1080\","
                + "\"ResponseTime\":\"5ms\",\"RefreshRate\":\"60 hertz\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
      //Missing required field (ModelNumber)
        String invalidRecordC = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"ScreenSize\":\"22in\",\"Resolution\":\"1920 x 1080\","
                + "\"ResponseTime\":\"5ms\",\"RefreshRate\":\"60 hertz\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        assertTrue(ScyllaProcessor.isValidRecord(validRecordA));
        assertFalse(ScyllaProcessor.isValidRecord(invalidRecordA));
        assertFalse(ScyllaProcessor.isValidRecord(invalidRecordB));
        assertFalse(ScyllaProcessor.isValidRecord(invalidRecordC));
    }
    
    @Test
    public void testIsValidNormalization() {
        //Has 2/4 important fields
        String validRecordB = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"ResponseTime\":\"5ms\",\"RefreshRate\":\"60 hertz\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        //Only has refresh rate
        String invalidRecordA = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"RefreshRate\":\"60 hertz\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        //Only has resolution
        String invalidRecordB = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"Resolution\":\"1920 x 1080\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        //Only has response time
        String invalidRecordC = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"\"ResponseTime\":\"5ms\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        //Only has screen size
        String invalidRecordD = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"VGA\":\"2\",\"ScreenSize\":\"22in\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        assertTrue(ScyllaProcessor.isValidNormalization(validRecordA));
        assertTrue(ScyllaProcessor.isValidNormalization(validRecordB));
        assertFalse(ScyllaProcessor.isValidNormalization(invalidRecordA));
        assertFalse(ScyllaProcessor.isValidNormalization(invalidRecordB));
        assertFalse(ScyllaProcessor.isValidNormalization(invalidRecordC));
        assertFalse(ScyllaProcessor.isValidNormalization(invalidRecordD));
    }
    
    @Test
    public void testSetKeyAsUrlHash() {
        String testUrl = "https://www.newegg.ca/Product/Product.aspx?Item=N82E16824014378";
        String testRecord = "{\"URL\":\""+testUrl+"\"}";
        String expected = "916972655fc2d994c5837b8479f13f08";
        
        assertEquals(expected,ScyllaProcessor.setKeyAsUrlHash(null,testRecord).key);
        assertEquals(testRecord,ScyllaProcessor.setKeyAsUrlHash(null,testRecord).value);
    }
    
    @Test
    public void testTagDuplicates() {
        assertEquals(ScyllaProcessor.tagDuplicates(validRecordA, null),validRecordA);
        assertEquals(ScyllaProcessor.tagDuplicates(validRecordA, ""),Fields.DUPLICATE);
    }
    
    @Test
    public void testDeduplicate() {
        assertFalse(ScyllaProcessor.deduplicate(Fields.DUPLICATE));
        assertTrue(ScyllaProcessor.deduplicate(validRecordA));
    }
    
    @Test
    public void testNormalizeValues() throws JsonParseException, JsonMappingException, IOException {
        Map<String,String> expectedMap = new HashMap<String,String>();
        expectedMap.put(Fields.CATEGORY,Fields.MONITOR);
        expectedMap.put(Fields.FOUNDTIME,"45629034585");
        expectedMap.put(Fields.URL,"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174");
        expectedMap.put(Fields.BRAND,"Acer America");
        expectedMap.put(Fields.MODEL,"PH-55621");
        expectedMap.put(Fields.PRICE,"$150.40");
        expectedMap.put(Fields.PANEL_TYPE,"IPS");
        expectedMap.put(Fields.SCREEN_SIZE,"22");
        expectedMap.put(Fields.RESOLUTION,"1920x1080");
        expectedMap.put(Fields.RESPONSE_TIME,"5");
        expectedMap.put(Fields.REFRESH_RATE,"60");
        expectedMap.put(Fields.VGA,"2");
        expectedMap.put(Fields.DVI,"Yes");
        expectedMap.put(Fields.HDMI,"No");
        expectedMap.put(Fields.DISPLAY_PORT,"2");
        expectedMap.put(Fields.ADAPTIVE_SYNC,"G-Sync");
        expectedMap.put(Fields.VESA,"100x100");
        
        String result = ScyllaProcessor.normalizeValues(validRecordA);
        
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> resultMap = objectMapper.readValue(result, new TypeReference<Map<String,String>>(){});
        
        assertEquals(resultMap.size(), expectedMap.size());
        
        for (String field : expectedMap.keySet()) {
            assertEquals(expectedMap.get(field), resultMap.get(field));
        }
    }
}
