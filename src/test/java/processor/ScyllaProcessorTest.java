package processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import fields.Fields;

public class ScyllaProcessorTest {
    
    private String simpleRecord = "{\"Category\":\"Display\",\"Brand\":\"mimo\",\"ScreenSize\":\"27\\\"\","
            + "\"Resolution\":\"1600 x 1200\",\"ResponseTime\":\"14ms\",\"RefreshRate\":\"144 hertz\","
            + "\"PanelType\":\"mva\",\"AdaptiveSync\":\"AMD Free-Sync\",\"VGA\":\"1\",\"DVI\":\"yes\","
            + "\"HDMI\":\"3\",\"DisplayPort\":\"no\",\"VesaMount\":\"yes\",\"FoundTime\":\"542143542\","
            + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\",\"ModelNumber\":\"PH-55621\","
            + "\"AspectRatio\":\"16:9\",\"PixelPitch\":\"~0.233mm,0.0233cm,~0.0008ft\",\"PixelDensity\":\"109ppi,42ppcm\","
            + "\"Brightness\":\"400cd/m2\",\"RemovableStand\":\"yes\",\"HeightAdjustment\":\"no\",\"PortraitPivot\":\"yes\","
            + "\"SwivelAdjustment\":\"yes\",\"LeftSwivel\":\"-15 degrees\",\"RightSwivel\":\"+15 degrees\","
            + "\"TiltAdjustment\":\"no\",\"ForwardTilt\":\"15*\",\"BackwardTilt\":\"45deg\",\"Curvature\":\"curved\","
            + "\"DisplayArea\":\"90.47%\",\"Price\":\"$150.40\"}";
    
    private String complexRecord = "{\"Category\":\"Display\",\"Brand\":\"geek buying\",\"ScreenSize\":\"24.3 inches\","
            + "\"Resolution\":\"1920 x 1200\",\"ResponseTime\":\"4 ms\",\"RefreshRate\":\"80 hz - 144 hz\","
            + "\"PanelType\":\"va\",\"AdaptiveSync\":\"G sync\",\"VGA\":\"1\",\"DVI\":\"yes\","
            + "\"HDMI\":\"3\",\"DisplayPort\":\"no\",\"VesaMount\":\"yes 100mm x 150mm\",\"FoundTime\":\"542143542\","
            + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\",\"ModelNumber\":\"PH-55621\","
            + "\"AspectRatio\":\"25:9\",\"PixelPitch\":\"~1.5890mm,0.0233cm,~0.0008ft\",\"PixelDensity\":\"109ppi,42ppcm\","
            + "\"Brightness\":\"455.50cd/m2\",\"RemovableStand\":\"yes\",\"HeightAdjustment\":\"true ~110mm,200cm,~5m\",\"PortraitPivot\":\"yes\","
            + "\"SwivelAdjustment\":\"yes\",\"LeftSwivel\":\"-15 degrees\",\"RightSwivel\":\"+15 degrees\","
            + "\"TiltAdjustment\":\"no\",\"ForwardTilt\":\"15*\",\"BackwardTilt\":\"45deg\",\"Curvature\":\"340cm\","
            + "\"DisplayArea\":\"90.47%\",\"Price\":\"$150.40\"}";


    
    @BeforeAll
    public static void setup() throws NoSuchAlgorithmException {
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
        
        assertTrue(ScyllaProcessor.isValidRecord(simpleRecord));
        assertFalse(ScyllaProcessor.isValidRecord(invalidRecordA));
        assertFalse(ScyllaProcessor.isValidRecord(invalidRecordB));
        assertFalse(ScyllaProcessor.isValidRecord(invalidRecordC));
    }
    
    @Test
    public void testIsValidNormalization() {
        //Has 2/4 important fields
        String validRecordA = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
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
                + "\"PanelType\":\"IPS panel\",\"\"<-INVALIDQUOTE-ResponseTime\":\"5ms\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        //Only has screen size
        String invalidRecordD = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"VGA\":\"2\",\"ScreenSize\":\"22in\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        assertTrue(ScyllaProcessor.isValidNormalization(simpleRecord));
        assertTrue(ScyllaProcessor.isValidNormalization(validRecordA));
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
    public void testNormalizeSimpleFields() throws JsonParseException, JsonMappingException, IOException {
        Map<String,String> expectedMap = new HashMap<String,String>();
        expectedMap.put(Fields.CATEGORY,Fields.DISPLAY);
        expectedMap.put(Fields.BRAND,"MIMO");
        expectedMap.put(Fields.SCREEN_SIZE,"27");
        expectedMap.put(Fields.RESOLUTION,"1600x1200");
        expectedMap.put(Fields.RESPONSE_TIME,"14");
        expectedMap.put(Fields.REFRESH_RATE,"144");
        expectedMap.put(Fields.PANEL_TYPE,"MVA");
        expectedMap.put(Fields.ADAPTIVE_SYNC,"FreeSync");
        expectedMap.put(Fields.VGA,"1");
        expectedMap.put(Fields.DVI,"true");
        expectedMap.put(Fields.HDMI,"3");
        expectedMap.put(Fields.DISPLAY_PORT,"false");
        expectedMap.put(Fields.VESA_MOUNT,"true");
        expectedMap.put(Fields.FOUNDTIME,"542143542");
        expectedMap.put(Fields.URL,"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174");
        expectedMap.put(Fields.MODEL,"PH-55621");
        expectedMap.put(Fields.ASPECT_RATIO, "16:9");
        expectedMap.put(Fields.PIXEL_PITCH, "0.233");
        expectedMap.put(Fields.PIXEL_DENSITY, "109");
        expectedMap.put(Fields.BRIGHTNESS, "400");
        expectedMap.put(Fields.REMOVABLE_STAND, "true");
        expectedMap.put(Fields.HEIGHT_ADJUSTMENT, "false");
        expectedMap.put(Fields.PORTRAIT_PIVOT, "true");
        expectedMap.put(Fields.SWIVEL_ADJUSTMENT, "true");
        expectedMap.put(Fields.LEFT_SWIVEL, "15");
        expectedMap.put(Fields.RIGHT_SWIVEL, "15");
        expectedMap.put(Fields.TILT_ADJUSTMENT, "false");
        expectedMap.put(Fields.FORWARD_TILT, "15");
        expectedMap.put(Fields.BACKWARD_TILT, "45");
        expectedMap.put(Fields.CURVATURE, "true");
        expectedMap.put(Fields.DISPLAY_AREA, "90.47");
        expectedMap.put(Fields.PRICE, "$150.40");
        
        String result = ScyllaProcessor.normalizeFields(simpleRecord);
        
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> resultMap = objectMapper.readValue(result, new TypeReference<Map<String,String>>(){});
        
        assertEquals(resultMap.size(), expectedMap.size());
        
        for (String field : expectedMap.keySet()) {
            assertEquals(expectedMap.get(field), resultMap.get(field));
        }
    }
    
    @Test
    public void testNormalizeComplexFields() throws JsonParseException, JsonMappingException, IOException {
        Map<String,String> expectedMap = new HashMap<String,String>();
        expectedMap.put(Fields.CATEGORY,Fields.DISPLAY);
        expectedMap.put(Fields.BRAND,"Geekbuying");
        expectedMap.put(Fields.SCREEN_SIZE,"24.3");
        expectedMap.put(Fields.RESOLUTION,"1920x1200");
        expectedMap.put(Fields.RESPONSE_TIME,"4");
        expectedMap.put(Fields.REFRESH_RATE,"144");
        expectedMap.put(Fields.PANEL_TYPE,"VA");
        expectedMap.put(Fields.ADAPTIVE_SYNC,"G-Sync");
        expectedMap.put(Fields.VGA,"1");
        expectedMap.put(Fields.DVI,"true");
        expectedMap.put(Fields.HDMI,"3");
        expectedMap.put(Fields.DISPLAY_PORT,"false");
        expectedMap.put(Fields.VESA_MOUNT,"100 150");
        expectedMap.put(Fields.FOUNDTIME,"542143542");
        expectedMap.put(Fields.URL,"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174");
        expectedMap.put(Fields.MODEL,"PH-55621");
        expectedMap.put(Fields.ASPECT_RATIO, "25:9");
        expectedMap.put(Fields.PIXEL_PITCH, "1.5890");
        expectedMap.put(Fields.PIXEL_DENSITY, "109");
        expectedMap.put(Fields.BRIGHTNESS, "455.50");
        expectedMap.put(Fields.REMOVABLE_STAND, "true");
        expectedMap.put(Fields.HEIGHT_ADJUSTMENT, "110");
        expectedMap.put(Fields.PORTRAIT_PIVOT, "true");
        expectedMap.put(Fields.SWIVEL_ADJUSTMENT, "true");
        expectedMap.put(Fields.LEFT_SWIVEL, "15");
        expectedMap.put(Fields.RIGHT_SWIVEL, "15");
        expectedMap.put(Fields.TILT_ADJUSTMENT, "false");
        expectedMap.put(Fields.FORWARD_TILT, "15");
        expectedMap.put(Fields.BACKWARD_TILT, "45");
        expectedMap.put(Fields.CURVATURE, "340");
        expectedMap.put(Fields.DISPLAY_AREA, "90.47");
        expectedMap.put(Fields.PRICE, "$150.40");
        
        String result = ScyllaProcessor.normalizeFields(complexRecord);
        
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> resultMap = objectMapper.readValue(result, new TypeReference<Map<String,String>>(){});
        
        assertEquals(resultMap.size(), expectedMap.size());
        
        for (String field : expectedMap.keySet()) {
            assertEquals(expectedMap.get(field), resultMap.get(field));
        }
    }
}
