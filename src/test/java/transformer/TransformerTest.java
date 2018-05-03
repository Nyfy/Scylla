package transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import fields.Fields;

public class TransformerTest {
    private String simpleRecord = "{\"Category\":\"Display\",\"Brand\":\"mimo\",\"ScreenSize\":\"27\\\"\","
            + "\"Resolution\":\"1600 x 1200\",\"ResponseTime\":\"14ms\",\"RefreshRate\":\"144 hertz\","
            + "\"PanelType\":\"mva\",\"AdaptiveSync\":\"AMD Free-Sync\",\"VGA\":\"1\",\"DVI\":\"yes\","
            + "\"HDMI\":\"3\",\"DisplayPort\":\"no\",\"VesaMount\":\"yes\",\"FoundTime\":\"542143542\","
            + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\",\"ModelNumber\":\"PH-55621\","
            + "\"AspectRatio\":\"16:9\",\"PixelPitch\":\"~0.233mm,0.0233cm,~0.0008ft\",\"PixelDensity\":\"109ppi,42ppcm\","
            + "\"Brightness\":\"400cd/m2\",\"RemovableStand\":\"yes\",\"HeightAdjustment\":\"no\",\"PivotAdjustment\":\"yes\","
            + "\"SwivelAdjustment\":\"yes\",\"LeftSwivel\":\"-15 degrees\",\"RightSwivel\":\"+15 degrees\","
            + "\"TiltAdjustment\":\"no\",\"ForwardTilt\":\"15*\",\"BackwardTilt\":\"45deg\",\"Curvature\":\"curved\","
            + "\"DisplayArea\":\"90.47%\",\"Price\":\"$150.40\"}";
    
    private String complexRecord = "{\"Category\":\"Display\",\"Brand\":\"geek buying\",\"ScreenSize\":\"24.3 inches\","
            + "\"Resolution\":\"1920 x 1200\",\"ResponseTime\":\"4 ms\",\"RefreshRate\":\"80 hz - 144 hz\","
            + "\"PanelType\":\"va\",\"AdaptiveSync\":\"G sync\",\"VGA\":\"1\",\"DVI\":\"yes\","
            + "\"HDMI\":\"3\",\"DisplayPort\":\"no\",\"VesaMount\":\"yes 100mm x 150mm\",\"FoundTime\":\"542143542\","
            + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\",\"ModelNumber\":\"PH-55621\","
            + "\"AspectRatio\":\"25:9\",\"PixelPitch\":\"~1.5890mm,0.0233cm,~0.0008ft\",\"PixelDensity\":\"109ppi,42ppcm\","
            + "\"Brightness\":\"455.50cd/m2\",\"RemovableStand\":\"yes\",\"HeightAdjustment\":\"true ~110mm,200cm,~5m\",\"PivotAdjustment\":\"yes\","
            + "\"SwivelAdjustment\":\"yes\",\"LeftSwivel\":\"-15 degrees\",\"RightSwivel\":\"+15 degrees\","
            + "\"TiltAdjustment\":\"no\",\"ForwardTilt\":\"15*\",\"BackwardTilt\":\"45deg\",\"Curvature\":\"340cm\","
            + "\"DisplayArea\":\"90.47%\",\"Price\":\"$150.40\"}";
    
    @Test
    public void testGenerateKey() {
        String testUrl = "https://www.newegg.ca/Product/Product.aspx?Item=N82E16824014378";
        String testRecord = "{\"URL\":\""+testUrl+"\"}";
        String expected = "916972655fc2d994c5837b8479f13f08";
        
        assertEquals(expected, Transformer.generateKey(null,testRecord).key);
        assertEquals(testRecord, Transformer.generateKey(null,testRecord).value);
    }
    
    @Test
    public void testNormalizeSimpleFields() throws JsonParseException, JsonMappingException, IOException {
        Map<String,String> expected = new HashMap<String,String>();
        expected.put(Fields.CATEGORY,Fields.DISPLAY);
        expected.put(Fields.BRAND,"MIMO");
        expected.put(Fields.SCREEN_SIZE,"27");
        expected.put(Fields.RESOLUTION,"1600x1200");
        expected.put(Fields.RESPONSE_TIME,"14");
        expected.put(Fields.REFRESH_RATE,"144");
        expected.put(Fields.PANEL_TYPE,"MVA");
        expected.put(Fields.ADAPTIVE_SYNC,"FreeSync");
        expected.put(Fields.VGA,"1");
        expected.put(Fields.DVI,"true");
        expected.put(Fields.HDMI,"3");
        expected.put(Fields.DISPLAY_PORT,"false");
        expected.put(Fields.VESA_MOUNT,"true");
        expected.put(Fields.FOUNDTIME,"542143542");
        expected.put(Fields.URL,"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174");
        expected.put(Fields.MODEL,"PH-55621");
        expected.put(Fields.ASPECT_RATIO, "16:9");
        expected.put(Fields.PIXEL_PITCH, "0.233");
        expected.put(Fields.PIXEL_DENSITY, "109");
        expected.put(Fields.BRIGHTNESS, "400");
        expected.put(Fields.REMOVABLE_STAND, "true");
        expected.put(Fields.HEIGHT_ADJUSTMENT, "false");
        expected.put(Fields.PIVOT_ADJUSTMENT, "true");
        expected.put(Fields.SWIVEL_ADJUSTMENT, "true");
        expected.put(Fields.LEFT_SWIVEL, "15");
        expected.put(Fields.RIGHT_SWIVEL, "15");
        expected.put(Fields.TILT_ADJUSTMENT, "false");
        expected.put(Fields.FORWARD_TILT, "15");
        expected.put(Fields.BACKWARD_TILT, "45");
        expected.put(Fields.CURVATURE, "true");
        expected.put(Fields.DISPLAY_AREA, "90.47");
        expected.put(Fields.PRICE, "$150.40");
        
        String normalized = Transformer.normalizeFields(simpleRecord);
        
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> result = objectMapper.readValue(normalized, new TypeReference<Map<String,String>>(){});
        
        assertEquals(result.size(), expected.size());
        
        for (String field : expected.keySet()) {
            assertEquals(expected.get(field), result.get(field));
        }
    }
    
    @Test
    public void testNormalizeComplexFields() throws JsonParseException, JsonMappingException, IOException {
        Map<String,String> expected = new HashMap<String,String>();
        expected.put(Fields.CATEGORY,Fields.DISPLAY);
        expected.put(Fields.BRAND,"Geekbuying");
        expected.put(Fields.SCREEN_SIZE,"24.3");
        expected.put(Fields.RESOLUTION,"1920x1200");
        expected.put(Fields.RESPONSE_TIME,"4");
        expected.put(Fields.REFRESH_RATE,"144");
        expected.put(Fields.PANEL_TYPE,"VA");
        expected.put(Fields.ADAPTIVE_SYNC,"G-Sync");
        expected.put(Fields.VGA,"1");
        expected.put(Fields.DVI,"true");
        expected.put(Fields.HDMI,"3");
        expected.put(Fields.DISPLAY_PORT,"false");
        expected.put(Fields.VESA_MOUNT,"100 150");
        expected.put(Fields.FOUNDTIME,"542143542");
        expected.put(Fields.URL,"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174");
        expected.put(Fields.MODEL,"PH-55621");
        expected.put(Fields.ASPECT_RATIO, "25:9");
        expected.put(Fields.PIXEL_PITCH, "1.5890");
        expected.put(Fields.PIXEL_DENSITY, "109");
        expected.put(Fields.BRIGHTNESS, "455.50");
        expected.put(Fields.REMOVABLE_STAND, "true");
        expected.put(Fields.HEIGHT_ADJUSTMENT, "110");
        expected.put(Fields.PIVOT_ADJUSTMENT, "true");
        expected.put(Fields.SWIVEL_ADJUSTMENT, "true");
        expected.put(Fields.LEFT_SWIVEL, "15");
        expected.put(Fields.RIGHT_SWIVEL, "15");
        expected.put(Fields.TILT_ADJUSTMENT, "false");
        expected.put(Fields.FORWARD_TILT, "15");
        expected.put(Fields.BACKWARD_TILT, "45");
        expected.put(Fields.CURVATURE, "340");
        expected.put(Fields.DISPLAY_AREA, "90.47");
        expected.put(Fields.PRICE, "$150.40");
        
        String normalized = Transformer.normalizeFields(complexRecord);
        
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> result = objectMapper.readValue(normalized, new TypeReference<Map<String,String>>(){});
        
        assertEquals(result.size(), expected.size());
        
        for (String field : expected.keySet()) {
            assertEquals(expected.get(field), result.get(field));
        }
    }
}
