package transformer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class DisplayTransformerTest {
    
    private String simpleRawRecord = "{\"Category\":\"Display\",\"Brand\":\"mimo\",\"ScreenSize\":\"27\\\"\","
            + "\"Resolution\":\"1600 x 1200\",\"ResponseTime\":\"14ms\",\"RefreshRate\":\"144 hertz\","
            + "\"Connectors\":\"1 x dvi\\n1 x displayport 1.2\\n2 x d-sub\\n3 x hdmi 2.0\",\"Ergonomics\":"
            + "\"height-adjustable stand: 130mm\\ntilt\\nswivel\\npivot\"}";
    
    private String simplePreProcessedRecord = "{\"Category\":\"Display\",\"Brand\":\"mimo\",\"ScreenSize\":\"27\\\"\","
            + "\"Resolution\":\"1600 x 1200\",\"ResponseTime\":\"14ms\",\"RefreshRate\":\"144 hertz\","
            + "\"PanelType\":\"mva\",\"AdaptiveSync\":\"AMD Free-Sync\",\"VGA\":\"1\",\"DVI\":\"yes\","
            + "\"HDMI\":\"3\",\"DisplayPort\":\"no\",\"VesaMount\":\"yes\",\"FoundTime\":\"542143542\","
            + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\",\"ModelNumber\":\"PH-55621\","
            + "\"AspectRatio\":\"16:9\",\"PixelPitch\":\"~0.233mm,0.0233cm,~0.0008ft\",\"PixelDensity\":\"109ppi,42ppcm\","
            + "\"Brightness\":\"400cd/m2\",\"RemovableStand\":\"yes\",\"HeightAdjustment\":\"no\",\"PivotAdjustment\":\"yes\","
            + "\"SwivelAdjustment\":\"yes\",\"LeftSwivel\":\"-15 degrees\",\"RightSwivel\":\"+15 degrees\","
            + "\"TiltAdjustment\":\"no\",\"ForwardTilt\":\"15*\",\"BackwardTilt\":\"45deg\",\"Curvature\":\"curved\","
            + "\"DisplayArea\":\"90.47%\",\"Price\":\"$150.40\"}";
    
    private String complexPreProcessedRecord = "{\"Category\":\"Display\",\"Brand\":\"geek buying\",\"ScreenSize\":\"24.3 inches\","
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
    public void testPreValidate() {
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
        
        DisplayTransformer displayTransformer = new DisplayTransformer();
        
        assertTrue(displayTransformer.preValidate(simpleRawRecord));
        assertFalse(displayTransformer.preValidate(invalidRecordA));
        assertFalse(displayTransformer.preValidate(invalidRecordB));
        assertFalse(displayTransformer.preValidate(invalidRecordC));
    }
    
    @Test
    public void testPostValidate() {
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
                + "\"PanelType\":\"IPS panel\",\"ResponseTime\":\"5ms\",\"VGA\":\"2\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        //Only has screen size
        String invalidRecordD = "{\"Category\":\"Monitor\",\"FoundEpoch\":\"45629034585\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\","
                + "\"Brand\":\"Acer\",\"ModelNumber\":\"PH-55621\",\"Price\":\"$150.40\","
                + "\"PanelType\":\"IPS panel\",\"VGA\":\"2\",\"ScreenSize\":\"22in\"," 
                + "\"DVI\":\"yes\",\"HDMI\":\"no\",\"DisplayPort\":\"yes, 2\",\"AdaptiveSync\":\"NVIDIA G-Sync supported\","
                + "\"VesaMount\":\"100mm x 100mm\"}";
        
        DisplayTransformer displayTransformer = new DisplayTransformer();
        
        assertTrue(displayTransformer.postValidate(simplePreProcessedRecord));
        assertTrue(displayTransformer.postValidate(validRecordA));
        assertFalse(displayTransformer.postValidate(invalidRecordA));
        assertFalse(displayTransformer.postValidate(invalidRecordB));
        assertFalse(displayTransformer.postValidate(invalidRecordC));
        assertFalse(displayTransformer.postValidate(invalidRecordD));
    }
    
    @Test
    public void testPreProcess() {
        String expected = "{\"Category\":\"Display\",\"Brand\":\"mimo\",\"ScreenSize\":\"27\\\"\","
                + "\"Resolution\":\"1600 x 1200\",\"ResponseTime\":\"14ms\",\"RefreshRate\":\"144 hertz\","
                + "\"DVI\":\"1 x dvi\",\"DisplayPort\":\"1 x displayport 1.2\",\"VGA\":\"2 x d-sub\",\"HDMI\":\"3 x hdmi 2.0\","
                + "\"HeightAdjustment\":\"height-adjustable stand: 130mm\",\"TiltAdjustment\":\"tilt\",\"SwivelAdjustment\":\"swivel\","
                + "\"PivotAdjustment\":\"pivot\"}";
        
        DisplayTransformer displayTransformer = new DisplayTransformer();
        
        String actual = displayTransformer.preProcess(simpleRawRecord);
        
        assertEquals(expected, actual);
    }
    
    @Test
    public void testPostProcess() {
        String expectedSimple = "{\"Category\":\"Display\",\"Brand\":\"mimo\",\"ScreenSize\":\"27\\\"\","
                + "\"Resolution\":\"1600 x 1200\",\"ResponseTime\":\"14ms\",\"RefreshRate\":\"144 hertz\","
                + "\"PanelType\":\"mva\",\"AdaptiveSync\":\"AMD Free-Sync\",\"FoundTime\":\"542143542\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\",\"ModelNumber\":\"PH-55621\","
                + "\"AspectRatio\":\"16:9\",\"PixelPitch\":\"~0.233mm,0.0233cm,~0.0008ft\",\"PixelDensity\":\"109ppi,42ppcm\","
                + "\"Brightness\":\"400cd/m2\",\"Curvature\":\"curved\",\"DisplayArea\":\"90.47%\",\"Price\":\"$150.40\","
                + "\"Ergonomics\":{\"VesaMount\":\"yes\",\"RemovableStand\":\"yes\",\"HeightAdjustment\":\"no\","
                + "\"PivotAdjustment\":\"yes\",\"SwivelAdjustment\":\"yes\",\"TiltAdjustment\":\"no\",\"ForwardTilt\":\"15*\","
                + "\"BackwardTilt\":\"45deg\",\"LeftSwivel\":\"-15 degrees\",\"RightSwivel\":\"+15 degrees\"},"
                + "\"Connectivity\":{\"VGA\":\"1\",\"DVI\":\"yes\",\"HDMI\":\"3\",\"DisplayPort\":\"no\"}}";
        
        String expectedComplex = "{\"Category\":\"Display\",\"Brand\":\"geek buying\",\"ScreenSize\":\"24.3 inches\","
                + "\"Resolution\":\"1920 x 1200\",\"ResponseTime\":\"4 ms\",\"RefreshRate\":\"80 hz - 144 hz\","
                + "\"PanelType\":\"va\",\"AdaptiveSync\":\"G sync\",\"FoundTime\":\"542143542\","
                + "\"URL\":\"https://www.newegg.ca/Product/Product.aspx?Item=N82E16824236174\",\"ModelNumber\":\"PH-55621\","
                + "\"AspectRatio\":\"25:9\",\"PixelPitch\":\"~1.5890mm,0.0233cm,~0.0008ft\",\"PixelDensity\":\"109ppi,42ppcm\","
                + "\"Brightness\":\"455.50cd/m2\",\"Curvature\":\"340cm\",\"DisplayArea\":\"90.47%\",\"Price\":\"$150.40\","
                + "\"Ergonomics\":{\"VesaMount\":\"yes 100mm x 150mm\",\"RemovableStand\":\"yes\","
                + "\"HeightAdjustment\":\"true ~110mm,200cm,~5m\",\"PivotAdjustment\":\"yes\",\"SwivelAdjustment\":\"yes\","
                + "\"TiltAdjustment\":\"no\",\"ForwardTilt\":\"15*\",\"BackwardTilt\":\"45deg\",\"LeftSwivel\":\"-15 degrees\","
                + "\"RightSwivel\":\"+15 degrees\"},\"Connectivity\":{\"VGA\":\"1\",\"DVI\":\"yes\",\"HDMI\":\"3\",\"DisplayPort\":\"no\"}}";
        
        DisplayTransformer displayTransformer = new DisplayTransformer();
        
        String resultSimple = displayTransformer.postProcess(simplePreProcessedRecord);
        String resultComplex = displayTransformer.postProcess(complexPreProcessedRecord);
        
        assertEquals(expectedSimple, resultSimple);
        
        assertEquals(expectedComplex, resultComplex);
    }

}
