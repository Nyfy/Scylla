# Scylla
The goal of the Scylla project is to create a Kafka Stream Processor to process, normalize, restructure 
and validate streamed JSON data prior to database storage. The current implementation specifically processes scraped monitor
and television specifications into normalized, validated and restructured values. Scylla processes data from a source topic
containing raw data, into a sink topic containing only processed data for independant services to consume.

The Kafka Streams library was chosen as it provides a simple domain-specific language (DSL) for defining a topology of 
subsequent operations. Kafka Streams Processors are also vertically and horizontally scalable, due to
Apache Kafka's inherent design.

Scylla is currently a work in progress, as additional operations and improvements still being developed. However, older
versions have been completely tested functionally.

## Implementation Notes
-The base structure of the processing topology is defined in the main controller class, with specific operations implemented
within extendable "Transformer" classes providing abstract functions for pre-validating, pre-processing, post-processing and
post-validating data.

## Example Normalization
Here is an example of a raw post, and what the normalized post looks like after passing through Scylla. Note that these examples may not relate to the current version of the code, as the code is currently still a work in progress. However, the normalization and validation has been fully tested in previous versions.

### Raw Post
{
  "Category": "Display",
  "Brand": "geek buying",
  "ScreenSize": "24.3 inches",
  "Resolution": "1920 x 1200",
  "ResponseTime": "4 ms",
  "RefreshRate": "80 hz - 144 hz",
  "PanelType": "va",
  "AdaptiveSync": "G sync",
  "VGA": "1",
  "DVI": "yes",
  "HDMI": "3",
  "DisplayPort": "no",
  "VesaMount": "yes 100mm x 150mm",
  "FoundTime": "542143542",
  "URL": "https:\/\/www.newegg.ca\/Product\/Product.aspx?Item=N82E16824236174",
  "ModelNumber": "PH-55621",
  "AspectRatio": "25:9",
  "PixelPitch": "~1.5890mm,0.0233cm,~0.0008ft",
  "PixelDensity": "109ppi,42ppcm",
  "Brightness": "455.50cd\/m2",
  "RemovableStand": "yes",
  "HeightAdjustment": "true ~110mm,200cm,~5m",
  "PivotAdjustment": "yes",
  "SwivelAdjustment": "yes",
  "LeftSwivel": "-15 degrees",
  "RightSwivel": "+15 degrees",
  "TiltAdjustment": "no",
  "ForwardTilt": "15*",
  "BackwardTilt": "45deg",
  "Curvature": "340cm",
  "DisplayArea": "90.47%",
  "Price": "$150.40"
} 

### Normalized Post
{
  "Category": "Display",
  "Brand": "Geek Buying",
  "ScreenSize": "24.3",
  "Resolution": "1920x1200",
  "ResponseTime": "4",
  "RefreshRate": "144",
  "PanelType": "VA",
  "AdaptiveSync": "Nvidia G-Sync",
  "FoundTime": "542143542",
  "URL": "https:\/\/www.newegg.ca\/Product\/Product.aspx?Item=N82E16824236174",
  "ModelNumber": "PH-55621",
  "AspectRatio": "25:9",
  "PixelPitch": "1.5890",
  "PixelDensity": "109",
  "Brightness": "455.50",
  "Curvature": "340",
  "DisplayArea": "90.47",
  "Price": "150.40",
  "Ergonomics": {
    "VesaMount": "100x150",
    "RemovableStand": "true",
    "HeightAdjustment": "110",
    "PivotAdjustment": "true",
    "SwivelAdjustment": "true",
    "TiltAdjustment": "false",
    "ForwardTilt": "15",
    "BackwardTilt": "45",
    "LeftSwivel": "15",
    "RightSwivel": "15"
  },
  "Connectivity": {
    "VGA": "1",
    "DVI": "true",
    "HDMI": "3",
    "DisplayPort": "false"
  }
}
