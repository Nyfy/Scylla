# Scylla
(Work In Progress)\n
The goal of the Scylla project is to create a Kafka Stream Processor to process, normalize, restructure 
and validate streamed JSON data prior to database storage. The current implementation specifically processes scraped monitor
and television specifications into normalized, validated and restructured values. Scylla processes data from a source topic
containing raw data, into a sink topic containing only processed data for independant services to consume.

The Kafka Streams library was chosen as it provides a simple domain-specific language (DSL) for defining a topology of 
subsequent operations. Kafka Streams Processors are also inherently vertically and horizontally scalable, due to
Apache Kafka's inherent design.

Scylla is currently a work in progress, as additional operations and improvements still being developed. However, older
versions have been completely tested successfully.

## Example Data
TODO: Add example data for observation

## Implementation Notes
The base structure of the processing topology is defined in the main controller class, with specific operations implemented
within extendable "Transformer" classes providing abstract functions for pre-validating, pre-processing, post-processing and
post-validating data.
