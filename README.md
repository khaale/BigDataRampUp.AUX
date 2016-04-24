# BigDataRampUp.AUX

## Hdfs2Kafka
Spark job which loads text files from HDFS to Kafka topic.

## Flume
### StreamEnrichmentInterceptor
Flume interceptor which adds some data to stream event:
- appends *user tags* to event body
- adds *has_user_tags* header, possible values are *Y* or *N*
- adds *event_date* header, example of value - *20130611*
