package kafka.s3.consumer;

import java.util.Map;

interface Configuration {

  public String getS3AccessKey();
  public String getS3SecretKey();
  public String getS3Bucket();
  public String getS3Prefix();

  public Map<String, Integer> getTopicsAndPartitions();
  public Map<String, Integer> getTopicSizes();
  
  public int getS3MaxObjectSize();
  public int getKafkaMaxMessageSize();
  
}
