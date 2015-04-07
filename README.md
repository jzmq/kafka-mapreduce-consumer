Intro
============

Kangaroo is Conductor's collection of open source Hadoop Map/Reduce utilities.

At the moment, we only have a scalable Kafka input format, but there is more to come!

# Setting up Kangaroo

You can build Kangaroo with:

```mvn clean package```

## Using the Kafka Input Format
For more details, check out our [blog post about the Kafka input format](http://www.conductor.com/nightlight/data-stream-processing-bulk-kafka-hadoop/ "Data Stream Processing: A Scalable Bridge from Kafka to Hadoop").

THIS IS CURRENTLY UNTESTED AGAINST KAFKA 0.8.1

### Create a Mapper
```java
public static class MyMapper extends Mapper<LongWritable, BytesWritable, KEY_OUT, VALUE_OUT> {

    @Override
    protected void map(final LongWritable key, final BytesWritable value, final Context context) throws IOException, InterruptedException {
        // implementation
    }
}
```

* The `BytesWritable` value is the raw bytes of a single Kafka message.
* The `LongWritable` key is the Kafka offset of the message.

### Single topic

```java
// Create a new job
final Job job = Job.getInstance(getConf(), "my_job");

// Set the InputFormat
job.setInputFormatClass(KafkaInputFormat.class);

// Set your Zookeeper connection string
KafkaInputFormat.setZkConnect(job, "zookeeper-1.xyz.com:2181");

// Set the topic you want to consume
KafkaInputFormat.setTopic(job, "my_topic");

// Set the consumer group associated with this job
KafkaInputFormat.setConsumerGroup(job, "my_consumer_group");

// Set the mapper that will consume the data
job.setMapperClass(MyMapper.class);

// (Optional) Only commit offsets if the job is successful
if (job.waitForCompletion(true)) {
    final ZkUtils zk = new ZkUtils(job.getConfiguration());
    zk.commit("my_consumer_group", "my_topic");
    zk.close();
}
```

### Multiple topics
```java
// Create a new job
final Job job = Job.getInstance(getConf(), "my_job");

// Set the InputFormat
job.setInputFormatClass(MultipleKafkaInputFormat.class);

// Set your Zookeeper connection string
KafkaInputFormat.setZkConnect(job, "zookeeper-1.xyz.com:2181");

// Add as many queue inputs as you'd like
MultipleKafkaInputFormat.addTopic(job, "my_first_topic", "my_consumer_group", MyMapper.class);
MultipleKafkaInputFormat.addTopic(job, "my_second_topic", "my_consumer_group", MyMapper.class);
// ...

// (Optional) Only commit offsets if the job is successful
if (job.waitForCompletion(true)) {
    final ZkUtils zk = new ZkUtils(job.getConfiguration());
    // commit the offsets for each topic
    zk.commit("my_consumer_group", "my_first_topic");
    zk.commit("my_consumer_group", "my_second_topic");
    // ...
    zk.close();
}
```

### Customize Your Job
Our Kafka input format allows you to limit the number of splits consumed in a single job:
* By consuming data created approximately on or after a timestamp.
```java
// Consume Kafka partition files with were last modified on or after October 13th, 2014
KafkaInputFormat.setIncludeOffsetsAfterTimestamp(job, 1413172800000);
```
* By consuming a maximum number of Kafka partition files (splits), per Kafka partition.
```java
// Consume the oldest five unconsumed Kafka files per partition
KafkaInputFormat.setMaxSplitsPerPartition(job, 5);
```

### Static Access to InputSplits
Our Kafka input format exposes static access to a hypothetical job's `KafkaInputSplits`.  We've found this information useful when estimating the number of reducers for certain jobs.
This calculation is pretty fast; for a topic with 30 partitions on a 10-node Kafka cluster, this calculation took about 1 second.
```java
final Configuration conf = new Configuration();
conf.set("kafka.zk.connect", "zookeeper-1.xyz.com:2181");

// Get all splits for "my_topic"
final List<InputSplit> allTopicSplits = KafkaInputFormat.getAllSplits(conf, "my_topic");
// Get all of "my_consumer_group"'s splits for "my_topic"
final List<InputSplit> consumerSplits = KafkaInputFormat.getSplits(conf, "my_topic", "my_consumer_group");

// Do some interesting calculations...
long totalInputBytesOfJob = 0;
for (final InputSplit split : consumerSplits) {
    totalInputBytesOfJob += split.getLength();
}
```
