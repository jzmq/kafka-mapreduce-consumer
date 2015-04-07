package com.conductor.mapreduce;

import com.conductor.kafka.hadoop.KafkaJobBuilder;
import com.conductor.kafka.zk.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Johnny
 * Date: 4/7/15
 * Time: 9:07 AM
 */

public class KafKaConsumer extends Configured implements Tool {


    static class KafkaReaderMap extends Mapper<LongWritable, BytesWritable, Text, Text> {
        public void setup(Context context) {

        }

        public void map(LongWritable offset, BytesWritable message, Context context) throws IOException, InterruptedException {
            context.write(new Text(new String(message.getBytes())), new Text(""));
        }

        public void cleanup(Context context) {

        }

    }

//    static class DefineReuduce extends Reducer<Text, Text, Text, Text> {
//        public void setup(Context context) {
//
//        }
//
//        public void reduce(Text key, Iterable<Text> values, Context context) {
//
//        }
//
//        public void cleanup(Context context) {
//
//        }
//    }

    @Override
    public int run(String[] args) throws Exception {
        String zkString = args[0];
        String topicName = args[1];
        String consumerGroup = args[2];
        long timestamp = Long.parseLong(args[3]);
        int maxSplitsPerPartition = Integer.parseInt(args[4]);
        String outputPath = args[5];

        KafkaJobBuilder kafkaJobBuilder = KafkaJobBuilder.newBuilder();
        kafkaJobBuilder.setJobName("test_kafka_2_hadoop");
        kafkaJobBuilder.setZkConnect(zkString);
        kafkaJobBuilder.addQueueInput(topicName, consumerGroup, KafkaReaderMap.class);
        kafkaJobBuilder.setNumReduceTasks(0);
        kafkaJobBuilder.setKafkaFetchSizeBytes(1000000);
        kafkaJobBuilder.setMapOutputKeyClass(Text.class);
        kafkaJobBuilder.setMapOutputValueClass(Text.class);
        kafkaJobBuilder.setTextFileOutputFormat(outputPath);

        Configuration configuration = new Configuration();
        configuration.setLong("kafka.timestamp.offset", timestamp);
        configuration.setInt("kafka.max.splits.per.partition", maxSplitsPerPartition);

        final Job job = kafkaJobBuilder.configureJob(getConf());


//        job.setJarByClass(KafKaConsumer.class);
//        job.setInputFormatClass(KafkaInputFormat.class);
//        KafkaInputFormat.setZkConnect(job, zkString);
//        KafkaInputFormat.setTopic(job, topicName);
//        KafkaInputFormat.setConsumerGroup(job, consumerGroup);
//        KafkaInputFormat.setIncludeOffsetsAfterTimestamp(job, timestamp);
//        job.setMapperClass(KafkaReaderMap.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job, new Path(outputPath));
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(0);

        if (job.waitForCompletion(true)) {
            final ZkUtils zkUtils = new ZkUtils(job.getConfiguration());
            zkUtils.commit(consumerGroup, topicName);
            zkUtils.close();
            return 0;
        }
        return 1;

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new KafKaConsumer(), args);
        System.exit(res);
    }

}
