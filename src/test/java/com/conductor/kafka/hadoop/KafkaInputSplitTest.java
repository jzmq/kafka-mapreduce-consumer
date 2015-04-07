/**
 * Copyright 2014 Conductor, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 */

package com.conductor.kafka.hadoop;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * @author cgreen
 */
public class KafkaInputSplitTest {

    @Test
    public void testSerialization() throws Exception {
        final Broker broker = new Broker("127.0.0.1", 9092, 1);
        final Partition partition = new Partition("topic_name", 0, broker);
        final KafkaInputSplit split = new KafkaInputSplit(partition, 0, 10l, false);
        final ByteArrayDataOutput out = ByteStreams.newDataOutput();
        split.write(out);

        final KafkaInputSplit actual = new KafkaInputSplit();
        actual.readFields(ByteStreams.newDataInput(out.toByteArray()));

        assertEquals(split, actual);
    }
}
