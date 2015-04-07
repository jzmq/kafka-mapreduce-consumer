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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.conductor.hadoop.DelegatingMapper;
import com.conductor.hadoop.TaggedInputSplit;
import com.conductor.kafka.hadoop.MultipleKafkaInputFormat.TopicConf;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * @author cgreen
 */
@RunWith(MockitoJUnitRunner.class)
public class MultipleKafkaInputFormatTest {

    @Mock
    private JobContext jobContext;
    @Mock
    private Job job;
    private Configuration conf = new Configuration(false);

    @Before
    public void setUp() throws Exception {
        when(jobContext.getConfiguration()).thenReturn(conf);
        when(job.getConfiguration()).thenReturn(conf);
        MultipleKafkaInputFormat.addTopic(job, "topic_1", "group_name", Mapper1.class);
        MultipleKafkaInputFormat.addTopic(job, "topic_2", "group_name", Mapper2.class);
        verify(job, times(2)).setMapperClass(DelegatingMapper.class);
    }

    @Test
    public void testTopicConfiguration() throws Exception {
        final List<TopicConf> result = MultipleKafkaInputFormat.getTopics(conf);
        assertEquals(2, result.size());
        assertTrue(result.contains(new TopicConf("topic_1", "group_name", Mapper1.class)));
        assertTrue(result.contains(new TopicConf("topic_2", "group_name", Mapper2.class)));
    }

    private static class Mapper1 extends Mapper {
    }

    private static class Mapper2 extends Mapper {
    }

    @Test
    public void testGetSplits() throws Exception {
        final MultipleKafkaInputFormat format = spy(new MultipleKafkaInputFormat());
        final InputSplit split1_1 = mock(KafkaInputSplit.class);
        final InputSplit split1_2 = mock(KafkaInputSplit.class);
        final InputSplit split2_1 = mock(KafkaInputSplit.class);
        final InputSplit split2_2 = mock(KafkaInputSplit.class);
        doReturn(Lists.newArrayList(split1_1, split1_2)).when(format).getInputSplits(conf, "group_name", "topic_1");
        doReturn(Lists.newArrayList(split2_1, split2_2)).when(format).getInputSplits(conf, "group_name", "topic_2");

        final List<InputSplit> splits = format.getSplits(jobContext);
        assertEquals(4, splits.size());
        final List<InputSplit> untagged = Lists.transform(splits, new Function<InputSplit, InputSplit>() {
            @Override
            public InputSplit apply(final InputSplit input) {
                assertTrue(input instanceof TaggedInputSplit);
                final TaggedInputSplit taggedSplit = (TaggedInputSplit) input;
                assertEquals(KafkaInputFormat.class, taggedSplit.getInputFormatClass());
                return taggedSplit.getInputSplit();
            }
        });
        assertTrue(untagged.contains(split1_1));
        assertTrue(untagged.contains(split1_2));
        assertTrue(untagged.contains(split2_1));
        assertTrue(untagged.contains(split2_2));
    }
}
