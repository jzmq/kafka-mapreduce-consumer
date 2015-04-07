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

import java.io.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.conductor.kafka.Partition;

/**
 * An {@link InputSplit} that has a specific {@code start} and {@code end} offset in a Kafka queue {@link Partition}.
 * 
 * @see KafkaInputFormat
 * @see MultipleKafkaInputFormat
 * @see KafkaRecordReader
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class KafkaInputSplit extends InputSplit implements Writable {

    private Partition partition;
    private long startOffset;
    private long endOffset;
    private boolean partitionCommitter;

    /**
     * The {@link Writable} constructor; use {@link #KafkaInputSplit(Partition, long, long, boolean)}.
     */
    public KafkaInputSplit() {
    }

    public KafkaInputSplit(final Partition partition, final long startOffset, final long endOffset,
            final boolean partitionCommitter) {
        this.partition = partition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.partitionCommitter = partitionCommitter;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.partition = new Partition();
        this.partition.readFields(in);
        this.startOffset = in.readLong();
        this.endOffset = in.readLong();
        this.partitionCommitter = in.readBoolean();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        this.partition.write(out);
        out.writeLong(startOffset);
        out.writeLong(endOffset);
        out.writeBoolean(partitionCommitter);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return endOffset - startOffset;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] { toString() };
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(final Partition partition) {
        this.partition = partition;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(final long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(final long endOffset) {
        this.endOffset = endOffset;
    }

    public boolean isPartitionCommitter() {
        return partitionCommitter;
    }

    public void setPartitionCommitter(final boolean partitionCommitter) {
        this.partitionCommitter = partitionCommitter;
    }

    @Override
    public String toString() {
        return String.format("%s:%d:%s_%d[%d, %d]", partition.getBroker().getHost(), partition.getBroker().getPort(),
                partition.getTopic(), partition.getPartId(), startOffset, endOffset);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof KafkaInputSplit))
            return false;

        final KafkaInputSplit that = (KafkaInputSplit) o;

        if (endOffset != that.endOffset)
            return false;
        if (partitionCommitter != that.partitionCommitter)
            return false;
        if (startOffset != that.startOffset)
            return false;
        if (partition != null ? !partition.equals(that.partition) : that.partition != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = partition != null ? partition.hashCode() : 0;
        result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
        result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
        result = 31 * result + (partitionCommitter ? 1 : 0);
        return result;
    }
}
