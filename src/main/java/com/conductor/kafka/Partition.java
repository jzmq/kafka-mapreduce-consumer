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

package com.conductor.kafka;

import java.io.*;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;

/**
 * Wraps Kafka partition information, namely the hosting {@link Broker}, the {@code topic} and {@code partition id}.
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class Partition implements Writable {

    private String topic;
    private int partId;
    private Broker broker;

    /**
     * The {@link Writable} constructor; use {@link #Partition(String, int, Broker)}.
     */
    public Partition() {
    }

    public Partition(final String topic, final int partId, final Broker broker) {
        this.topic = topic;
        this.partId = partId;
        this.broker = broker;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartId() {
        return partId;
    }

    public Broker getBroker() {
        return broker;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public void setPartId(final int partId) {
        this.partId = partId;
    }

    public void setBroker(final Broker broker) {
        this.broker = broker;
    }

    /**
     * Returns the Zookeeper representation of this partition as {@code broker_id-partition_id}.
     * 
     * @return {@code broker_id-partition_id}
     */
    public String getBrokerPartition() {
        return String.format("%d-%d", broker.getId(), partId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(topic);
        dataOutput.writeInt(partId);
        broker.write(dataOutput);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.topic = dataInput.readUTF();
        this.partId = dataInput.readInt();
        this.broker = new Broker();
        this.broker.readFields(dataInput);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("topic", topic).add("partId", partId).add("broker", broker).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Partition))
            return false;

        final Partition partition = (Partition) o;

        if (partId != partition.partId)
            return false;
        if (broker != null ? !broker.equals(partition.broker) : partition.broker != null)
            return false;
        if (topic != null ? !topic.equals(partition.topic) : partition.topic != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + partId;
        result = 31 * result + (broker != null ? broker.hashCode() : 0);
        return result;
    }
}
