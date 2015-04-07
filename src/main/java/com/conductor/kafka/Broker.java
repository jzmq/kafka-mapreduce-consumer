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
 * Wraps Kafka broker information, namely {@code host:port}, and {@code broker id}.
 * 
 * @author <a href="mailto:cgreen@conductor.com">Casey Green</a>
 */
public class Broker implements Writable {

    private int id;
    private String host;
    private int port;

    /**
     * The {@link Writable} constructor; use {@link #Broker(String, int, int)}
     */
    public Broker() {
    }

    public Broker(final String host, final int port, final int id) {
        this.port = port;
        this.host = host;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(host);
        dataOutput.writeInt(port);
        dataOutput.writeInt(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        this.host = dataInput.readUTF();
        this.port = dataInput.readInt();
        this.id = dataInput.readInt();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", id).add("host", host).add("port", port).toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Broker))
            return false;

        final Broker broker = (Broker) o;

        if (id != broker.id)
            return false;
        if (port != broker.port)
            return false;
        if (host != null ? !host.equals(broker.host) : broker.host != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + port;
        return result;
    }
}
