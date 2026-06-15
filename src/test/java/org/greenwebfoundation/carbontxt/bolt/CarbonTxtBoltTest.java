/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.greenwebfoundation.carbontxt.bolt;

import static org.apache.stormcrawler.Constants.StatusStreamName;
import static org.greenwebfoundation.carbontxt.MetadataKeys.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.persistence.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CarbonTxtBoltTest {

    private CarbonTxtBolt bolt;
    private OutputCollector collector;
    private TopologyContext context;

    @BeforeEach
    void setUp() {
        bolt = new CarbonTxtBolt();
        collector = mock(OutputCollector.class);
        context = mock(TopologyContext.class);
        bolt.prepare(new HashMap<>(), context, collector);
    }

    @Test
    void testExecuteWithValidToml() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/carbon.txt");
        
        String tomlContent = "[org]\nname = \"The Green Web Foundation\"\n";
        byte[] contentBytes = tomlContent.getBytes(StandardCharsets.UTF_8);
        when(input.getBinaryByField("content")).thenReturn(contentBytes);
        
        Metadata metadata = new Metadata();
        when(input.getValueByField("metadata")).thenReturn(metadata);

        bolt.execute(input);

        // Verify metadata values
        assertEquals("true", metadata.getFirstValue(VALID));
        String expectedBase64 = Base64.getEncoder().encodeToString(contentBytes);
        assertEquals(expectedBase64, metadata.getFirstValue(CONTENT));
        assertNull(metadata.getValues(ERRORS));

        // Verify emit on default stream
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), valuesCaptor.capture());
        
        Values values = valuesCaptor.getValue();
        assertEquals("https://example.com/carbon.txt", values.get(0));
        assertArrayEquals(contentBytes, (byte[]) values.get(1));
        assertEquals(metadata, values.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testExecuteWithInvalidToml() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/carbon.txt");
        
        String tomlContent = "[org\nname = \"Unclosed bracket";
        byte[] contentBytes = tomlContent.getBytes(StandardCharsets.UTF_8);
        when(input.getBinaryByField("content")).thenReturn(contentBytes);
        
        Metadata metadata = new Metadata();
        when(input.getValueByField("metadata")).thenReturn(metadata);

        bolt.execute(input);

        // Verify metadata values
        assertEquals("false", metadata.getFirstValue(VALID));
        String expectedBase64 = Base64.getEncoder().encodeToString(contentBytes);
        assertEquals(expectedBase64, metadata.getFirstValue(CONTENT));
        
        String[] errors = metadata.getValues(ERRORS);
        assertNotNull(errors);
        assertTrue(errors.length > 0);
        assertFalse(errors[0].isBlank());

        // Verify emit on status stream with status ERROR
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq("status"), eq(input), valuesCaptor.capture());
        
        Values values = valuesCaptor.getValue();
        assertEquals("https://example.com/carbon.txt", values.get(0));
        assertEquals(metadata, values.get(1));
        assertEquals(Status.ERROR, values.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testExecuteWithEmptyContent() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/carbon.txt");
        
        byte[] contentBytes = new byte[0];
        when(input.getBinaryByField("content")).thenReturn(contentBytes);
        
        Metadata metadata = new Metadata();
        when(input.getValueByField("metadata")).thenReturn(metadata);

        bolt.execute(input);

        // Verify metadata values
        assertEquals("false", metadata.getFirstValue(VALID));
        assertEquals("", metadata.getFirstValue(CONTENT));
        
        String[] errors = metadata.getValues(ERRORS);
        assertNotNull(errors);
        assertEquals(1, errors.length);
        assertEquals("Empty content", errors[0]);

        // Verify emit on status stream with status ERROR
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq("status"), eq(input), valuesCaptor.capture());
        
        Values values = valuesCaptor.getValue();
        assertEquals("https://example.com/carbon.txt", values.get(0));
        assertEquals(metadata, values.get(1));
        assertEquals(Status.ERROR, values.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testDeclareOutputFields() {
        org.apache.storm.topology.OutputFieldsDeclarer declarer = mock(org.apache.storm.topology.OutputFieldsDeclarer.class);
        bolt.declareOutputFields(declarer);
        verify(declarer).declareStream(eq(StatusStreamName), any(Fields.class));
        verify(declarer).declare(any(Fields.class));
    }
}
