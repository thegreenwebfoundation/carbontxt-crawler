/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.greenwebfoundation.carbontxt.bolt;

import static org.greenwebfoundation.carbontxt.MetadataKeys.METHOD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.stormcrawler.Metadata;
import org.apache.stormcrawler.parse.Outlink;
import org.apache.stormcrawler.persistence.Status;
import org.apache.stormcrawler.protocol.ProtocolResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class HeaderDiscoveryBoltTest {

    private TestHeaderDiscoveryBolt bolt;
    private OutputCollector collector;
    private TopologyContext context;

    // Testable subclass to extract and override filterOutlink
    private static class TestHeaderDiscoveryBolt extends HeaderDiscoveryBolt {
        private Outlink outlinkToReturn;
        private URL capturedSourceURL;
        private String capturedTargetURL;
        private Metadata capturedMetadata;

        void setOutlinkToReturn(Outlink outlink) {
            this.outlinkToReturn = outlink;
        }

        @Override
        protected Outlink filterOutlink(URL sourceUrl, String targetUrl, Metadata sourceMetadata, String... keyValues) {
            this.capturedSourceURL = sourceUrl;
            this.capturedTargetURL = targetUrl;
            this.capturedMetadata = sourceMetadata;
            return outlinkToReturn;
        }
    }

    @BeforeEach
    void setUp() {
        bolt = new TestHeaderDiscoveryBolt();
        collector = mock(OutputCollector.class);
        context = mock(TopologyContext.class);

        Map<String, Object> conf = new HashMap<>();
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "protocol.");
        bolt.prepare(conf, context, collector);
    }

    @Test
    void testExecuteWithNoMatchingHeader() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/");
        
        Metadata metadata = new Metadata();
        metadata.setValue("protocol.other-header", "some-value");
        when(input.getValueByField("metadata")).thenReturn(metadata);
        when(input.getValueByField("status")).thenReturn(Status.FETCHED);

        bolt.execute(input);

        // Verify only ONE emit on "status" stream passing the original url through
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(1)).emit(eq("status"), eq(input), valuesCaptor.capture());
        
        Values values = valuesCaptor.getValue();
        assertEquals("https://example.com/", values.get(0));
        assertEquals(metadata, values.get(1));
        assertEquals(Status.FETCHED, values.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testExecuteWithMatchingHeaderAndOutlinkPassedFilter() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/");
        
        Metadata metadata = new Metadata();
        // Use matching key with different casing to verify case-insensitivity
        metadata.setValue("protocol.CarbonTxt-Location", "https://example.com/custom/carbon.txt");
        when(input.getValueByField("metadata")).thenReturn(metadata);
        when(input.getValueByField("status")).thenReturn(Status.FETCHED);

        // Mock the outlink returned by filterOutlink
        Outlink mockOutlink = mock(Outlink.class);
        Metadata outlinkMetadata = new Metadata();
        when(mockOutlink.getMetadata()).thenReturn(outlinkMetadata);
        when(mockOutlink.getTargetURL()).thenReturn("https://example.com/custom/carbon.txt");

        bolt.setOutlinkToReturn(mockOutlink);

        bolt.execute(input);

        // Verify filterOutlink was called with correct parameters
        assertNotNull(bolt.capturedSourceURL);
        assertEquals("https://example.com/", bolt.capturedSourceURL.toString());
        assertEquals("https://example.com/custom/carbon.txt", bolt.capturedTargetURL);
        assertEquals(metadata, bolt.capturedMetadata);

        // Verify outlink metadata got method=http
        assertEquals("http", outlinkMetadata.getFirstValue(METHOD));

        // Verify TWO emits on "status" stream (discovered outlink first, then original url)
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(2)).emit(eq("status"), eq(input), valuesCaptor.capture());
        
        List<Values> allValues = valuesCaptor.getAllValues();
        assertEquals(2, allValues.size());

        // First emit: discovered outlink
        Values firstEmit = allValues.get(0);
        assertEquals("https://example.com/custom/carbon.txt", firstEmit.get(0));
        assertEquals(outlinkMetadata, firstEmit.get(1));
        assertEquals(Status.DISCOVERED, firstEmit.get(2));

        // Second emit: original url
        Values secondEmit = allValues.get(1);
        assertEquals("https://example.com/", secondEmit.get(0));
        assertEquals(metadata, secondEmit.get(1));
        assertEquals(Status.FETCHED, secondEmit.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testExecuteWithMatchingHeaderAndOutlinkFiltered() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/");
        
        Metadata metadata = new Metadata();
        metadata.setValue("protocol.carbontxt-location", "https://example.com/invalid");
        when(input.getValueByField("metadata")).thenReturn(metadata);
        when(input.getValueByField("status")).thenReturn(Status.FETCHED);

        // Filter returns null (outlink is filtered out)
        bolt.setOutlinkToReturn(null);

        bolt.execute(input);

        // Verify only ONE emit on "status" stream passing original url through (since outlink was filtered out)
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(1)).emit(eq("status"), eq(input), valuesCaptor.capture());
        
        Values values = valuesCaptor.getValue();
        assertEquals("https://example.com/", values.get(0));
        assertEquals(metadata, values.get(1));
        assertEquals(Status.FETCHED, values.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testDeclareOutputFields() {
        org.apache.storm.topology.OutputFieldsDeclarer declarer = mock(org.apache.storm.topology.OutputFieldsDeclarer.class);
        bolt.declareOutputFields(declarer);
        verify(declarer).declareStream(eq("status"), any(Fields.class));
    }
}
