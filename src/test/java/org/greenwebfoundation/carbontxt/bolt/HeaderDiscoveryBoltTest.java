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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.net.URL;
import java.util.HashMap;
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
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "");
        bolt.prepare(conf, context, collector);
    }

    @Test
    void testExecuteWithNoMatchingHeader() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/");
        
        Metadata metadata = new Metadata();
        metadata.setValue("other-header", "some-value");
        when(input.getValueByField("metadata")).thenReturn(metadata);

        bolt.execute(input);

        // Verify no status stream emissions since no matching header
        verify(collector, never()).emit(eq("status"), any(Tuple.class), any(Values.class));

        // Verify emit of original url with FETCHED status
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), valuesCaptor.capture());
        
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
        metadata.setValue("CarbonTxt-Location", "https://example.com/custom/carbon.txt");
        when(input.getValueByField("metadata")).thenReturn(metadata);

        // Mock the outlink returned by filterOutlink
        Outlink mockOutlink = mock(Outlink.class);
        Metadata outlinkMetadata = new Metadata();
        when(mockOutlink.getMetadata()).thenReturn(outlinkMetadata);
        when(mockOutlink.getTargetURL()).thenReturn("https://example.com/custom/carbon.txt");

        bolt.setOutlinkToReturn(mockOutlink);

        bolt.execute(input);

        // Verify filterOutlink was called with correct parameters
        assertEquals("https://example.com/", bolt.capturedSourceURL.toString());
        assertEquals("https://example.com/custom/carbon.txt", bolt.capturedTargetURL);
        assertEquals(metadata, bolt.capturedMetadata);

        // Verify outlink metadata got method=http
        assertEquals("http", outlinkMetadata.getFirstValue(METHOD));

        // Verify emit of discovered outlink to status stream
        ArgumentCaptor<Values> statusValuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq("status"), eq(input), statusValuesCaptor.capture());
        
        Values statusValues = statusValuesCaptor.getValue();
        assertEquals("https://example.com/custom/carbon.txt", statusValues.get(0));
        assertEquals(outlinkMetadata, statusValues.get(1));
        assertEquals(Status.DISCOVERED, statusValues.get(2));

        // Verify emit of original url with FETCHED status on default stream
        ArgumentCaptor<Values> defaultValuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), defaultValuesCaptor.capture());
        
        Values defaultValues = defaultValuesCaptor.getValue();
        assertEquals("https://example.com/", defaultValues.get(0));
        assertEquals(metadata, defaultValues.get(1));
        assertEquals(Status.FETCHED, defaultValues.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testExecuteWithMatchingHeaderAndOutlinkFiltered() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("https://example.com/");
        
        Metadata metadata = new Metadata();
        metadata.setValue("carbontxt-location", "https://example.com/invalid");
        when(input.getValueByField("metadata")).thenReturn(metadata);

        // Filter returns null (outlink is filtered out)
        bolt.setOutlinkToReturn(null);

        bolt.execute(input);

        // Verify no status stream emissions since outlink is filtered
        verify(collector, never()).emit(eq("status"), any(Tuple.class), any(Values.class));

        // Verify emit of original url with FETCHED status
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), valuesCaptor.capture());
        
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
        verify(declarer).declare(any(Fields.class));
    }
}
