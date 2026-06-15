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
import static org.greenwebfoundation.carbontxt.MetadataKeys.HOSTNAME;
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

class DNSDiscoveryBoltTest {

    private TestDNSDiscoveryBolt bolt;
    private OutputCollector collector;
    private TopologyContext context;

    private static final String TEST_INPUT_URL = "https://digitalpebble.com/carbon.txt";

    // Testable subclass to extract and override filterOutlink
    private static class TestDNSDiscoveryBolt extends DNSDiscoveryBolt {
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
        bolt = new TestDNSDiscoveryBolt();
        collector = mock(OutputCollector.class);
        context = mock(TopologyContext.class);

        Map<String, Object> conf = new HashMap<>();
        conf.put(ProtocolResponse.PROTOCOL_MD_PREFIX_PARAM, "");
        bolt.prepare(conf, context, collector);
    }

    @Test
    void testExecuteWithNoMatchingDnsRecord() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn(TEST_INPUT_URL);
        
        Metadata metadata = new Metadata();
        metadata.setValue(METHOD, "root");
        metadata.setValue(HOSTNAME, "example.com"); // example.com typically has no carbon-txt-location TXT record
        when(input.getValueByField("metadata")).thenReturn(metadata);
        when(input.getValueByField("status")).thenReturn(Status.FETCHED);

        bolt.execute(input);

        // Verify only ONE emit on "status" stream passing original url through
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(1)).emit(eq("status"), eq(input), valuesCaptor.capture());
        
        Values values = valuesCaptor.getValue();
        assertEquals(TEST_INPUT_URL, values.get(0));
        assertEquals(metadata, values.get(1));
        assertEquals(Status.FETCHED, values.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testExecuteWithRealDnsLookupForDigitalPebble() throws Exception {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn(TEST_INPUT_URL);
        
        Metadata metadata = new Metadata();
        metadata.setValue(METHOD, "root");
        metadata.setValue(HOSTNAME, "digitalpebble.com");
        when(input.getValueByField("metadata")).thenReturn(metadata);
        when(input.getValueByField("status")).thenReturn(Status.FETCHED);

        // Mock the outlink returned by filterOutlink
        Outlink mockOutlink = mock(Outlink.class);
        Metadata outlinkMetadata = new Metadata();
        when(mockOutlink.getMetadata()).thenReturn(outlinkMetadata);
        when(mockOutlink.getTargetURL()).thenReturn("https://digitalpebble.com/carbon.txt");

        bolt.setOutlinkToReturn(mockOutlink);

        bolt.execute(input);

        // Verify filterOutlink was called with correct parameters
        assertNotNull(bolt.capturedSourceURL, "filterOutlink should have been called (real DNS lookup must have found the TXT record)");
        assertEquals(TEST_INPUT_URL, bolt.capturedSourceURL.toString());
        assertEquals("https://digitalpebble.com/carbon.txt", bolt.capturedTargetURL);
        assertEquals(metadata, bolt.capturedMetadata);

        // Verify outlink metadata got method=dns
        assertEquals("dns", outlinkMetadata.getFirstValue(METHOD));

        // Verify TWO emits on "status" stream (discovered outlink first, then original url)
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(2)).emit(eq("status"), eq(input), valuesCaptor.capture());
        
        List<Values> allValues = valuesCaptor.getAllValues();
        assertEquals(2, allValues.size());

        // First emit: discovered outlink
        Values firstEmit = allValues.get(0);
        assertEquals("https://digitalpebble.com/carbon.txt", firstEmit.get(0));
        assertEquals(outlinkMetadata, firstEmit.get(1));
        assertEquals(Status.DISCOVERED, firstEmit.get(2));

        // Second emit: original url
        Values secondEmit = allValues.get(1);
        assertEquals(TEST_INPUT_URL, secondEmit.get(0));
        assertEquals(metadata, secondEmit.get(1));
        assertEquals(Status.FETCHED, secondEmit.get(2));

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
