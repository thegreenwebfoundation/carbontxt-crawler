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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.net.URL;
import java.nio.charset.StandardCharsets;
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
        
        byte[] contentBytes = "dummy content".getBytes(StandardCharsets.UTF_8);
        when(input.getBinaryByField("content")).thenReturn(contentBytes);

        Metadata metadata = new Metadata();
        metadata.setValue(METHOD, "root");
        metadata.setValue(HOSTNAME, "example.com"); // example.com typically has no carbon-txt-location TXT record
        when(input.getValueByField("metadata")).thenReturn(metadata);

        bolt.execute(input);

        // Verify no status stream emissions since no matching record was found
        verify(collector, never()).emit(eq("status"), any(Tuple.class), any(Values.class));

        // Verify emit of original url on default stream
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), valuesCaptor.capture());
        
        Values values = valuesCaptor.getValue();
        assertEquals(TEST_INPUT_URL, values.get(0));
        assertArrayEquals(contentBytes, (byte[]) values.get(1));
        assertEquals(metadata, values.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testExecuteWithRealDnsLookupForDigitalPebble() throws Exception {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn(TEST_INPUT_URL);
        
        byte[] contentBytes = "digital pebble content".getBytes(StandardCharsets.UTF_8);
        when(input.getBinaryByField("content")).thenReturn(contentBytes);

        Metadata metadata = new Metadata();
        metadata.setValue(METHOD, "root");
        metadata.setValue(HOSTNAME, "digitalpebble.com");
        when(input.getValueByField("metadata")).thenReturn(metadata);

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

        // Verify emit of discovered outlink to status stream
        ArgumentCaptor<Values> statusValuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq("status"), eq(input), statusValuesCaptor.capture());
        
        Values statusValues = statusValuesCaptor.getValue();
        assertEquals("https://digitalpebble.com/carbon.txt", statusValues.get(0));
        assertEquals(outlinkMetadata, statusValues.get(1));
        assertEquals(Status.DISCOVERED, statusValues.get(2));

        // Verify emit of original url on default stream
        ArgumentCaptor<Values> defaultValuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector).emit(eq(input), defaultValuesCaptor.capture());
        
        Values defaultValues = defaultValuesCaptor.getValue();
        assertEquals(TEST_INPUT_URL, defaultValues.get(0));
        assertArrayEquals(contentBytes, (byte[]) defaultValues.get(1));
        assertEquals(metadata, defaultValues.get(2));

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
