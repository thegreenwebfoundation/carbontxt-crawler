// SPDX-License-Identifier: Apache-2.0

package org.greenwebfoundation.carbontxt.bolt;

import static org.apache.stormcrawler.Constants.StatusStreamName;
import static org.greenwebfoundation.carbontxt.MetadataKeys.METHOD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.List;
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

class SeedGeneratorTest {

    private SeedGenerator seedGenerator;
    private OutputCollector collector;
    private TopologyContext context;

    @BeforeEach
    void setUp() {
        seedGenerator = new SeedGenerator();
        collector = mock(OutputCollector.class);
        context = mock(TopologyContext.class);
        seedGenerator.prepare(new HashMap<>(), context, collector);
    }

    @Test
    void testExecuteGeneratesCandidates() {
        Tuple input = mock(Tuple.class);
        when(input.getStringByField("url")).thenReturn("example.com");

        seedGenerator.execute(input);

        // Capture emits
        ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
        verify(collector, times(2)).emit(eq("status"), eq(input), valuesCaptor.capture());

        List<Values> capturedValues = valuesCaptor.getAllValues();
        assertEquals(2, capturedValues.size());

        // Verify first candidate (root)
        Values first = capturedValues.get(0);
        assertEquals("https://example.com/carbon.txt", first.get(0));
        Metadata firstMetadata = (Metadata) first.get(1);
        assertEquals("root", firstMetadata.getFirstValue(METHOD));
        assertEquals(Status.DISCOVERED, first.get(2));

        // Verify second candidate (well-known)
        Values second = capturedValues.get(1);
        assertEquals("https://example.com/.well-known/carbon.txt", second.get(0));
        Metadata secondMetadata = (Metadata) second.get(1);
        assertEquals("well-known", secondMetadata.getFirstValue(METHOD));
        assertEquals(Status.DISCOVERED, second.get(2));

        // Verify ack
        verify(collector).ack(input);
    }

    @Test
    void testDeclareOutputFields() {
        org.apache.storm.topology.OutputFieldsDeclarer declarer = mock(org.apache.storm.topology.OutputFieldsDeclarer.class);
        seedGenerator.declareOutputFields(declarer);
        verify(declarer).declareStream(eq(StatusStreamName), any(Fields.class));
    }
}
