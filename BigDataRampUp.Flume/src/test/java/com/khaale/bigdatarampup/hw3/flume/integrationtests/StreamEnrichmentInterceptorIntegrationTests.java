package com.khaale.bigdatarampup.hw3.flume.integrationtests;

import com.khaale.bigdatarampup.hw3.flume.interceptors.StreamEnrichmentInterceptor;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.StringEndsWith.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

/**
 * Created by Aleksander_Khanteev on 4/23/2016.
 */
public class StreamEnrichmentInterceptorIntegrationTests {

    private static String streamEvent =
            "11baa543d120063f0f161b54232c7202\t" +
                    "20130611232904865\t" +
                    "Vh27Z5sxDva4Jg2\t" +
                    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)\t" +
                    "118.254.16.*\t" +
                    "201\t" +
                    "209\t" +
                    "3\t" +
                    "3KFal19xGq1m1YdI5SqfNX\t" +
                    "1df08a1077dbcc6b147b6b2a0889f999\t" +
                    "null\t" +
                    "Digital_F_Width1\t" +
                    "1000\t" +
                    "90\t" +
                    "0\t" +
                    "0\t" +
                    "31\t" +
                    "c46090c887c257b61ab1fa11baee91d8\t" +
                    "241\t" +
                    "3427\t" +
                    "282825712806\t" +
                    "0";

    @Test
    public void should_processEvent() {

        //arrange
        Event event = new SimpleEvent();
        event.setBody(streamEvent.getBytes());

        //act
        StreamEnrichmentInterceptor sut = new StreamEnrichmentInterceptor();
        sut.initialize();
        List<Event> result = sut.intercept(Collections.singletonList(event));

        //assert
        assertEquals(result.size(), 1);
        Event actualEvent = result.get(0);
        assertTrue(actualEvent.getHeaders().containsKey("has_user_tags"));
        assertTrue(actualEvent.getHeaders().containsKey("event_date"));
    }

}
