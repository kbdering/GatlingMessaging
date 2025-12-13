package pl.perfluencer.kafka.actions;

import io.gatling.core.CoreComponents;
import io.gatling.core.action.Action;
import io.gatling.core.session.Session;
import io.gatling.core.stats.StatsEngine;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.testkit.javadsl.TestKit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import pl.perfluencer.kafka.actors.KafkaProducerActor;
import pl.perfluencer.kafka.cache.InMemoryRequestStore;
import pl.perfluencer.kafka.cache.RequestStore;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.util.SerializationType;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class SendFailureTest {

    private ActorSystem system;
    private Producer<String, Object> mockProducer;
    private InMemoryRequestStore requestStore;
    private KafkaProtocolBuilder.KafkaProtocol kafkaProtocol;
    private CoreComponents coreComponents;

    @Before
    public void setUp() throws Exception {
        system = ActorSystem.create("SendFailureTest");
        mockProducer = mock(Producer.class);
        requestStore = new InMemoryRequestStore();

        kafkaProtocol = mock(KafkaProtocolBuilder.KafkaProtocol.class);
        when(kafkaProtocol.getRequestStore()).thenReturn(requestStore);
        when(kafkaProtocol.getCorrelationHeaderName()).thenReturn("correlationId");

        ActorRef producerRouter = system.actorOf(KafkaProducerActor.props(mockProducer, null, null));
        when(kafkaProtocol.getProducerRouter()).thenReturn(producerRouter);

        coreComponents = mock(CoreComponents.class);
        StatsEngine statsEngine = mock(StatsEngine.class);
        when(coreComponents.statsEngine()).thenReturn(statsEngine);
        when(coreComponents.clock()).thenReturn(new io.gatling.commons.util.DefaultClock());
    }

    @After
    public void tearDown() throws Exception {
        requestStore.close();
        TestKit.shutdownActorSystem(system);
    }

    @Test
    public void testSendFailureVerifyDeleteCall() throws Exception {
        // Mock RequestStore to verify deleteRequest interaction
        RequestStore mockStore = mock(RequestStore.class);
        when(kafkaProtocol.getRequestStore()).thenReturn(mockStore);

        // Mock Producer to fail
        Mockito.doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, new RuntimeException("Simulated Failure"));
            return null;
        }).when(mockProducer).send(any(ProducerRecord.class), any(Callback.class));

        Action nextAction = mock(Action.class);
        KafkaRequestReplyAction action = new KafkaRequestReplyAction(
                "requestName", kafkaProtocol, coreComponents, nextAction,
                "req-topic", session -> "key", session -> "value", Collections.emptyMap(),
                SerializationType.STRING, true, 1000, TimeUnit.MILLISECONDS);

        Session session = mock(Session.class);
        when(session.scenario()).thenReturn("test-scenario");

        action.execute(session);

        Thread.sleep(100);

        // Verify storeRequest was called (to generate ID) and deleteRequest was called
        verify(mockStore).storeRequest(any(), any(), any(), any(), any(), any(), any(Long.class), any(Long.class));
        verify(mockStore).deleteRequest(any());
    }
}
