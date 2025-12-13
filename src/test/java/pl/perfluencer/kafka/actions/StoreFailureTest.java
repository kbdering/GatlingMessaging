package pl.perfluencer.kafka.actions;

import io.gatling.core.CoreComponents;
import io.gatling.core.action.Action;
import io.gatling.core.action.Action;
import io.gatling.core.session.Session;
import io.gatling.core.stats.StatsEngine;
import org.apache.kafka.clients.producer.Producer;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.PoisonPill;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.perfluencer.kafka.actors.KafkaProducerActor;
import pl.perfluencer.kafka.cache.RequestStore;
import pl.perfluencer.kafka.javaapi.KafkaProtocolBuilder;
import pl.perfluencer.kafka.util.SerializationType;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class StoreFailureTest {

    private KafkaProtocolBuilder.KafkaProtocol kafkaProtocol;
    private CoreComponents coreComponents;
    private Producer<String, Object> mockProducer;
    private RequestStore mockStore;
    private ActorRef mockControllerActor;

    @Before
    public void setUp() throws Exception {
        kafkaProtocol = mock(KafkaProtocolBuilder.KafkaProtocol.class);
        coreComponents = mock(CoreComponents.class);
        mockProducer = mock(Producer.class);
        ActorSystem system = ActorSystem.create("TestSystem");
        when(kafkaProtocol.getActorSystem()).thenReturn(system);

        // Mock Producer Router
        // Use generic casting to avoid lint errors with specific generic types in mocks
        // if possible,
        // or just suppression.
        ActorRef producerRouter = system.actorOf(KafkaProducerActor.props(mockProducer));
        when(kafkaProtocol.getProducerRouter()).thenReturn(producerRouter);

        // Mock RequestStore to throw exception
        mockStore = mock(RequestStore.class);
        doThrow(new RuntimeException("Simulated Store Failure")).when(mockStore).storeRequest(any(), any(), any(),
                any(),
                any(), any(), any(Long.class), any(Long.class));
        when(kafkaProtocol.getRequestStore()).thenReturn(mockStore);

        // Mock Controller ActorRef
        // Mock Controller ActorRef
        // CoreComponents.controller() returns io.gatling.core.actor.ActorRef
        // (interface)
        // But our code casts it to org.apache.pekko.actor.ActorRef (class) to use
        // 'tell'
        // So we need a mock that implements BOTH.
        mockControllerActor = mock(ActorRef.class,
                withSettings().extraInterfaces(io.gatling.core.actor.ActorRef.class));
        doReturn(mockControllerActor).when(coreComponents).controller();

        StatsEngine statsEngine = mock(StatsEngine.class);
        when(coreComponents.statsEngine()).thenReturn(statsEngine);
        io.gatling.commons.util.Clock clock = mock(io.gatling.commons.util.Clock.class);
        when(coreComponents.clock()).thenReturn(clock);
    }

    @After
    public void tearDown() throws Exception {
        if (kafkaProtocol != null && kafkaProtocol.getActorSystem() != null) {
            kafkaProtocol.getActorSystem().terminate();
        }
    }

    @Test
    public void testStoreFailureStopsTest() {
        Action nextAction = mock(Action.class);
        KafkaRequestReplyAction action = new KafkaRequestReplyAction(
                "requestName", kafkaProtocol, coreComponents, nextAction,
                "req-topic", session -> "key", session -> "value", Collections.emptyMap(),
                SerializationType.STRING, true, 1000, TimeUnit.MILLISECONDS);

        Session session = mock(Session.class);
        when(session.scenario()).thenReturn("test-scenario");

        action.execute(session);

        // Verify that controller.tell() was called with PoisonPill
        verify(mockControllerActor).tell(eq(PoisonPill.getInstance()), any());

        // Verify session was marked as failed
        verify(session).markAsFailed();
    }
}
