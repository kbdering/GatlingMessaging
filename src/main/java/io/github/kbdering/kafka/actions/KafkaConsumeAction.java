package io.github.kbdering.kafka.actions;

import io.gatling.commons.stats.Status;
import io.gatling.core.action.Action;
import io.gatling.core.stats.StatsEngine;
import io.gatling.core.session.Session;
import io.gatling.core.CoreComponents;
import io.github.kbdering.kafka.actors.KafkaRawConsumerActor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.pattern.Patterns;
import org.apache.pekko.util.Timeout;
import scala.concurrent.Future;
import scala.collection.immutable.List$;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaConsumeAction implements Action {

    private final String requestName;
    private final ActorRef consumerActor;
    private final CoreComponents coreComponents;
    private final Action next;
    private final long timeout;
    private final TimeUnit timeUnit;

    public KafkaConsumeAction(String requestName, ActorRef consumerActor, CoreComponents coreComponents, Action next,
            long timeout, TimeUnit timeUnit) {
        this.requestName = requestName;
        this.consumerActor = consumerActor;
        this.coreComponents = coreComponents;
        this.next = next;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @Override
    public String name() {
        return "kafka-consume-action";
    }

    @Override
    public void execute(Session session) {
        StatsEngine statsEngine = coreComponents.statsEngine();
        long startTime = coreComponents.clock().nowMillis();

        Timeout askTimeout = new Timeout(timeout, timeUnit);
        Future<Object> future = Patterns.ask(consumerActor, new KafkaRawConsumerActor.GetMessage(), askTimeout);

        java.util.concurrent.CompletionStage<Object> javaFuture = scala.jdk.javaapi.FutureConverters.asJava(future);

        javaFuture.whenComplete((response, exception) -> {
            long endTime = coreComponents.clock().nowMillis();
            if (exception != null) {
                handleFailure(session, statsEngine, startTime, endTime, exception);
            } else {
                if (response instanceof ConsumerRecord) {
                    ConsumerRecord<?, ?> record = (ConsumerRecord<?, ?>) response;
                    // Here we could apply checks if we had them.
                    // For now, we just log success.
                    // We might want to store the record in the session?
                    // But Action.execute is void and next.execute takes the session.
                    // We can return a new session with the record.

                    // TODO: Add checks support. For now, just raw consume.

                    statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), requestName, startTime,
                            endTime, Status.apply("OK"),
                            scala.Some.apply("200"), scala.Some.apply(""));
                    next.execute(session);
                } else {
                    handleFailure(session, statsEngine, startTime, endTime,
                            new RuntimeException("Unknown response: " + response));
                }
            }
        });
    }

    private void handleFailure(Session session, StatsEngine statsEngine, long startTime, long endTime, Throwable e) {
        session.markAsFailed();
        statsEngine.logResponse(session.scenario(), List$.MODULE$.empty(), name(), startTime, endTime,
                Status.apply("KO"),
                scala.Some.apply("500"), scala.Some.apply("ERROR: " + e.getMessage()));
        next.execute(session);
    }

    @Override
    public void com$typesafe$scalalogging$StrictLogging$_setter_$logger_$eq(com.typesafe.scalalogging.Logger x$1) {
    }

    @Override
    public com.typesafe.scalalogging.Logger logger() {
        return null;
    }
}
