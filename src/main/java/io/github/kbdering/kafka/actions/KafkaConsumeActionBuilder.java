package io.github.kbdering.kafka.actions;

import io.gatling.core.action.Action;
import io.gatling.javaapi.core.ActionBuilder;
import io.gatling.core.structure.ScenarioContext;
import io.github.kbdering.kafka.javaapi.KafkaProtocolBuilder;
import io.gatling.core.protocol.Protocol;
import org.apache.pekko.actor.ActorRef;

import java.util.concurrent.TimeUnit;

public class KafkaConsumeActionBuilder implements ActionBuilder {

    private final String requestName;
    private final String topic;
    private final long timeout;
    private final TimeUnit timeUnit;

    private String saveAsKey;

    public KafkaConsumeActionBuilder(String requestName, String topic) {
        this(requestName, topic, 30, TimeUnit.SECONDS);
    }

    public KafkaConsumeActionBuilder(String requestName, String topic, long timeout, TimeUnit timeUnit) {
        this.requestName = requestName;
        this.topic = topic;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    public KafkaConsumeActionBuilder saveAs(String key) {
        this.saveAsKey = key;
        return this;
    }

    @Override
    public io.gatling.core.action.builder.ActionBuilder asScala() {
        return new io.gatling.core.action.builder.ActionBuilder() {
            @Override
            public Action build(ScenarioContext ctx, Action next) {
                Protocol protocol = ctx.protocolComponentsRegistry()
                        .components(KafkaProtocolBuilder.KafkaProtocolComponents.protocolKey).protocol();
                // We need to get the consumer actor for this topic.
                // The protocol should probably manage these actors.
                // Or we create one if not exists?
                // For now, let's assume the protocol has a way to get/create a raw consumer
                // actor.

                // Since we haven't updated KafkaProtocol to support raw consumers yet, we need
                // to do that.
                // Let's assume we can get it from the protocol.
                ActorRef consumerActor = ((KafkaProtocolBuilder.KafkaProtocol) protocol).getRawConsumerActor(topic);

                return new KafkaConsumeAction(requestName, consumerActor, ctx.coreComponents(), next, timeout,
                        timeUnit, saveAsKey);
            }
        };
    }
}
