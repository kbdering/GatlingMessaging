package pl.perfluencer.kafka.actions;

import io.gatling.core.action.Action;
import io.gatling.core.CoreComponents;
import io.gatling.core.session.Session;

/**
 * A pass-through Gatling action for consume-only mode.
 *
 * <p>
 * The actual consumption and check validation happens in the background
 * via {@code KafkaConsumerThread} in consume-only mode. This action simply
 * advances the Gatling scenario to the next step.
 * </p>
 */
public class KafkaConsumeAction implements Action {

    private final String requestName;
    private final CoreComponents coreComponents;
    private final Action next;

    public KafkaConsumeAction(String requestName, CoreComponents coreComponents, Action next) {
        this.requestName = requestName;
        this.coreComponents = coreComponents;
        this.next = next;
    }

    @Override
    public String name() {
        return "kafka-consume-" + requestName;
    }

    @Override
    public void execute(Session session) {
        // Consumer threads handle consumption in the background.
        // This action just advances the scenario.
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
