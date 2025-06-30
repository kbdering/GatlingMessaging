package io.github.kbdering.kafka;

import java.util.Optional;
import java.util.function.BiFunction;

public class MessageCheck<ReqT, ResT> { // Now generic

    private final String checkName;
    private final Class<ReqT> requestClass;
    private final SerializationType requestSerdeType;
    private final Class<ResT> responseClass;
    private final SerializationType responseSerdeType; // Expected response serialization type
    private final BiFunction<ReqT, ResT, Optional<String>> checkLogic;

    public MessageCheck(String checkName,
                        Class<ReqT> requestClass, SerializationType requestSerdeType,
                        Class<ResT> responseClass, SerializationType responseSerdeType,
                        BiFunction<ReqT, ResT, Optional<String>> checkLogic) {
        this.checkName = checkName;
        this.requestClass = requestClass;
        this.requestSerdeType = requestSerdeType;
        this.responseClass = responseClass;
        this.responseSerdeType = responseSerdeType;
        this.checkLogic = checkLogic;
    }

    public String getCheckName() {
        return checkName;
    }

    public Class<ReqT> getRequestClass() { return requestClass; }
    public SerializationType getRequestSerdeType() { return requestSerdeType; }
    public Class<ResT> getResponseClass() { return responseClass; }
    public SerializationType getResponseSerdeType() { return responseSerdeType; }
    public BiFunction<ReqT, ResT, Optional<String>> getCheckLogic() { return checkLogic; }

    // The actual execution of the check logic will be handled by MessageProcessorActor
    // after it performs deserialization.
}
    
