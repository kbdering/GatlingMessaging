package io.github.kbdering.kafka;

import java.util.Optional;
import java.util.function.BiFunction;

public class MessageCheck<Req, Res> {

    private final BiFunction<Req, Res, Optional<String>> check;
    public MessageCheck(BiFunction<Req, Res, Optional<String>> check) {
        this.check = check;
    }
    public Optional<String> check(Res message){
        return check.apply(null, message);
    }

    public Optional<String> check(Req request, Res response){
        return check.apply(request, response);
    }
}
