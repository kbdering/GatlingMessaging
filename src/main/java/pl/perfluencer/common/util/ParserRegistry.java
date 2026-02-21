/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.perfluencer.common.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Registry for pre-registered parsers to avoid reflection overhead on hot
 * paths.
 * 
 * <p>
 * Users can register Protobuf, Avro, or custom parsers at protocol setup time,
 * and the framework will use direct function calls instead of reflection during
 * message processing.
 * 
 * <p>
 * Example usage:
 * 
 * <pre>{@code
 * ParserRegistry registry = new ParserRegistry();
 * registry.register(OrderRequest.class, OrderRequest::parseFrom);
 * registry.register(OrderResponse.class, OrderResponse::parseFrom);
 * }</pre>
 * 
 * @author Jakub Dering
 */
public class ParserRegistry {

    private final ConcurrentHashMap<Class<?>, Function<byte[], ?>> parsers = new ConcurrentHashMap<>();

    /**
     * Registers a parser function for a specific class type.
     * 
     * @param clazz  the class type to register
     * @param parser function that converts byte[] to the target type
     * @param <T>    the target type
     */
    public <T> void register(Class<T> clazz, Function<byte[], T> parser) {
        if (clazz == null || parser == null) {
            throw new IllegalArgumentException("Class and parser cannot be null");
        }
        parsers.put(clazz, parser);
    }

    /**
     * Parses byte array to the specified class using a registered parser.
     * 
     * @param clazz the target class type
     * @param data  the byte array to parse
     * @param <T>   the target type
     * @return parsed object, or null if no parser is registered for this class
     */
    @SuppressWarnings("unchecked")
    public <T> T parse(Class<T> clazz, byte[] data) {
        Function<byte[], ?> parser = parsers.get(clazz);
        if (parser != null) {
            return (T) parser.apply(data);
        }
        return null;
    }

    /**
     * Checks if a parser is registered for the given class.
     * 
     * @param clazz the class to check
     * @return true if a parser is registered
     */
    public boolean hasParser(Class<?> clazz) {
        return parsers.containsKey(clazz);
    }

    /**
     * Returns the number of registered parsers.
     * 
     * @return count of registered parsers
     */
    public int size() {
        return parsers.size();
    }

    /**
     * Clears all registered parsers.
     */
    public void clear() {
        parsers.clear();
    }
}
