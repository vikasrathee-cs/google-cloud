package io.cdap.plugin.gcp.publisher.source;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Interface used to make a Java.util.function.Function serializable.
 * @param <T> Function input class
 * @param <U> Function output class
 */
public interface SerializableFunction<T, U> extends Function<T, U>, Serializable {
}
