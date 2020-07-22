package com.wavefront.ingester;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A decoder for input data. A more generic version of {@link Decoder},
 * that supports other object types besides points.
 *
 * @author vasily@wavefront.com
 */
public interface ReportableEntityDecoder<T, E> {
  /**
   * Decode graphite points and dump them into an output array. The supplied customer id will be set
   * and no customer id extraction will be attempted.
   *
   * @param msg         Message to parse.
   * @param out         List to output the parsed point.
   * @param customerId  The customer id to use as the table for the result ReportPoint.
   * @param ctx         The ingester context with extra params for decoding.
   */
  void decode(T msg, List<E> out, String customerId, @Nullable IngesterContext ctx);

  /**
   * Decode entities (points, spans, etc) and dump them into an output array. The supplied customer
   * id will be set and no customer id extraction will be attempted.
   *
   * @param msg        Message to parse.
   * @param out        List to output the parsed point.
   * @param customerId The customer id to use as the table for the resulting entities.
   */
  default void decode(T msg, List<E> out, String customerId) {
    decode(msg, out, customerId, null);
  }

  /**
   * Certain decoders support decoding the customer id from the input line itself.
   *
   * @param msg Message to parse.
   * @param out List to output the parsed point.
   */
  default void decode(T msg, List<E> out) {
    decode(msg, out, "dummy");
  }
}
