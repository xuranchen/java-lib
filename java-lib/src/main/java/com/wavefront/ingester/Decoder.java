package com.wavefront.ingester;

import java.util.List;

import wavefront.report.ReportPoint;

/**
 * A decoder of an input line.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
@Deprecated
public interface Decoder<T> {
  /**
   * Decode graphite points and dump them into an output array. The supplied customer id will be set
   * and no customer id extraction will be attempted.
   *
   * @param msg        Message to parse.
   * @param out        List to output the parsed point.
   * @param customerId The customer id to use as the table for the result ReportPoint.
   */
  void decodeReportPoints(T msg, List<ReportPoint> out, String customerId);

  /**
   * Decode graphite points and dump them into an output array. The supplied customer id will be set
   * and no customer id extraction will be attempted.
   *
   * @param msg             Message to parse.
   * @param out             List to output the parsed point.
   * @param customerId      The customer id to use as the table for the result ReportPoint.
   * @param ingesterContext The ingester context with extra params for decoding.
   */
  void decodeReportPoints(T msg, List<ReportPoint> out, String customerId,
                          IngesterContext ingesterContext);

  /**
   * Certain decoders support decoding the customer id from the input line itself.
   *
   * @param msg Message to parse.
   * @param out List to output the parsed point.
   */
  void decodeReportPoints(T msg, List<ReportPoint> out);
}
