package com.wavefront.common;

/**
 * Metric constants.
 *
 * @author Vikram Raman (vikram@wavefront.com)
 */
public abstract class MetricConstants {
  public static final char DELTA_PREFIX_CHAR = '\u2206'; // ∆: INCREMENT
  public static final char DELTA_PREFIX_CHAR_2 = '\u0394'; // Δ: GREEK CAPITAL LETTER DELTA

  public static final String DELTA_PREFIX = Character.toString(DELTA_PREFIX_CHAR);
  public static final String DELTA_PREFIX_2 = Character.toString(DELTA_PREFIX_CHAR_2);
}
