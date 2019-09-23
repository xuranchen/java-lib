package com.wavefront.ingester;

import java.util.List;

import com.google.common.collect.ImmutableList;
import wavefront.report.ReportSourceTag;

import static com.wavefront.ingester.ReportSourceTagIngesterFormatter.ACTION_ADD;
import static com.wavefront.ingester.ReportSourceTagIngesterFormatter.ACTION_DELETE;

/**
 * This class is used to decode the source tags sent by the clients.
 *
 * [@SourceTag action=save source=source sourceTag1 sourceTag2]
 * [@SourceDescription action=save source=source description=Description]
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class ReportSourceTagDecoder implements ReportableEntityDecoder<String, ReportSourceTag>{

  public static final String SOURCE_TAG = "@SourceTag";
  public static final String SOURCE_DESCRIPTION = "@SourceDescription";

  private static final AbstractIngesterFormatter<ReportSourceTag> FORMAT =
      ReportSourceTagIngesterFormatter.newBuilder()
          .whiteSpace()
          .appendCaseSensitiveLiterals(new String[]{SOURCE_TAG, SOURCE_DESCRIPTION})
          .whiteSpace()
          .appendLoopOfKeywords()
          .whiteSpace()
          .appendLoopOfValues()
          .build();

  @Override
  public void decode(String msg, List<ReportSourceTag> out, String customerId) {
    ReportSourceTag tag = FORMAT.drive(msg, "dummy", customerId, null);
    if (out == null) return;
    if ((tag.getAction().equals(ACTION_ADD) || tag.getAction().equals(ACTION_DELETE)) &&
        tag.getAnnotations().size() > 1) {
      tag.getAnnotations().forEach(x -> out.add(ReportSourceTag.newBuilder(tag).
          setAnnotations(ImmutableList.of(x)).build()));
    } else {
      out.add(tag);
    }
  }
}
