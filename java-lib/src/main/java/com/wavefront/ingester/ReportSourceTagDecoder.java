package com.wavefront.ingester;

import com.google.common.collect.ImmutableList;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceTagAction;

import java.util.List;

import static com.wavefront.ingester.AbstractIngesterFormatter.SOURCE_DESCRIPTION_LITERAL;
import static com.wavefront.ingester.AbstractIngesterFormatter.SOURCE_TAG_LITERAL;

/**
 * This class is used to decode the source tags sent by the clients.
 *
 * [@SourceTag action=save source=source sourceTag1 sourceTag2]
 * [@SourceDescription action=save source=source description=Description]
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class ReportSourceTagDecoder implements ReportableEntityDecoder<String, ReportSourceTag>{

  private static final AbstractIngesterFormatter<ReportSourceTag> FORMAT =
      ReportSourceTagIngesterFormatter.newBuilder()
          .whiteSpace()
          .appendCaseSensitiveLiterals(new String[]{SOURCE_TAG_LITERAL, SOURCE_DESCRIPTION_LITERAL})
          .whiteSpace()
          .appendLoopOfKeywords()
          .whiteSpace()
          .appendLoopOfValues()
          .build();

  @Override
  public void decode(String msg, List<ReportSourceTag> out, String customerId) {
    ReportSourceTag tag = FORMAT.drive(msg, "dummy", customerId, null);
    if (out == null) return;
    if ((tag.getAction() == SourceTagAction.ADD || tag.getAction() == SourceTagAction.DELETE) &&
        tag.getAnnotations().size() > 1) {
      tag.getAnnotations().forEach(x -> out.add(ReportSourceTag.newBuilder(tag).
          setAnnotations(ImmutableList.of(x)).build()));
    } else {
      out.add(tag);
    }
  }
}
