package com.wavefront.ingester;

import com.google.common.collect.ImmutableList;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

import java.util.List;
import java.util.Map;

import static com.wavefront.ingester.AbstractIngesterFormatter.SOURCE_DESCRIPTION_LITERAL;
import static com.wavefront.ingester.AbstractIngesterFormatter.SOURCE_TAG_LITERAL;

/**
 * This class is used to decode the source tags sent by the clients.
 *
 * [@SourceTag action=save source=source sourceTag1 sourceTag2]
 * [@SourceDescription action=save source=source description=Description]
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 * @author vasily@wavefront.com
 */
public class ReportSourceTagDecoder implements ReportableEntityDecoder<String, ReportSourceTag> {
  private static final String SOURCE = "source";
  private static final String ACTION = "action";
  private static final String ACTION_ADD = "add";
  private static final String ACTION_SAVE = "save";
  private static final String ACTION_DELETE = "delete";

  private static final AbstractIngesterFormatter<ReportSourceTag> FORMAT =
      ReportSourceTagIngesterFormatter.newBuilder().
          caseSensitiveLiterals(ImmutableList.of(SOURCE_TAG_LITERAL, SOURCE_DESCRIPTION_LITERAL),
              ReportSourceTagDecoder::setOperation).
          annotationMap(ReportSourceTagDecoder::setKeywords, 2).
          textList(ReportSourceTag::setAnnotations).
          build();

  @Override
  public void decode(String msg, List<ReportSourceTag> out, String customerId,
                     IngesterContext ctx) {
    ReportSourceTag tag = FORMAT.drive(msg, null, customerId, null, null, null, null, null, ctx);
    if (out == null) return;
    if ((tag.getAction() == SourceTagAction.ADD || tag.getAction() == SourceTagAction.DELETE) &&
        tag.getAnnotations().size() > 1) {
      tag.getAnnotations().forEach(x -> out.add(ReportSourceTag.newBuilder(tag).
          setAnnotations(ImmutableList.of(x)).build()));
    } else {
      out.add(tag);
    }
  }

  private static void setOperation(ReportSourceTag target, String operation) {
    switch (operation) {
      case SOURCE_TAG_LITERAL:
        target.setOperation(SourceOperationType.SOURCE_TAG);
        return;
      case SOURCE_DESCRIPTION_LITERAL:
        target.setOperation(SourceOperationType.SOURCE_DESCRIPTION);
        return;
      default:
        throw new IllegalArgumentException("Literal " + operation + " is not allowed!");
    }
  }

  private static void setKeywords(ReportSourceTag target, Map<String, String> annotations) {
    String action = annotations.get(ACTION);
    if (action == null) {
      throw new IllegalArgumentException("No 'action' provided");
    }
    switch (action.toLowerCase()) {
      case ACTION_ADD:
        target.setAction(SourceTagAction.ADD);
        break;
      case ACTION_SAVE:
        target.setAction(SourceTagAction.SAVE);
        break;
      case ACTION_DELETE:
        target.setAction(SourceTagAction.DELETE);
        break;
      default:
        throw new IllegalArgumentException("Invalid action '" + action + "'!");
    }
    String source = annotations.get(SOURCE);
    if (source == null) {
      throw new IllegalArgumentException("No 'source' provided");
    }
    target.setSource(source);
  }
}
