package com.wavefront.ingester;

import org.antlr.v4.runtime.Token;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceTagAction;
import wavefront.report.SourceOperationType;

import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * This class can be used to parse sourceTags and description.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class ReportSourceTagIngesterFormatter extends AbstractIngesterFormatter<ReportSourceTag> {

  private static final String SOURCE = "source";
  private static final String ACTION = "action";
  private static final String ACTION_ADD = "add";
  private static final String ACTION_SAVE = "save";
  private static final String ACTION_DELETE = "delete";

  private ReportSourceTagIngesterFormatter(List<FormatterElement> elements) {
    super(elements);
  }

  /**
   * Factory method to create an instance of the format builder.
   *
   * @return The builder, which can be used to create the parser.
   */
  public static SourceTagIngesterFormatBuilder newBuilder() {
    return new SourceTagIngesterFormatBuilder();
  }

  /**
   * This method can be used to parse the input line into a ReportSourceTag object.
   *
   * @return The parsed ReportSourceTag object.
   */
  @Override
  public ReportSourceTag drive(String input, String defaultHostName, String customerId,
                               List<String> customerSourceTags) {
    Queue<Token> queue = getQueue(input);

    ReportSourceTag sourceTag = new ReportSourceTag();
    ReportSourceTagWrapper wrapper = new ReportSourceTagWrapper(sourceTag);
    try {
      for (FormatterElement element : elements) {
        element.consume(queue, wrapper);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not parse: " + input, ex);
    }
    String action = wrapper.getAnnotationMap().get(ReportSourceTagIngesterFormatter.ACTION);
    if (action == null) {
      throw new IllegalArgumentException("No action key was present in the input: " + input);
    }
    switch (action.toLowerCase()) {
      case ACTION_ADD:
        sourceTag.setAction(SourceTagAction.ADD);
        break;
      case ACTION_SAVE:
        sourceTag.setAction(SourceTagAction.SAVE);
        break;
      case ACTION_DELETE:
        sourceTag.setAction(SourceTagAction.DELETE);
        break;
      default:
        throw new IllegalArgumentException("Invalid action '" + action + "'!");
    }
    String source = wrapper.getAnnotationMap().get(ReportSourceTagIngesterFormatter.SOURCE);
    if (source == null) {
      throw new IllegalArgumentException("No source key was present in the input: " + input);
    }
    sourceTag.setSource(source);

    if (sourceTag.getAnnotations() == null || sourceTag.getAnnotations().isEmpty()) {
      if (!(sourceTag.getOperation() == SourceOperationType.SOURCE_DESCRIPTION &&
          sourceTag.getAction() == SourceTagAction.DELETE)) {
        throw new IllegalArgumentException("No data provided for operation `" +
            sourceTag.getOperation() + "` action: " + sourceTag.getAction());
      }
    } else if (sourceTag.getOperation() == SourceOperationType.SOURCE_DESCRIPTION &&
        sourceTag.getAnnotations().size() > 1) {
      throw new IllegalArgumentException("Only one description expected");
    }
    return ReportSourceTag.newBuilder(sourceTag).build();
  }

  /**
   * A builder pattern to create a format for the source tag parser.
   */
  public static class SourceTagIngesterFormatBuilder extends IngesterFormatBuilder<ReportSourceTag> {

    @Override
    public ReportSourceTagIngesterFormatter build() {
      return new ReportSourceTagIngesterFormatter(elements);
    }
  }
}
