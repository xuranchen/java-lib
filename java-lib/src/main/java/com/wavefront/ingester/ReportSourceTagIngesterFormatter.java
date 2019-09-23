package com.wavefront.ingester;

import com.google.common.collect.ImmutableSet;

import org.antlr.v4.runtime.Token;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import wavefront.report.ReportSourceTag;

/**
 * This class can be used to parse sourceTags and description.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class ReportSourceTagIngesterFormatter extends AbstractIngesterFormatter<ReportSourceTag> {

  static final String SOURCE = "source";
  static final String DESCRIPTION = "description";
  static final String ACTION = "action";
  public static final String ACTION_ADD = "add";
  public static final String ACTION_SAVE = "save";
  public static final String ACTION_DELETE = "delete";
  static final Set<String> VALID_ACTIONS = ImmutableSet.of(ACTION_ADD, ACTION_SAVE, ACTION_DELETE);

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
    if (!queue.isEmpty()) {
      throw new RuntimeException("Could not parse: " + input);
    }
    Map<String, String> annotations = wrapper.getAnnotationMap();
    for (Map.Entry<String, String> entry : annotations.entrySet()) {
      switch (entry.getKey()) {
        case ReportSourceTagIngesterFormatter.ACTION:
          sourceTag.setAction(entry.getValue());
          break;
        case ReportSourceTagIngesterFormatter.SOURCE:
          sourceTag.setSource(entry.getValue());
          break;
        case ReportSourceTagIngesterFormatter.DESCRIPTION:
          sourceTag.setDescription(entry.getValue());
          break;
        default:
          throw new RuntimeException("Unknown tag key = " + entry.getKey() + " specified.");
      }
    }

    // verify the values - especially 'action' field
    if (sourceTag.getSource() == null)
      throw new RuntimeException("No source key was present in the input: " + input);

    final String action = sourceTag.getAction();
    if (action == null) {
      throw new RuntimeException("No action key was present in the input: " + input);
    }
    // verify it's a valid action one of 'add', 'save' or 'delete'
    if (!VALID_ACTIONS.contains(action)) {
      throw new RuntimeException("Action string did not match save/delete: " + input);
    }
    if (sourceTag.getSourceTagLiteral().equals("SourceTag") && sourceTag.getAnnotations() == null) {
      throw new RuntimeException("No tag(s) provided for action type `" + action + "`");
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
