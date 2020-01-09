package com.wavefront.ingester;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import wavefront.report.ReportSourceTag;
import wavefront.report.SourceTagAction;
import wavefront.report.SourceOperationType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class is used to test the source tag points
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public class ReportSourceTagDecoderTest {
  private static final Logger logger = Logger.getLogger(ReportSourceTagDecoderTest.class
      .getCanonicalName());

  @Test
  public void testSimpleSourceTagFormat() throws Exception {
    ReportSourceTagDecoder decoder = new ReportSourceTagDecoder();
    List<ReportSourceTag> out = new ArrayList<>();
    // Testwith 3sourceTags
    decoder.decode("@SourceTag action=save source=aSource sourceTag1 sourceTag2 sourceTag3", out);
    ReportSourceTag reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", SourceTagAction.SAVE,
        reportSourceTag.getAction());
    assertEquals("Source did not match.", "aSource", reportSourceTag.getSource());
    assertTrue("SourceTag1 did not match.", reportSourceTag.getAnnotations().contains
        ("sourceTag1"));

    // Test with one sourceTag
    out.clear();
    decoder.decode("@SourceTag action = save source = \"A Source\" sourceTag3", out);
    reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", SourceTagAction.SAVE,
        reportSourceTag.getAction());
    assertEquals("Source did not match.", "A Source", reportSourceTag.getSource());
    assertTrue("SourceTag3 did not match.", reportSourceTag.getAnnotations()
        .contains("sourceTag3"));

    // Test with a multi-word source tag
    out.clear();
    decoder.decode("@SourceTag action = save source=aSource \"A source tag\" " +
        "\"Another tag\"", out);
    reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", SourceTagAction.SAVE, reportSourceTag.getAction());
    assertEquals("Source did not match.", "aSource", reportSourceTag.getSource());
    assertTrue("'A source tag' did not match.", reportSourceTag.getAnnotations()
        .contains("A source tag"));
    assertTrue("'Another tag' did not match", reportSourceTag.getAnnotations()
        .contains("Another tag"));

    // Test sourceTag without any action -- this should result in an exception
    String msg = "@SourceTag source=aSource sourceTag4 sourceTag5";
    out.clear();
    boolean isException = false;
    try {
      decoder.decode(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception for SourceTag message without an action for input : " +
        msg, isException);

    // Test sourceTag without any source -- this should result in an exception
    msg = "@SourceTag action=save \"desc\" sourceTag5";
    out.clear();
    isException = false;
    try {
      decoder.decode(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception for SourceTag message without a source for input : " +
        msg, isException);

    // Test sourceTag message without the source field -- should throw an exception
    out.clear();
    isException = false;
    msg = "@SourceTag action=remove sourceTag3";
    try {
      decoder.decode(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception when source field was absent for input: " + msg,
        isException);

    // Test a message where action is not save or delete -- should throw an exception
    out.clear();
    isException = false;
    msg = "@SourceTag action = anAction source=aSource sourceTag2 sourceTag3";
    try {
      decoder.decode(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Did not see an exception when action field was invalid for input : " + msg,
        isException);
  }

  @Test
  public void testSimpleSourceDescriptions() throws Exception {
    ReportSourceTagDecoder decoder = new ReportSourceTagDecoder();
    List<ReportSourceTag> out = new ArrayList<>();
    // Testwith source description
    decoder.decode("@SourceDescription action=save source= aSource \"desc with space\"", out);
    ReportSourceTag reportSourceTag = out.get(0);
    assertEquals("Action name didn't match.", SourceTagAction.SAVE,
        reportSourceTag.getAction());
    assertEquals("Source did not match.", "aSource", reportSourceTag.getSource());
    assertEquals("Description did not match.", "desc with space",
        reportSourceTag.getAnnotations().get(0));

    // Test delete action where description field is not necessary
    out.clear();
    decoder.decode("@SourceDescription action=delete source=aSource", out);
    reportSourceTag = out.get(0);
    assertEquals("Action name did not match", SourceTagAction.DELETE, reportSourceTag.getAction());
    assertEquals("Source did not match", "aSource", reportSourceTag.getSource());

    // Add a source tag to the SourceDescription message -- this should cause an exception
    out.clear();
    String msg = "@SourceDescription action = save source = aSource desc sourceTag4";
    boolean isException = false;
    try {
      decoder.decode(msg, out);
    } catch (Exception ex) {
      isException = true;
      logger.info(ex.getMessage());
    }
    assertTrue("Expected an exception, since source tag was supplied in SourceDescription " +
        "message for input : " + msg, isException);
  }

  @Test
  public void testExpandSourceTagAddDeleteWithMultipleTags() {
    // Test sourceTag add with 3 tags -- this should generate 3 ReportSourceTag objects
    ReportSourceTagDecoder decoder = new ReportSourceTagDecoder();
    List<ReportSourceTag> out = new ArrayList<>();
    decoder.decode("@SourceTag action=add source=aSource sourceTag1 sourceTag2 sourceTag3", out);
    assertEquals(3, out.size());

    assertEquals(SourceOperationType.SOURCE_TAG, out.get(0).getOperation());
    assertEquals(SourceTagAction.ADD, out.get(0).getAction());
    assertEquals("aSource", out.get(0).getSource());
    assertEquals(1, out.get(0).getAnnotations().size());
    assertEquals("sourceTag1", out.get(0).getAnnotations().get(0));

    assertEquals(SourceOperationType.SOURCE_TAG, out.get(1).getOperation());
    assertEquals(SourceTagAction.ADD, out.get(1).getAction());
    assertEquals("aSource", out.get(1).getSource());
    assertEquals(1, out.get(1).getAnnotations().size());
    assertEquals("sourceTag2", out.get(1).getAnnotations().get(0));

    assertEquals(SourceOperationType.SOURCE_TAG, out.get(2).getOperation());
    assertEquals(SourceTagAction.ADD, out.get(2).getAction());
    assertEquals("aSource", out.get(2).getSource());
    assertEquals(1, out.get(2).getAnnotations().size());
    assertEquals("sourceTag3", out.get(2).getAnnotations().get(0));

    out.clear();

    decoder.decode("@SourceTag action=delete source=aSource sourceTag4 sourceTag5", out);
    assertEquals(2, out.size());

    assertEquals(SourceOperationType.SOURCE_TAG, out.get(0).getOperation());
    assertEquals(SourceTagAction.DELETE, out.get(0).getAction());
    assertEquals("aSource", out.get(0).getSource());
    assertEquals(1, out.get(0).getAnnotations().size());
    assertEquals("sourceTag4", out.get(0).getAnnotations().get(0));

    assertEquals(SourceOperationType.SOURCE_TAG, out.get(1).getOperation());
    assertEquals(SourceTagAction.DELETE, out.get(1).getAction());
    assertEquals("aSource", out.get(1).getSource());
    assertEquals(1, out.get(1).getAnnotations().size());
    assertEquals("sourceTag5", out.get(1).getAnnotations().get(0));
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidSourceTagAddWithNoTagsThrows() {
    // Test sourceTag add with no tags -- this should result in an exception
    new ReportSourceTagDecoder().decode("@SourceTag action=add source=source",
        new ArrayList<>());
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidSourceTagDeleteWithNoTagsThrows() {
    // Test sourceTag delete with no tags -- this should result in an exception
    new ReportSourceTagDecoder().decode("@SourceTag action=delete source=source",
        new ArrayList<>());
  }
}
