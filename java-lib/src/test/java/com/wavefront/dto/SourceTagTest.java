package com.wavefront.dto;

import com.wavefront.ingester.ReportSourceTagDecoder;
import org.junit.Test;
import wavefront.report.ReportSourceTag;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author vasily@wavefront.com
 */
public class SourceTagTest {
  @Test
  public void testMultipleTags() {
    ReportSourceTagDecoder decoder = new ReportSourceTagDecoder();
    List<ReportSourceTag> out = new ArrayList<>();
    decoder.decode("@SourceTag action=delete source=aSource sourceTag4 sourceTag5", out);
    assertEquals(2, out.size());
    assertEquals("@SourceTag action=delete source=\"aSource\" \"sourceTag4\"",
        new SourceTag(out.get(0)).toString());
    assertEquals("@SourceTag action=delete source=\"aSource\" \"sourceTag5\"",
        new SourceTag(out.get(1)).toString());
  }

  @Test
  public void testSimpleSourceDescriptions() throws Exception {
    ReportSourceTagDecoder decoder = new ReportSourceTagDecoder();
    List<ReportSourceTag> out = new ArrayList<>();
    decoder.decode("@SourceDescription action=save source= aSource \"desc with space\"", out);
    assertEquals("@SourceDescription action=save source=\"aSource\" \"desc with space\"",
        new SourceTag(out.get(0)).toString());

    // Test delete action where description field is not necessary
    out.clear();
    decoder.decode("@SourceDescription action=delete source=aSource", out);
    assertEquals("@SourceDescription action=delete source=\"aSource\"",
        new SourceTag(out.get(0)).toString());
  }
}