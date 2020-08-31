package com.wavefront.common;

import java.util.function.Predicate;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RunAutomaton;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by clement on 2/24/17.
 */
public class PatternMatchPredicateTest {

  @Test
  public void testAutomaton() {
    Automaton empty = new Automaton();
    assertFalse(empty.run("test"));
    assertFalse(empty.run(""));
    Automaton literal = Automaton.makeString("hello");
    assertFalse(literal.run("test"));
    assertFalse(literal.run(""));
    assertTrue(literal.run("hello"));
    Automaton pattern = Automaton.concatenate(ImmutableList.of(
        Automaton.makeString("hello"),
        Automaton.makeAnyString(),
        Automaton.makeString("world")));
    assertFalse(pattern.run("test"));
    assertFalse(pattern.run(""));
    assertFalse(pattern.run("hello"));
    assertTrue(pattern.run("helloworld"));
    assertTrue(pattern.run("hellothisworld"));
    assertTrue(pattern.run("hello_this_world"));
    assertTrue(pattern.run("helloworworld"));
  }

  @Test
  public void testPatternMatch() {
    RunAutomaton test = PatternMatchPredicate.makeAutomaton("test");
    assertTrue(test.run("test"));
    assertFalse(test.run("tes"));
    test = PatternMatchPredicate.makeAutomaton("");
    assertTrue(test.run(""));
    test = PatternMatchPredicate.makeAutomaton("hello*world*hello*world");
    assertTrue(test.run("helloworldhelloworld"));
    assertTrue(test.run("helloworworldldhelloworld"));
    assertFalse(test.run("helloworld"));
    assertFalse(test.run("helloworldhelloworl"));
    assertFalse(test.run("2helloworldhelloworld"));
    assertFalse(test.run("helloworldhelloworld2"));
    test = PatternMatchPredicate.makeAutomaton("*auth*prod*");
    assertTrue(test.run("authaskdfkkl.auth.2490349prod.prod"));
    assertTrue(test.run("authprod"));
    assertTrue(test.run("auth.2490349prod"));
    assertFalse(test.run("prod.auth.auth"));
  }

  @Test
  public void testPredicate() {
    Predicate<String> test = PatternMatchPredicate.buildPredicate("test", false);
    assertTrue(test.test("test"));
    assertFalse(test.test("Test"));
    assertFalse(test.test("tesT"));
    assertFalse(test.test("atest"));
    assertFalse(test.test("testa"));

    test = PatternMatchPredicate.buildPredicate("test", true);
    assertTrue(test.test("test"));
    assertTrue(test.test("Test"));
    assertTrue(test.test("tesT"));
    assertFalse(test.test("atest"));
    assertFalse(test.test("testa"));

    test = PatternMatchPredicate.buildPredicate("*test", false);
    assertTrue(test.test("test"));
    assertFalse(test.test("Test"));
    assertFalse(test.test("tesT"));
    assertTrue(test.test("atest"));
    assertFalse(test.test("aTest"));
    assertFalse(test.test("aTest"));
    assertTrue(test.test("Atest"));
    assertFalse(test.test("testa"));

    test = PatternMatchPredicate.buildPredicate("*test", true);
    assertTrue(test.test("test"));
    assertTrue(test.test("Test"));
    assertTrue(test.test("tesT"));
    assertTrue(test.test("atest"));
    assertTrue(test.test("aTest"));
    assertTrue(test.test("Atest"));
    assertFalse(test.test("testa"));

    test = PatternMatchPredicate.buildPredicate("test*", false);
    assertTrue(test.test("test"));
    assertFalse(test.test("Test"));
    assertFalse(test.test("tesT"));
    assertFalse(test.test("atest"));
    assertFalse(test.test("aTest"));
    assertTrue(test.test("testa"));

    test = PatternMatchPredicate.buildPredicate("test*", true);
    assertTrue(test.test("test"));
    assertTrue(test.test("Test"));
    assertTrue(test.test("tesT"));
    assertFalse(test.test("atest"));
    assertTrue(test.test("testa"));
    assertTrue(test.test("tesTa"));
    assertTrue(test.test("testA"));

    test = PatternMatchPredicate.buildPredicate("*test*", false);
    assertTrue(test.test("test"));
    assertFalse(test.test("Test"));
    assertFalse(test.test("tesT"));
    assertTrue(test.test("atest"));
    assertFalse(test.test("aTest"));
    assertTrue(test.test("testa"));

    test = PatternMatchPredicate.buildPredicate("*test*", true);
    assertTrue(test.test("test"));
    assertTrue(test.test("Test"));
    assertTrue(test.test("tesT"));
    assertTrue(test.test("atest"));
    assertTrue(test.test("testa"));
    assertTrue(test.test("tesTa"));
    assertTrue(test.test("testA"));

    test = PatternMatchPredicate.buildPredicate("hello*world*hello*world", false);
    assertTrue(test.test("helloworldhelloworld"));
    assertTrue(test.test("helloworworldldhelloworld"));
    assertFalse(test.test("helloworld"));
    assertFalse(test.test("helloworldhelloworl"));
    assertFalse(test.test("2helloworldhelloworld"));
    assertFalse(test.test("helloworldhelloworld2"));
    test = PatternMatchPredicate.buildPredicate("*auth*prod*", false);
    assertTrue(test.test("authaskdfkkl.auth.2490349prod.prod"));
    assertTrue(test.test("authprod"));
    assertTrue(test.test("auth.2490349prod"));
    assertFalse(test.test("prod.auth.auth"));
  }
}
