/*
 * Copyright 2015 George Aristy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.llorllale.liquibasefacade;

import org.llorllale.liquibasefacade.Version;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author George Aristy
 */
public class VersionTest {
  
  public VersionTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  @Test
  public void testOf() {
    int major = 1;
    int minor = 0;
    int release = 0;
    Version version = Version.of(major, minor, release);
    assertEquals(1, version.getMajor());
    assertEquals(0, version.getMinor());
    assertEquals(0, version.getRelease());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOfNegativeVersion() {
    Version.of(-1,0,0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroVersion() {
    Version.of(0,0,0);
  }

  @Test
  public void testComparisons() {
    Version ref = Version.of(2,0,0);
    Version test = Version.of(1,1,1);
    assertTrue(ref.isGreaterThan(test));
    assertTrue(test.isLessThan(ref));

    test = Version.of(2,0,0);
    assertTrue(ref.isEqualOrGreaterThan(test));
    assertTrue(ref.isEqualOrLessThan(test));
    assertTrue(ref.equals(test));
  }

  @Test
  public void testValueOf() {
    String s = "0.1.0";
    Version expResult = Version.of(0,1,0);
    Version result = Version.valueOf(s);
    assertEquals(expResult, result);
  }

  @Test
  public void testString() {
    Version instance = Version.of(2,1,1);
    String expResult = "2.1.1";
    String result = instance.string();
    assertEquals(expResult, result);
  }
}
