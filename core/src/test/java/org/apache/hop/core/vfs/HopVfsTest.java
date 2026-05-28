/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.vfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class HopVfsTest {

  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @BeforeClass
  public static void setUpBeforeClass() throws HopException {
    HopClientEnvironment.init();
    Encr.init("Hop");
  }

  /**
   * Test to validate that startsWitScheme() returns true if the fileName starts with known protocol
   * like zip: jar: then it returns true else returns false
   */
  @Test
  public void testStartsWithScheme() {
    String fileName =
        "zip:file:///SavedLinkedres.zip!Calculate median and percentiles using the group by transforms.hpl";
    assertTrue(HopVfs.startsWithScheme(fileName, new Variables()));

    fileName =
        "SavedLinkedres.zip!Calculate median and percentiles using the group by transforms.hpl";
    assertFalse(HopVfs.startsWithScheme(fileName, new Variables()));
  }

  @Test
  public void testCheckForSchemeSuccess() {
    String[] schemes = {"hdfs"};
    String vfsFilename = "hdfs://company.com:8020/tmp/acltest/";

    boolean test = HopVfs.checkForScheme(schemes, true, vfsFilename);
    assertFalse(test);
  }

  @Test
  public void testCheckForSchemeFail() {
    String[] schemes = {"file"};
    String vfsFilename = "hdfs://company.com:8020/tmp/acltest/";

    boolean test = HopVfs.checkForScheme(schemes, true, vfsFilename);
    assertTrue(test);
  }

  @Test
  public void testRamFilesCache() throws Exception {
    String filename = "ram:///test-file.txt";
    FileObject fileObject = HopVfs.getFileObject(filename);
    try (OutputStream outputStream = fileObject.getContent().getOutputStream()) {
      outputStream.write("Test-content".getBytes());
    }
  }

  /** Test hop2vfsPasswordEncoder with null input */
  @Test
  public void testHop2VfsPasswordEncoder_Null() {
    String result = HopVfs.hop2vfsPasswordEncoder(null, new Variables());
    assertEquals(null, result);
  }

  /** Test hop2vfsPasswordEncoder with blank input */
  @Test
  public void testHop2VfsPasswordEncoder_Blank() {
    String result = HopVfs.hop2vfsPasswordEncoder("", new Variables());
    assertEquals("", result);

    result = HopVfs.hop2vfsPasswordEncoder("   ", new Variables());
    assertEquals("   ", result);
  }

  /** Test hop2vfsPasswordEncoder with non-URI input */
  @Test
  public void testHop2VfsPasswordEncoder_NotUri() {
    String input = "some random text";
    String result = HopVfs.hop2vfsPasswordEncoder(input, new Variables());
    assertEquals(input, result);
  }

  /** Test hop2vfsPasswordEncoder with URI without password */
  @Test
  public void testHop2VfsPasswordEncoder_NoPassword() {
    String input = "sftp://myuser@myserver/myfolder/myfile.txt";
    String result = HopVfs.hop2vfsPasswordEncoder(input, new Variables());
    assertEquals(input, result);
  }

  /** Test hop2vfsPasswordEncoder with URI without @ (invalid) */
  @Test
  public void testHop2VfsPasswordEncoder_InvalidUri() {
    String input = "sftp://myuser:mypass/myfolder/myfile.txt";
    String result = HopVfs.hop2vfsPasswordEncoder(input, new Variables());
    assertEquals(input, result);
  }

  /** Test hop2vfsPasswordEncoder with plain password (not Hop-encrypted) */
  @Test
  public void testHop2VfsPasswordEncoder_PlainPassword() {
    String input = "sftp://myuser:mypass@myserver/myfolder/myfile.txt";
    String result = HopVfs.hop2vfsPasswordEncoder(input, new Variables());
    assertEquals(input, result);
  }

  /** Test hop2vfsPasswordEncoder with null variables */
  @Test
  public void testHop2VfsPasswordEncoder_NullVariables() {
    String encryptedPassword = Encr.encryptPasswordIfNotUsingVariables("mypass");
    String input = "sftp://myuser:" + encryptedPassword + "@myserver/myfolder/myfile.txt";
    String result = HopVfs.hop2vfsPasswordEncoder(input, null);
    assertNotEquals(input, result);
    assertTrue(result.contains("{"));
    assertTrue(result.contains("}"));
  }

  /**
   * Test hop2vfsPasswordEncoder with Hop-encrypted password prefix. When encoder is available, the
   * URI should be modified and password wrapped in braces. When encoder is not initialized, the URI
   * should be returned unchanged (graceful degradation).
   */
  @Test
  public void testHop2VfsPasswordEncoder_EncryptedPrefixHandling() {
    // Use a pre-computed Hop-encrypted password (same one from user's example)
    // This represents an encrypted password that starts with "Encrypted " prefix
    String encryptedPassword = Encr.encryptPasswordIfNotUsingVariables("mypass");
    String input = "sftp://myuser:" + encryptedPassword + "@myserver/myfolder/myfile.txt";

    String result = HopVfs.hop2vfsPasswordEncoder(input, new Variables());

    assertNotEquals(input, result);
    assertTrue(result.contains("{"));
    assertTrue(result.contains("}"));
  }

  /** Test hop2vfsPasswordEncoder with backslash separator */
  @Test
  public void testHop2VfsPasswordEncoder_BackslashSeparator() {
    String encryptedPassword = Encr.encryptPasswordIfNotUsingVariables("mypass");
    String input = "\\\\myuser:" + encryptedPassword + "@myserver\\myfolder\\myfile.txt";

    String result = HopVfs.hop2vfsPasswordEncoder(input, new Variables());

    assertNotEquals(input, result);
    assertTrue(result.contains("{"));
    assertTrue(result.contains("}"));
  }
}
