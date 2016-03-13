/* * Copyright 2015 George Aristy.
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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
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
public class LinearProgressionFacadeTest {
  //specify shutdown=true so that structure is not persisted between tests
  private static final String DB_URL = "jdbc:hsqldb:mem:test;shutdown=true";

  private static Connection connection;

  private static List<Version> versions;

  private static Function<Version, String> changesetFileLocator = v -> String.format("test/Version-%d.%d.%d.xml", v.getMajor(), v.getMinor(), v.getRelease());

  private static Function<Version, ResourceAccessor> resourceAccessorGenerator = v -> new ClassLoaderResourceAccessor();
  
  public LinearProgressionFacadeTest() {
  }
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    versions = Arrays.asList(new Version(1,0,0), new Version(1,1,0), new Version(2,0,0));
  }
  
  @AfterClass
  public static void tearDownClass() throws Exception {
  }
  
  @Before
  public void setUp() throws Exception {
    connection = DriverManager.getConnection(DB_URL);
  }
  
  @After
  public void tearDown() throws Exception {
    connection.close();
  }

  @Test
  public void testGetCurrentVersion() throws Exception {
    LinearProgressionFacade facade = new LinearProgressionFacade(connection, 
            versions, 
            changesetFileLocator,
            resourceAccessorGenerator
    );
    Version expResult = new UndefinedVersion();
    Version result = facade.getCurrentVersion();
    assertEquals(expResult, result);

    facade.apply(1,0,0);
    expResult = new Version(1,0,0);
    assertEquals(expResult, facade.getCurrentVersion());

    facade.apply(2,0,0);
    expResult = new Version(2,0,0);
    assertEquals(expResult, facade.getCurrentVersion());
  }

  @Test
  public void testApply() throws Exception {
    LinearProgressionFacade facade = new LinearProgressionFacade(connection, 
            versions, 
            changesetFileLocator,
            resourceAccessorGenerator
    );
    facade.apply(1,0,0);
    assertTrue(tableExists("Customer", connection));
    assertTrue(columnExists("Customer", "id", connection));
    assertTrue(columnExists("Customer", "first_name", connection));
    assertTrue(columnExists("Customer", "last_name", connection));
    assertTrue(columnExists("Customer", "age", connection));
    assertTrue(columnExists("Customer", "email", connection));
    assertFalse(tableExists("Address", connection));
    assertFalse(tableExists("Product", connection));

    facade.apply(1,1,0);
    assertTrue(tableExists("Customer", connection));
    assertTrue(tableExists("Address", connection));
    assertFalse(tableExists("Product", connection));

    facade.apply(2,0,0);
    assertTrue(tableExists("Customer", connection));
    assertTrue(tableExists("Address", connection));
    assertTrue(tableExists("Product", connection));

    facade.apply(1,0,0);
    assertTrue(tableExists("Customer", connection));
    assertFalse(tableExists("Address", connection));
    assertFalse(tableExists("Product", connection));
  }

  /**
   * Issue #2: Constructor of LinearProgressionFacade should check parameters for null
   * An instance of LinearProgressionFacade should be always be assumed to be properly constructed. 
   * None of the constructor's parameters are intended to be optional. Therefore, null checks 
   * should be performed and the constructor should fail with an NPE if any of them are missing. 
   */
  @Test(expected = NullPointerException.class)
  public void constructorMustFailIfConnectionIsNull() throws Exception {
    new LinearProgressionFacade(
            null, 
            versions, 
            changesetFileLocator, 
            resourceAccessorGenerator
    );
  }

  /**
   * Issue #2: Constructor of LinearProgressionFacade should check parameters for null
   * An instance of LinearProgressionFacade should be always be assumed to be properly constructed. 
   * None of the constructor's parameters are intended to be optional. Therefore, null checks 
   * should be performed and the constructor should fail with an NPE if any of them are missing. 
   */
  @Test(expected = NullPointerException.class)
  public void constructorMustFailIfVersionsIsNull() throws Exception {
    new LinearProgressionFacade(
            connection, 
            null,
            changesetFileLocator, 
            resourceAccessorGenerator
    );
  }

  /**
   * Issue #2: Constructor of LinearProgressionFacade should check parameters for null
   * An instance of LinearProgressionFacade should be always be assumed to be properly constructed. 
   * None of the constructor's parameters are intended to be optional. Therefore, null checks 
   * should be performed and the constructor should fail with an NPE if any of them are missing. 
   */
  @Test(expected = NullPointerException.class)
  public void constructorMustFailIfChangesetFileLocatorIsNull() throws Exception {
    new LinearProgressionFacade(
            connection, 
            versions,
            null,
            resourceAccessorGenerator
    );
  }

  /**
   * Issue #2: Constructor of LinearProgressionFacade should check parameters for null
   * An instance of LinearProgressionFacade should be always be assumed to be properly constructed. 
   * None of the constructor's parameters are intended to be optional. Therefore, null checks 
   * should be performed and the constructor should fail with an NPE if any of them are missing. 
   */
  @Test(expected = NullPointerException.class)
  public void constructorMustFailIfResourceAccessorGeneratorIsNull() throws Exception {
    new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            null
    );
  }

  /**
   * Issue #3: LinearProgressionFacade should check for empty lists in constructor
   * There is no conceivable use case where the LinearProgressionFacade would be 
   * used with an empty list. Therefore, the constructor should fail if an empty 
   * list is supplied.
   */
  @Test(expected = IllegalArgumentException.class)
  public void constructorMustFailIfVersionsIsEmpty() throws Exception {
    new LinearProgressionFacade(
            connection, 
            new ArrayList<>(),
            changesetFileLocator,
            resourceAccessorGenerator
    );
  }

  /**
   * Issue #7: LinearProgressionFacade: no validation on input for isUpgrade(Version)and isDowngrade(Version)
   * The implementation should validate whether the input Version is defined in the list of user-supplied versions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void applyVersionMustFailIfVersionNotFoundInListOfVersions() throws Exception {
    new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            resourceAccessorGenerator
    ).apply(Version.of(5, 1, 0));
  }

  /**
   * Issue #7: LinearProgressionFacade: no validation on input for isUpgrade(Version)and isDowngrade(Version)
   * The implementation should validate whether the input Version is defined in the list of user-supplied versions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void isUpgradeMustFailIfVersionNotFoundInListOfVersions() throws Exception {
    new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            resourceAccessorGenerator
    ).isUpgrade(Version.of(5, 1, 0));
  }

  /**
   * Issue #7: LinearProgressionFacade: no validation on input for isUpgrade(Version)and isDowngrade(Version)
   * The implementation should validate whether the input Version is defined in the list of user-supplied versions.
   */
  @Test(expected = IllegalArgumentException.class)
  public void isDowngradeMustFailIfVersionNotFoundInListOfVersions() throws Exception {
    new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            resourceAccessorGenerator
    ).isDowngrade(Version.of(5, 1, 0));
  }

  /**
   * Issue #4: LinearProgressionFacade - no validation whether the schema's applied versions are included in the user-supplied list of versions
   * 
   * It is possible for the current implementation of getCurrentVersion() to return a Version value that is not present in the list of valid 
   * versions that the user supplied. This can lead to obvious inconsistencies. 
   * 
   * Furthermore, the LinearProgressionFacade is intended to act in a "continuous" mode, that is, not skipping over any versions as it is upgrading
   * or rolling back the schema. It is definitely within the user's expectations that if eg. a schema is supposed to be rolled back from 
   * version 5 to 1 then no artifacts should be left behind belonging to any version higher than 1.
   * 
   * Consider validating whether there are any inconsistencies between a schema's applied versions and the list of versions supplied by the user 
   * and fail with an appropriate error.
   */
  @Test(expected = IllegalStateException.class)
  public void constructorMustFailIfCurrentDBVersionIsNotInList() throws Exception {
    LinearProgressionFacade f = new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            resourceAccessorGenerator
    );

    f.applyAll();
    insertVersion(Version.of(10, 1, 3));

    //this should fail because 'versions' does not have the '10.1.3' inserted above
    new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            resourceAccessorGenerator
    );
  }

  /**
   * Issue #4: LinearProgressionFacade - no validation whether the schema's applied versions are included in the user-supplied list of versions
   * 
   * It is possible for the current implementation of getCurrentVersion() to return a Version value that is not present in the list of valid 
   * versions that the user supplied. This can lead to obvious inconsistencies. 
   * 
   * Furthermore, the LinearProgressionFacade is intended to act in a "continuous" mode, that is, not skipping over any versions as it is upgrading
   * or rolling back the schema. It is definitely within the user's expectations that if eg. a schema is supposed to be rolled back from 
   * version 5 to 1 then no artifacts should be left behind belonging to any version higher than 1.
   * 
   * Consider validating whether there are any inconsistencies between a schema's applied versions and the list of versions supplied by the user 
   * and fail with an appropriate error.
   */
  @Test(expected = IllegalStateException.class)
  public void getCurrentVersionMustFailIfCurrentDBVersionIsNotInList() throws Exception {
    LinearProgressionFacade f = new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            resourceAccessorGenerator
    );

    f.applyAll();
    insertVersion(Version.of(10, 1, 3));

    //this should fail because 'versions' does not have the '10.1.3' inserted above
    f.getCurrentVersion();
  }

  /**
   * Issue #4: LinearProgressionFacade - no validation whether the schema's applied versions are included in the user-supplied list of versions
   * 
   * It is possible for the current implementation of getCurrentVersion() to return a Version value that is not present in the list of valid 
   * versions that the user supplied. This can lead to obvious inconsistencies. 
   * 
   * Furthermore, the LinearProgressionFacade is intended to act in a "continuous" mode, that is, not skipping over any versions as it is upgrading
   * or rolling back the schema. It is definitely within the user's expectations that if eg. a schema is supposed to be rolled back from 
   * version 5 to 1 then no artifacts should be left behind belonging to any version higher than 1.
   * 
   * Consider validating whether there are any inconsistencies between a schema's applied versions and the list of versions supplied by the user 
   * and fail with an appropriate error.
   */
  @Test(expected = IllegalStateException.class)
  public void applyMustFailIfDatabaseIsInconsistent() throws Exception {
    LinearProgressionFacade f = new LinearProgressionFacade(
            connection, 
            versions,
            changesetFileLocator,
            resourceAccessorGenerator
    );

    f.applyAll();
    insertVersion(Version.of(14, 12, 3));

    //this should fail because 'versions' does not have the version inserted above
    f.apply(versions.get(1));
  }

  private boolean tableExists(String table, Connection conn) throws SQLException {
    DatabaseMetaData meta = conn.getMetaData();

    try(ResultSet r = meta.getTables(null, null, table.toUpperCase(), new String[]{"TABLE"})){
      return r.next();
    }
  }

  private boolean columnExists(String table, String column, Connection conn) throws SQLException {
    DatabaseMetaData meta = conn.getMetaData();

    try(ResultSet r = meta.getColumns(null, null, table.toUpperCase(), column.toUpperCase())){
      return r.next();
    }
  }

  private void insertVersion(Version version) throws SQLException {
    try(PreparedStatement stmt = connection.prepareStatement("insert into DATABASECHANGELOG values (?,?,?,?,?,?,?,?,?,?,?)")){
      stmt.setString(1, "");  //ID
      stmt.setString(2, "test");  //author
      stmt.setString(3, "filename");  //filename
      stmt.setTimestamp(4, Timestamp.from(Instant.now()));  //dateexecuted
      stmt.setInt(5, 5000); //order executed
      stmt.setString(6, "c"); //exec type
      stmt.setString(7, "q345lkjl");  //md5 hash
      stmt.setString(8, "test");  //description
      stmt.setString(9, "comment"); //comments
      stmt.setString(10, version.string()); //tag <-- this is the offending version
      stmt.setString(11, "3.2.0"); //liquibase
      stmt.execute();
    }
  }
}