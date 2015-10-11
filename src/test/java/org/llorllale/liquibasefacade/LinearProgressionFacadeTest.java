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

import org.llorllale.liquibasefacade.Version;
import org.llorllale.liquibasefacade.LinearProgressionFacade;
import org.llorllale.liquibasefacade.UndefinedVersion;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.commons.dbutils.DbUtils;
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
            v -> String.format("test/Version-%d.%d.%d.xml", v.getMajor(), v.getMinor(), v.getRelease()), 
            v -> new ClassLoaderResourceAccessor());
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
            v -> String.format("test/Version-%d.%d.%d.xml", v.getMajor(), v.getMinor(), v.getRelease()), 
            v -> new ClassLoaderResourceAccessor());
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

  private boolean tableExists(String table, Connection conn) throws SQLException {
    ResultSet r = null;

    try{
      DatabaseMetaData meta = conn.getMetaData();
      r = meta.getTables(null, null, table.toUpperCase(), new String[]{"TABLE"});
      return r.next();
    }finally{
      DbUtils.closeQuietly(r);
    }
  }

  private boolean columnExists(String table, String column, Connection conn) throws SQLException {
    ResultSet r = null;

    try{
      DatabaseMetaData meta = conn.getMetaData();
      r = meta.getColumns(null, null, table.toUpperCase(), column.toUpperCase());
      return r.next();
    }finally{
      DbUtils.closeQuietly(r);
    }
  }
}