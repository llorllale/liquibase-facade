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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ResourceAccessor;

/**
 *
 * @author George Aristy
 * @since 1.0.0
 */
public class LinearProgressionFacade {
  private final Connection connection;
  private final List<Version> versions;
  private final Function<Version, String> changesetFileLocator;
  private final Function<Version, ResourceAccessor> resourceAccessorGenerator;

  private static final String CHANGELOG_TABLE = "databasechangelog".toUpperCase();

  private static final String SQL_GET_CURRENT_VERSION = String.format("select tag from %s order by orderexecuted desc", CHANGELOG_TABLE);

  /**
   * 
   * @param connection The JDBC connection to work on.
   * @param versions The versions used as reference.
   * @param changesetFileLocator A function that returns the path to where the liquibase changeset file is located for a given version.
   * @param resourceAccessorGenerator A function that returns the {@code ResourceAccessor} used to fetch a given version's changeset file.
   * @throws NullPointerException if any of the parameters are {@code null}
   * @throws IllegalArgumentException if {@code versions} is empty
   * @since 1.0.0
   */
  public LinearProgressionFacade(
          Connection connection, 
          List<Version> versions, 
          Function<Version, String> changesetFileLocator, 
          Function<Version, ResourceAccessor> resourceAccessorGenerator
  ){
    this.connection = Objects.requireNonNull(connection, "null connection.");
    this.versions = Optional.of(new ArrayList<>(Objects.requireNonNull(versions, "null version list.")))
            .filter(v -> v.size() > 0)
            .orElseThrow(() -> new IllegalArgumentException("empty version list."));
    this.changesetFileLocator = Objects.requireNonNull(changesetFileLocator, "null changesetFileLocator function.");
    this.resourceAccessorGenerator = Objects.requireNonNull(resourceAccessorGenerator, "null resourceAccessorGenerator function.");
  }

  /**
   * Returns the underlying {@link Connection connection}.
   * @return the underlying {@link Connection connection} being used.
   * @since 1.0.0
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * Returns the reference list of versions used.
   * @return the reference list of versions used
   * @see #LinearProgressionFacade(java.sql.Connection, java.util.List, java.util.function.Function, java.util.function.Function) 
   * @since 1.0.0
   */
  public List<Version> getVersions() {
    return Collections.unmodifiableList(versions);
  }

  /**
   * Returns whether the input {@link Version version} is an 'upgrade' over the database schema's current version.
   * @param version a version that must be included in the list of reference versions.
   * @return whether the input {@link Version version} is an 'upgrade' over the database schema's current version.
   * @throws LiquibaseException wrapping any underlying SQLException
   * @throws IllegalArgumentException if {@code version} is not included in list of {@code versions}.
   * @see #getCurrentVersion() 
   * @since 1.0.0
   */
  public boolean isUpgrade(Version version) throws LiquibaseException {
    errorIfInvalidInput(version);
    return getCurrentVersion().isLessThan(version);
  }

  /**
   * Returns whether the input {@link Version version} is a 'downgrade' under the database schema's current version.
   * @param version a version that must be part of the list of reference versions.
   * @return whether the input {@link Version version} is a 'downgrade' under the database schema's current version.
   * @throws LiquibaseException wrapping any underlying SQLException
   * @throws IllegalArgumentException if {@code version} is not included in list of {@code versions}.
   * @see #getCurrentVersion() 
   * @since 1.0.0
   */
  public boolean isDowngrade(Version version) throws LiquibaseException {
    errorIfInvalidInput(version);
    return getCurrentVersion().isGreaterThan(version);
  }

  /**
   * Shorthand for {@link #apply(org.llorllale.liquibasefacade.Version)}.<br>
   * @param major the major version number
   * @param minor the minor version number
   * @param release the point/bugfix release version number
   * @return the number of changes applied
   * @throws LiquibaseException thrown by liquibase, or wrapping any underlying SQLException
   * @throws IllegalArgumentException as per the rules in {@link Version#of(int, int, int)} or if 
   * the values define a {@link Version} that is not included in list of {@code versions}.
   * @see #apply(org.llorllale.liquibasefacade.Version) 
   * @since 1.0.0
   */
  public int apply(int major, int minor, int release) throws LiquibaseException {
    return apply(Version.of(major, minor, release));
  }

  /**
   * Applies all changes defined in the reference list of versions.
   * @return the number of changes applied.
   * @throws LiquibaseException thrown by liquibase, or wrapping any underlying SQLException
   * @see #LinearProgressionFacade(java.sql.Connection, java.util.List, java.util.function.Function, java.util.function.Function) 
   * @since 1.0.0
   */
  public int applyAll() throws LiquibaseException {
    return apply(versions.stream().max((v1, v2) -> v1.compareTo(v2)).get());
  }

  /**
   * Rolls back all changes defined in the reference list of versions.
   * @return the number of changes applied.
   * @throws LiquibaseException thrown by liquibase, or wrapping any underlying SQLException
   * @since 1.0.0
   */
  public int rollbackAll() throws LiquibaseException {
    return downgradeDatabase(new NullVersion());
  }

  /**
   * Applies the changes necessary to take the database schema from its {@link #getCurrentVersion() current state} to the state
   * defined by {@code targetVersion}.<br>
   * Performing these changes may either:<br>
   * <ul>
   * <li>Upgrade the schema if {@code targetVersion} is {@link Version#isGreaterThan(org.llorllale.liquibasefacade.Version) greater than} the current version.</li>
   * <li>Downgrade the schema if {@code targetVersion} is {@link Version#isLessThan(org.llorllale.liquibasefacade.Version) greater than} the current version.</li>
   * <li>Do nothing if the current version equals the {@code targetVersion}.</li>
   * </ul>
   * @param targetVersion applies the changes required to bring the schema's version to the given {@code targetVersion}.
   * @return the number of changes (changeSets) applied
   * @throws LiquibaseException thrown by liquibase, or wrapping any underlying SQLException
   * @throws IllegalArgumentException if {@code targetVersion} is not included in list of {@code versions}.
   * @throws NullPointerException if {@code targetVersion} is {@code null}.
   * @see #LinearProgressionFacade(java.sql.Connection, java.util.List, java.util.function.Function, java.util.function.Function) 
   * @since 1.0.0
   */
  public int apply(Version targetVersion) throws LiquibaseException {
    errorIfInvalidInput(targetVersion);
    int changes = 0;

    if(isDowngrade(targetVersion)){
      changes = downgradeDatabase(targetVersion);
    }else if(isUpgrade(targetVersion)){
      changes = upgradeDatabase(targetVersion);
    }

    return changes;
  }

  /**
   * Returns the database schema's current version.<br>
   * If the schema {@link #isVersioned() is not versioned} then its state is considered as <em>undefined</em>, resulting in {@link UndefinedVersion} being returned.<br>
   * If the schema is versioned but no versions have been applied, the result will be {@link NullVersion}.<br>
   * Otherwise, an appropriate instance of {@link Version} is returned.
   * @return the schema's current version
   * @throws LiquibaseException wrapping any {@link java.sql.SQLException}
   * @since 1.0.0
   */
  public Version getCurrentVersion() throws LiquibaseException {
    Version version = null;

    if(isVersioned()){
      try(PreparedStatement stmt = connection.prepareStatement(SQL_GET_CURRENT_VERSION); 
               ResultSet result = stmt.executeQuery()){
        if(result.next()){
          version = Version.valueOf(result.getString(1));
        }else{
          version = new NullVersion();
        }
      }catch(Exception e){
        throw new LiquibaseException("Unable to read the current version from the database.", e);
      }
    }else{
      version = new UndefinedVersion();
    }

    return version;
  }

  /**
   * Determines whether the database schema is versioned: {@link UndefinedVersion} if no version metadata can be gathered; a {@link Version} if a version has been applied, or {@link NullVersion} if there exists version metadata indicating that no version has been applied.
   * @return whether the database schema is versioned.
   * @throws LiquibaseException wrapping any underlying SQLException
   * @since 1.0.0
   */
  public boolean isVersioned() throws LiquibaseException {
    ResultSet r = null;

    try{
      DatabaseMetaData md = connection.getMetaData();
      r = md.getTables(null, null, CHANGELOG_TABLE, new String[]{"TABLE"});
      return r.next();
    }catch(SQLException e){
      throw new LiquibaseException("Unable to determine if database is versioned.", e);
    }finally{
      try{
        r.close();
      }catch(Exception e){
        //ignore
      }
    }
  }

  /**
   * Intended for use in validating user input
   * @param version 
   * @throws IllegalArgumentException 
   */
  private void errorIfInvalidInput(Version version){
    if(UndefinedVersion.isUndefinedVersion(version)){
      throw new IllegalArgumentException("Illegal argument for 'version' - version is 'UndefinedVersion'.");
    }

    if(NullVersion.isNullVersion(version)){
      throw new IllegalArgumentException("Illegal argument for 'version' - version is 'NullVersion'.");
    }

    if(!versions.contains(version)){
      throw new IllegalArgumentException(
              String.format(
                      "Illegal argument for 'version' - version not found in list of versions. Version: %s List of versions: %s", 
                      version, 
                      versions
              )
      );
    }
  }

  private int upgradeDatabase(Version targetVersion) throws LiquibaseException {
    Version currentVersion = getCurrentVersion();
    int changesApplied = 0; //return variable

    if(targetVersion.isEqualOrLessThan(currentVersion)){
      throw new LiquibaseException(String.format("Target version is equal or less than the current version. Target: %s Current: %s", targetVersion, currentVersion));
    }

    List<Version> forwardList = new ArrayList<>(versions);
    Collections.sort(forwardList);

    try{
      for(Version version : versions){
        if(version.isGreaterThan(currentVersion) && version.isEqualOrLessThan(targetVersion)){
          Liquibase liquibase = getLiquibaseInstance(version, changesetFileLocator, resourceAccessorGenerator, connection);
          int changeSetCount = getChangesetCount(liquibase);

          for(int i = 0; i < changeSetCount; i++){
            liquibase.update(1, null);
            changesApplied++;
          }

          liquibase.tag(version.string());
        }
      }
    }catch(Exception e){
      throw new LiquibaseException("Error while attempting to upgrade the schema to version " + targetVersion, e);
    }

    return changesApplied;
  }

  private int downgradeDatabase(Version targetVersion) throws LiquibaseException {
    Version currentVersion = getCurrentVersion();
    int changesApplied = 0; //return variable

    if(targetVersion.isEqualOrGreaterThan(currentVersion)){
      throw new LiquibaseException(String.format("Target version is equal or greater than the current version. Target: %s Current: %s", targetVersion, currentVersion));
    }

    List<Version> reversedList = new ArrayList<>(versions);
    Collections.sort(reversedList);
    Collections.reverse(reversedList);

    try{
      for(Version version : reversedList){
        if(version.isEqualOrLessThan(currentVersion) && version.isGreaterThan(targetVersion)){
          Liquibase liquibase = getLiquibaseInstance(version, changesetFileLocator, resourceAccessorGenerator, connection);
          int changeSetCount = getChangesetCount(liquibase);

          for(int i = 0; i < changeSetCount; i++){
            liquibase.rollback(1, null);
            changesApplied++;
          }
        }
      }
    }catch(Exception e){
      throw new LiquibaseException("Error while attempting to downgrade the schema to version " + targetVersion, e);
    }

    return changesApplied;
  }

  private Database getLiquibaseDatabase(Connection connection) throws LiquibaseException {
    Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));

    if(database == null){
      throw new LiquibaseException("Null liquibase database.");
    }

    database.setAutoCommit(true);
    return database;
  }

  private int getChangesetCount(Liquibase liquibase) throws LiquibaseException {
    return liquibase.getDatabaseChangeLog().getChangeSets().size();
  }

  private Liquibase getLiquibaseInstance(Version version, Function<Version, String> locator, Function<Version, ResourceAccessor> generator, Connection connection) throws LiquibaseException{
    return new Liquibase(locator.apply(version), generator.apply(version), getLiquibaseDatabase(connection));
  }
}