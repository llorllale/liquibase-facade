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

/**
 * Represents an arbitrary <em>state</em> for the database schema.<br><br>
 * @author George Aristy
 */
public class Version implements Comparable<Version> {
  private final int major;
  private final int minor;
  private final int release;

  /**
   * 
   * @param major major release number
   * @param minor minor release number
   * @param release bugfix release number
   */
  protected Version(int major, int minor, int release) {
    this.major = major;
    this.minor = minor;
    this.release = release;
  }

  /**
   * Factory method for {@link Version} instances.<br>
   * It is illegal to pass in values less than {@code 0} for any of the parameters. '{@code 0.0.0} is a special case and reserved for the {@link NullVersion}.
   * @param major the major release number
   * @param minor the minor release number
   * @param release the bugfix release number
   * @return a version representing the given version numbers
   */
  public static Version of(int major, int minor, int release) {
    if(major == 0 && minor == 0 && release == 0){
      throw new IllegalArgumentException(String.format("Version '0.0.0' reserved for '%s'.", new NullVersion()));
    }

    if(major < 0) 
      throw new IllegalArgumentException("Negative major version value: " + major);

    if(minor < 0) 
      throw new IllegalArgumentException("Negative minor version value: " + minor);

    if(release < 0) 
      throw new IllegalArgumentException("Negative release version value: " + release);

    return new Version(major, minor, release);
  }

  /**
   * 
   * @return the major release
   */
  public int getMajor() {
    return major;
  }

  /**
   * 
   * @return the minor release
   */
  public int getMinor() {
    return minor;
  }

  /**
   * 
   * @return the bugfix release
   */
  public int getRelease() {
    return release;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 79 * hash + this.major;
    hash = 79 * hash + this.minor;
    hash = 79 * hash + this.release;
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Version other = (Version) obj;
    if (this.major != other.major) {
      return false;
    }
    if (this.minor != other.minor) {
      return false;
    }
    if (this.release != other.release) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Version o) {
    if(o == null) throw new IllegalArgumentException("Null argument.");

    if(major > o.major){
      return 1;
    }else if (major < o.major){
      return -1;
    }

    if(minor > o.minor){
      return 1;
    }else if(minor < o.minor){
      return -1;
    }

    if(release > o.release){
      return 1;
    }else if(release < o.release){
      return -1;
    }

    return 0;
  }

  /**
   * 
   * @param other other version to compare against
   * @return whether this version is 'more recent' than the given version
   */
  public boolean isGreaterThan(Version other){
    return compareTo(other) > 0;
  }

  /**
   * 
   * @param other other version to compare against
   * @return whether this version is 'the same or more recent' than the given version
   */
  public boolean isEqualOrGreaterThan(Version other){
    return equals(other) || compareTo(other) > 0;
  }

  /**
   * 
   * @param other other version to compare against
   * @return whether this version is 'older' than the given version
   */
  public boolean isLessThan(Version other){
    return compareTo(other) < 0;
  }

  /**
   * 
   * @param other other version to compare against
   * @return whether this version is 'the same or older' than the given version
   */
  public boolean isEqualOrLessThan(Version other){
    return equals(other) || compareTo(other) < 0;
  }

  /**
   * Utility method to help determine a schema's current version.
   * @param s a string of the form X.Y.Z where each letter is an integer
   * @return a version representing the given version number
   * @throws IllegalArgumentException if {@code s} is {@code null} or if it is not of the form X.Y.Z where X, Y, and Z are each integers.
   */
  static Version valueOf(String s){
    if(s == null) throw new IllegalArgumentException("null argument.");
    if(s.isEmpty()) throw new IllegalArgumentException("Empty string.");

    String[] parts = s.split("\\.", 3);

    if(parts.length != 3){
      throw new IllegalArgumentException(String.format("Wrong format in string %s. The format is X.Y.Z", s));
    }

    try{
      final int major = Integer.valueOf(parts[0]);
      final int minor = Integer.valueOf(parts[1]);
      final int release = Integer.valueOf(parts[2]);
      return new Version(major, minor, release);
    }catch(NumberFormatException nfe){
      throw new IllegalArgumentException(String.format("Wrong format in string %s. The format is X.Y.Z where X, Y, and Z must the intergers.", s));
    }
  }

  /**
   * 
   * @return returns a string representation of the version numbers
   * @see #valueOf(java.lang.String) 
   * @see #of(int, int, int) 
   */
  public String string(){
    return major + "." + minor + "." + release;
  }

  @Override
  public String toString() {
    return "Version{" + "major=" + major + ", minor=" + minor + ", release=" + release + '}';
  }
}