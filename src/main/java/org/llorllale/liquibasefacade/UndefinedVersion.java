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
 * Special {@link Version} that represents the state where a schema's {@link LinearProgressionFacade#isVersioned() version} cannot be determined.<br>
 * @author George Aristy
 */
public final class UndefinedVersion extends Version {
  UndefinedVersion(){
    super(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
  }

  @Override
  public String string(){
    return toString();
  }

  /**
   * Determines whether {@code other} is an instance of {@link UndefinedVersion}.
   * @param other
   * @return 
   * @throws NullPointerException if {@code other} is {@code null}.
   */
  public static boolean isUndefinedVersion(Version other){
    return UndefinedVersion.class.equals(other.getClass());
  }

  @Override
  public String toString(){
    return getClass().getSimpleName();
  }
}
