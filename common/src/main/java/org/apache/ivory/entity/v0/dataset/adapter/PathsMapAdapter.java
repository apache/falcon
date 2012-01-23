/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.ivory.entity.v0.dataset.adapter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.apache.ivory.entity.v0.dataset.Path;
import org.apache.ivory.entity.v0.dataset.Paths;

public class PathsMapAdapter extends XmlAdapter<Paths, Map<String, Path>> {

  @Override
  public Paths marshal(Map<String, Path> v) throws Exception {
    if (v == null) return null;
    Paths paths = new Paths();
    List<Path> pathsList = paths.getPath();
    for (Path path : v.values()) {
      pathsList.add(path);
    }
    return paths;
  }

  @Override
  public Map<String, Path> unmarshal(Paths paths) throws Exception {
    if (paths == null) return null;
    Map<String, Path> map = new HashMap<String, Path>();
    for (Path path : paths.getPath()) {
      map.put(path.getType(), path);
    }
    return map;
  }

}
