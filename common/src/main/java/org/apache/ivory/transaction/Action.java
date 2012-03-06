/*
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
 */

package org.apache.ivory.transaction;

public class Action {

    private String name;
    private String category;
    private String label;

    private Action() { }

    public Action(String name, String category, String label) {
        this.name = name;
        this.category = category;
        this.label = label;
    }

    private void setName(String name) {
        this.name = name;
    }

    private void setCategory(String category) {
        this.category = category;
    }

    private void setLabel(String label) {
        this.label = label;
    }

    public String getName() {
        return name;
    }

    public String getCategory() {
        return category;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return "Action{" +
                "name=" + name +
                ", category=" + category +
                ", label=" + label + '}';
    }

    public static Action fromLine(String line) {
        try {
            String[] data = (line.substring(line.indexOf("{") + 1,
                    line.length() - 1)).split(",");
            Action action = new Action();
            action.setName(data[0].substring(data[0].indexOf('=') + 1));
            action.setCategory(data[1].substring(data[1].indexOf('=') + 1));
            action.setLabel(data[2].substring(data[2].indexOf('=') + 1));
            return action;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid input line " + line);
        }
    }
}
