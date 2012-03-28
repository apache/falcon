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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;

public abstract class Action {
    private static final String SEPARATOR = "&";
    private static final String PAYLOAD_SEPARATOR = "#";
    
    private String name;
    private String category;
    private Payload payload = new Payload();

    protected static class Payload {
        private Map<String, String> params = new HashMap<String, String>();
        
        public Payload(String... typeValues) {
            for(int index = 0 ; index < typeValues.length ; index += 2) {
                params.put(typeValues[index], typeValues[index + 1]);
            }
        }
        
        public void add(String... typeValues) {
            for(int index = 0 ; index < typeValues.length ; index += 2) {
                params.put(typeValues[index], typeValues[index + 1]);
            }            
        }
        
        private Payload() { }
        
        public String get(String key) {
            return params.get(key);
        }
        
        public static Payload fromString(String str) {
            if(StringUtils.isNotEmpty(str)) {
                String[] parts = str.substring(1, str.length()-1).split(PAYLOAD_SEPARATOR);
                Payload payload = new Payload();
                for(String part:parts) {
                    if(StringUtils.isEmpty(part))
                        continue;
                    String key = part.substring(0, part.indexOf('='));
                    String value = part.substring(part.indexOf('=') + 1, part.length());
                    payload.params.put(key, value);
                }
                return payload;
            }
            return null;
        }
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("{");
            for(Entry<String, String> entry: params.entrySet()) {
                if(entry.getValue() != null) {
                    builder.append(entry.getKey()).append('=').append(entry.getValue()).append(PAYLOAD_SEPARATOR);
                }
            }
            builder.append('}');
            return builder.toString();
        }
        
    }
    
    protected Action() { }

    public Action(String category) {
        this.name = getClass().getName();
        this.category = category;
    }

    private void setCategory(String category) {
        this.category = category;
    }

    protected void setPayload(Payload payload) {
        this.payload = payload;
    }

    public String getName() {
        return name;
    }

    public String getCategory() {
        return category;
    }

    public Payload getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Action{" +
                "name=" + name + SEPARATOR +
                "category=" + category + SEPARATOR +
                "payload=" + payload.toString() + '}';
    }

    public static Action fromLine(String line) {
        try {
            String[] data = (line.substring(line.indexOf("{") + 1,
                    line.length() - 1)).split(SEPARATOR);
            
            String name = data[0].substring(data[0].indexOf('=') + 1);
            String category = data[1].substring(data[1].indexOf('=') + 1);
            String payloadStr = data[2].substring(data[2].indexOf('=') + 1);
            
            Action action = (Action) Class.forName(name).newInstance();
            action.setCategory(category);
            action.setPayload(Payload.fromString(payloadStr));
            return action;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid input line " + line, e);
        }
    }

    public abstract void rollback() throws IvoryException;
    public abstract void commit() throws IvoryException;
}
