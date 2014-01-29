/**
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

package org.apache.falcon.designer.primitive;

/**
 * Messages are pieces of information that may be surfaced to the caller
 * by any of the operation on the primitives. For ex. Compilation on the
 * transformation or flow may return some messages, some of which may be
 * serious errors or warnings.
 */
public class Message {

    /**
     * Message type that each message is associated with.
     */
    public enum Type {ERROR, WARNING, INFORMATION}

    private final Type type;
    private final String message;
    private Object context;

    public Message(Type messageType, String messageText) {
        this.type = messageType;
        this.message = messageText;
    }

    public Type getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }

    public Object getContext() {
        return context;
    }

    public void setContext(Object context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return "Message{"
                + "type=" + type
                + ", message='" + message + '\''
                + ", context=" + context
                + '}';
    }
}
