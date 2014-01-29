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

import java.util.Iterator;

/**
 * Checked excpetion that is thrown when there are one more
 * issues with the primitive that is being compiled.
 */
public class CompilationException extends Exception implements Iterable<Message> {

    private final String detailedMessage;

    private final Iterable<Message> messages;
    /**
     * Construct default CompilationException with the messages (that
     * may typically be returned by the validation phase).
     *
     * @param validationMessages - Iterable messages.
     */
    public CompilationException(Iterable<Message> validationMessages) {
        StringBuilder buffer = new StringBuilder();
        for (Message message : validationMessages) {
            if (buffer.length() > 0) {
                buffer.append('\n');
            }
            buffer.append(message);
        }
        detailedMessage = buffer.toString();
        messages = validationMessages;
    }

    @Override
    public String getMessage() {
        return detailedMessage;
    }


    @Override
    public Iterator<Message> iterator() {
        return messages.iterator();
    }
}
