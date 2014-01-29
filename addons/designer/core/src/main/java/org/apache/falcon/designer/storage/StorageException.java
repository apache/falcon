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

package org.apache.falcon.designer.storage;

/**
 * Checked Exception that the {@link org.apache.falcon.designer.storage.Storeable}
 * throws when there is an issue with either storing or restoring contents
 * for an object from persistent storage.
 */
public class StorageException extends Exception {

    /**
     * Constructs a default exception with no cause or message.
     */
    public StorageException() {
        super();
    }

    /**
     * Constructs an exception with a specific message.
     *
     * @param message - Message on the exception
     */
    public StorageException(String message) {
        super(message);
    }

    /**
     * Constructs an exception with a specific message and cause.
     *
     * @param message - Message on the exception
     * @param cause - Underlying exception that resulted in this being thrown
     */
    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an exception with a cause and message is initialized
     * to be same as that of the cause.
     *
     * @param cause - Underlying exception that resulted in this being thrown
     */
    public StorageException(Throwable cause) {
        super(cause);
    }
}
