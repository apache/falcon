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
package org.apache.falcon.unit.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 * Java Example for file copy.
 */
public final class JavaExample {

    private JavaExample() {}

    public static void main(String[] args) throws IOException {
        System.out.println("Java Main Example");

        if (args.length != 2) {
            throw new IllegalArgumentException("No of arguments should be two");
        }
        String inputPath = args[0];
        String outPath = args[1];
        FileSystem fs = FileSystem.get(new Configuration());
        fs.mkdirs(new Path(outPath));
        OutputStream out = fs.create(new Path(outPath + "/" + "part"));
        FileStatus[] files = fs.listStatus(new Path(inputPath));
        if (files != null) {
            for (FileStatus file : files) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.startsWith("#")) {
                        out.write(line.getBytes());
                        out.write("\n".getBytes());
                        System.out.println(line);
                    }
                }
                reader.close();
            }
        }
        out.close();
    }
}
