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

package org.apache.falcon.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Command parser for CLI.
 */
public class TestCLIParser {

    @Test
    public void testEmptyParser() throws Exception {
        try {
            CLIParser parser = new CLIParser("falcon", new String[]{});
            CLIParser.Command c = parser.parse(new String[]{"a"});
            Assert.fail();
        } catch (ParseException ex) {
            // nop
        }
    }

    @Test
    public void testCommandParser() throws Exception {
        try {
            CLIParser parser = new CLIParser("oozie", new String[]{});
            parser.addCommand("a", "<A>", "AAAAA", new Options(), false);
            CLIParser.Command c = parser.parse(new String[]{"a", "b"});
            Assert.assertEquals("a", c.getName());
            Assert.assertEquals("b", c.getCommandLine().getArgs()[0]);
        } catch (ParseException ex) {
            Assert.fail();
        }
    }

    @Test
    public void testCommandParserX() throws Exception {
        Option opt = new Option("o", false, "O");
        Options opts = new Options();
        opts.addOption(opt);
        CLIParser parser = new CLIParser("test", new String[]{});
        parser.addCommand("c", "-X ",
                "(everything after '-X' are pass-through parameters)", opts,
                true);
        CLIParser.Command c = parser.parse("c -o -X -o c".split(" "));
        Assert.assertEquals("-X", c.getCommandLine().getArgList().get(0));
        Assert.assertEquals(3, c.getCommandLine().getArgList().size());
    }
}
