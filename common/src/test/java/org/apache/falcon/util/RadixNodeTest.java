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

package org.apache.falcon.util;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Tests for Radix Node.
 */
public class RadixNodeTest {
    private RadixNode<String> rootNode =  new RadixNode<String>();
    private RadixNode<String> normalNode = new RadixNode<String>();

    @BeforeMethod
    public void setUp(){
        rootNode.setKey("");
        rootNode.setValues(new HashSet<String>(Arrays.asList("root")));

        normalNode.setKey("/data/cas/");
        normalNode.setValues(new HashSet<String>(Arrays.asList("CAS Project")));

    }


    @Test
    public void testMatchingWithRoot(){
        String inputKey = "/data/cas/";
        Assert.assertEquals(rootNode.getMatchLength(inputKey), 0);
    }

    @Test
    public void testEmptyMatchingWithRoot(){
        String inputKey = "";
        Assert.assertEquals(rootNode.getMatchLength(inputKey), 0);
    }

    @Test
    public void testNullMatchingWithRoot(){
        Assert.assertEquals(rootNode.getMatchLength(null), 0);
    }

    @Test
    public void testDistinctStringMatching(){
        String inputKey = "data/cas";
        Assert.assertEquals(normalNode.getMatchLength(inputKey), 0);
    }

    @Test
    public void testSameStringMatching(){
        String inputKey = "/data/cas";
        Assert.assertEquals(normalNode.getMatchLength(inputKey), 9);
    }

    @Test
    public void testNullStringMatching(){
        Assert.assertEquals(normalNode.getMatchLength(null), 0);
    }


    @Test
    public void testAddingDuplicateValues() {
        rootNode.addValue("root");
        Assert.assertEquals(rootNode.getValues().size(), 1);
    }

    @Test
    public void testAddMultipleValues() {
        normalNode.addValue("data");
        Assert.assertTrue(normalNode.containsValue("data"));
        Assert.assertTrue(normalNode.containsValue("CAS Project"));
    }

    @Test
    public void testMatchInput() {
        RadixNode<String> node = new RadixNode<String>();

        FalconRadixUtils.INodeAlgorithm matcher = new FalconRadixUtils.FeedRegexAlgorithm();
        node.setKey("/data/cas/projects/${YEAR}/${MONTH}/${DAY}");
        Assert.assertTrue(node.matches("/data/cas/projects/2014/09/09", matcher));
        Assert.assertFalse(node.matches("/data/cas/projects/20140909", matcher));
        Assert.assertFalse(node.matches("/data/2014/projects/2014/09/09", matcher));
        Assert.assertFalse(node.matches("/data/2014/projects/2014/09/", matcher));
        Assert.assertFalse(node.matches("/data/cas/projects/2014/09/09trail", matcher));
        Assert.assertFalse(node.matches("/data/cas/projects/2014/09/09/", matcher));
        Assert.assertFalse(node.matches("/data/cas/projects/2014/09/", matcher));
    }

}
