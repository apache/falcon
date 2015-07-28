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

import org.apache.falcon.entity.store.FeedPathStore;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.resource.FeedLookupResult;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;

/**
 * Tests for Radix Tree.
 */
public class RadixTreeTest {

    private RadixTree<String> tree;
    private FalconRadixUtils.INodeAlgorithm regexAlgorithm = new FalconRadixUtils.FeedRegexAlgorithm();

    @BeforeMethod
    public void setUp() {
        tree = new RadixTree<String>();
        tree.insert("key1", "value1");
        tree.insert("key2", "value2");
        tree.insert("random", "random");
    }

    @AfterMethod
    public void reset() {
        tree = null;
    }

    @Test
    public void testInsertAtRootTest()  {
        FeedPathStore<String> tree2 = new RadixTree<String>();
        tree2.insert("/data/cas/projects/dwh/", "dwh");
        Assert.assertEquals(tree2.find("/data/cas/projects/dwh/").size(), 1);
        Assert.assertTrue(tree2.find("/data/cas/projects/dwh/").contains("dwh"));

    }


    @Test
    public void testDuplicateKeyInsert() {
        tree.insert("duplicatekey", "value1");
        tree.insert("duplicatekey", "value2");
        Assert.assertEquals(tree.find("duplicatekey").size(), 2);
        Assert.assertTrue(tree.find("duplicatekey").contains("value1"));
        Assert.assertTrue(tree.find("duplicatekey").contains("value2"));
    }

    @Test
    public void testGetNextCandidate() {
        tree.insert("/projects/userplatform/${YEAR}-${MONTH}-${DAY}", "feed1");
        tree.insert("/projects/userplatform/another", "feed2");
        Collection<String> result = tree.find("/projects/userplatform/another");
        Assert.assertTrue(result.contains("feed2"));

        result = tree.find("/projects/userplatform/2014-07-07", regexAlgorithm);
        Assert.assertTrue(result.contains("feed1"));
    }

    @Test
    public void testNoOverlap() {
        tree.insert("water", "No Overlap");
        Assert.assertEquals(tree.getSize(), 4);
    }

    @Test
    public void testInputKeySubset() {
        tree.insert("rand", "Input Subset");
        Assert.assertEquals(tree.getSize(), 4);

    }

    @Test
    public void testInputKeySuperset() {
        tree.insert("randomiser", "Input Superset");
        Assert.assertEquals(tree.getSize(), 4);
    }


    @Test
    public void testInputKeyPathStyle() {
        tree.insert("/data/cas/projects/", "path");
        Assert.assertEquals(tree.getSize(), 4);
        Assert.assertTrue(tree.find("/data/cas/projects/").contains("path"));
    }


    // Tests for find String
    @Test
    public void testSubstringPathFind() {
        tree.insert("/data/cas/projects/rtbd/", "rtbd");
        tree.insert("/data/cas/projects/dwh/", "dwh");
        Assert.assertEquals(tree.getSize(), 5);
        Assert.assertTrue(tree.find("/data/cas/projects/rtbd/").contains("rtbd"));
        Assert.assertTrue(tree.find("/data/cas/projects/dwh/").contains("dwh"));
        Assert.assertNull(tree.find("/data/cas/projects/"));
    }

    @Test
    public void testStringSplitFind() {
        tree.insert("rand", "rand");
        tree.insert("randomizer", "randomizer");
        Assert.assertTrue(tree.find("rand").contains("rand"));
        Assert.assertTrue(tree.find("random").contains("random"));
        Assert.assertTrue(tree.find("randomizer").contains("randomizer"));

    }

    //Tests for find using regular expression
    @Test
    public void testFindUsingRegex() {
        tree.insert("/data/cas/${YEAR}/", "rtbd");
        Assert.assertTrue(tree.find("/data/cas/2014/", regexAlgorithm).contains("rtbd"));
        Assert.assertNull(tree.find("/data/cas/", regexAlgorithm));
        Assert.assertNull(tree.find("/data/cas/2014/09", regexAlgorithm));
        Assert.assertNull(tree.find("/data/cas/${YEAR}/", regexAlgorithm));

        tree.insert("/data/cas/${YEAR}/colo", "local");
        tree.insert("/data/cas/${YEAR}/colo", "duplicate-local");
        Assert.assertNull(tree.find("/data/cas/${YEAR}/", regexAlgorithm));
        Assert.assertNull(tree.find("/data/cas/${YEAR}/colo", regexAlgorithm));
        Assert.assertNull(tree.find("/data/cas/", regexAlgorithm));
        Assert.assertTrue(tree.find("/data/cas/2014/", regexAlgorithm).contains("rtbd"));
        Assert.assertTrue(tree.find("/data/cas/2014/colo", regexAlgorithm).contains("local"));
        Assert.assertTrue(tree.find("/data/cas/2014/colo", regexAlgorithm).contains("duplicate-local"));


    }

    // Tests for delete method
    @Test
    public void testDeleteChildOfTerminal() {
        tree.insert("rand", "rand");
        tree.insert("randomizer", "randomizer");
        Assert.assertTrue(tree.delete("randomizer", "randomizer"));
        Assert.assertNull(tree.find("randomizer"));
        Assert.assertTrue(tree.find("random").contains("random"));
    }

    @Test
    public void testMarkingNonTerminal() {
        tree.insert("rand", "rand");
        tree.insert("randomizer", "randomizer");
        tree.delete("rand", "rand");
        Assert.assertNull(tree.find("rand"));
        Assert.assertTrue(tree.find("random").contains("random"));
        Assert.assertTrue(tree.find("randomizer").contains("randomizer"));
    }

    @Test
    public void testDoubleDelete() {
        tree.insert("rand", "rand");
        tree.insert("randomizer", "randomizer");
        Assert.assertTrue(tree.delete("rand", "rand"));
        Assert.assertFalse(tree.delete("rand", "rand"));
        Assert.assertNull(tree.find("rand"));
        Assert.assertTrue(tree.find("random").contains("random"));
        Assert.assertTrue(tree.find("randomizer").contains("randomizer"));
    }

    @Test
    public void testChildCompactionDelete() {
        tree.insert("rand", "rand");
        tree.insert("randomizer", "randomizer");
        Assert.assertTrue(tree.delete("random", "random"));
        Assert.assertNull(tree.find("random"));
        Assert.assertTrue(tree.find("rand").contains("rand"));
        Assert.assertTrue(tree.find("randomizer").contains("randomizer"));
        Assert.assertEquals(tree.getSize(), 4);
    }

    @Test
    public void testParentCompactionDelete() {
        tree.insert("rand", "rand");
        tree.insert("randomizer", "randomizer");
        Assert.assertTrue(tree.delete("randomizer", "randomizer"));
        Assert.assertNull(tree.find("randomizer"));
        Assert.assertTrue(tree.find("rand").contains("rand"));
        Assert.assertTrue(tree.find("random").contains("random"));
        Assert.assertEquals(tree.getSize(), 4);

    }

    @Test
    public void testSequencesOfDelete() {
        tree.insert("rand", "rand");
        tree.insert("randomizer", "randomizer");

        Assert.assertTrue(tree.delete("randomizer", "randomizer"));
        Assert.assertNull(tree.find("randomizer"));
        Assert.assertTrue(tree.find("rand").contains("rand"));
        Assert.assertTrue(tree.find("random").contains("random"));
        Assert.assertEquals(tree.getSize(), 4);

        Assert.assertTrue(tree.delete("rand", "rand"));
        Assert.assertNull(tree.find("rand"));
        Assert.assertTrue(tree.find("random").contains("random"));
        Assert.assertEquals(tree.getSize(), 3);

        Assert.assertTrue(tree.delete("random", "random"));
        Assert.assertNull(tree.find("random"));
        Assert.assertEquals(tree.getSize(), 2);

    }

    @Test
    public void testRootNotCompactedInDelete() {
        Assert.assertTrue(tree.delete("random", "random"));
        Assert.assertTrue(tree.delete("key2", "value2"));
        tree.insert("water", "water");
        Assert.assertTrue(tree.find("water").contains("water"));
    }

    @Test
    public void testDeleteFromListAndChildren() {
        //check that a delete of a key with multiple values and children is handled
        tree.insert("keyWithManyValuesAndChild", "value1");
        tree.insert("keyWithManyValuesAndChild", "value2");
        tree.insert("keyWithManyValuesAndChildren", "childValue");
        Assert.assertTrue(tree.delete("keyWithManyValuesAndChild", "value1"));
    }

    @Test
    public void testDeleteNonExistent() {
        Assert.assertFalse(tree.delete("zzz", "zzz"));
    }

    @Test
    public void testDeleteSubstring() {
        Assert.assertFalse(tree.delete("ke", "ke"));
    }

    @Test
    public void testDeleteNonTerminal() {
        Assert.assertFalse(tree.delete("key", "key"));
    }


    @Test
    public void testDeleteBlankOrEmptyOrNullString(){
        Assert.assertFalse(tree.delete("", ""));
        Assert.assertFalse(tree.delete(" ", " "));
        Assert.assertFalse(tree.delete(null, null));
    }

    @Test
    public void testAllSuffixForFirstLevelKey() {
        tree.insert("key123", "Key was key123");
        tree.insert("key124", "Key was key124");
        List<String> result = tree.findSuffixChildren("key", 2);
        Assert.assertEquals(result.size(), 2);
        Assert.assertTrue(result.contains("1"));
        Assert.assertTrue(result.contains("2"));
    }

    @Test
    public void testAllSuffixForNestedLevelKey() {
        tree.insert("key123", "Key was key123");
        tree.insert("key124", "Key was key124");
        Assert.assertEquals(tree.findSuffixChildren("key1", 2).size(), 1);
        Assert.assertEquals(tree.findSuffixChildren("key1", 2).get(0), "2");
    }

    @Test
    public void testFeedPropertiesEquals() {
        FeedLookupResult.FeedProperties f1 = new FeedLookupResult.FeedProperties("feed",
                LocationType.DATA, "cluster");
        FeedLookupResult.FeedProperties f1Copy = new FeedLookupResult.FeedProperties("feed",
                LocationType.DATA, "cluster");
        FeedLookupResult.FeedProperties f3 = new FeedLookupResult.FeedProperties("anotherFeed",
                LocationType.DATA, "cluster");
        FeedLookupResult.FeedProperties f4 = new FeedLookupResult.FeedProperties("feed",
                LocationType.STATS, "cluster");
        FeedLookupResult.FeedProperties f5 = new FeedLookupResult.FeedProperties("feed",
                LocationType.DATA, "anotherCluster");

        Assert.assertTrue(f1.equals(f1Copy));
        Assert.assertFalse(f1.equals(f3));
        Assert.assertFalse(f1.equals(f4));
        Assert.assertFalse(f1.equals(f5));

    }

    @Test
    public void testMultipleValues(){
        tree.insert("keyWithMultipleValues", "value1");
        tree.insert("keyWithMultipleValues", "value2");
        Assert.assertEquals(tree.find("keyWithMultipleValues").size(), 2);
        Assert.assertTrue(tree.find("keyWithMultipleValues").contains("value1"));
        Assert.assertTrue(tree.find("keyWithMultipleValues").contains("value2"));

        tree.delete("keyWithMultipleValues", "value1");
        Assert.assertTrue(tree.find("keyWithMultipleValues").contains("value2"));
        Assert.assertFalse(tree.find("keyWithMultipleValues").contains("value1"));

        tree.delete("keyWithMultipleValues", "value2");
        Assert.assertNull(tree.find("keyWithMultipleValues"));
    }
}
