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

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Represents a node in Radix Tree.
 *
 * Each node contains a part of the key, links to it's children and a collection of values
 * stored against the key(if the node is the suffix of a key)
 *
 */
public class RadixNode<T> {

    private String key;

    private List<RadixNode<T>> children;

    private boolean isTerminal;

    private Set<T> values;

    public RadixNode(){
        key = "";
        children = new LinkedList<RadixNode<T>>();
        isTerminal = false;
        values = new HashSet<T>();
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<RadixNode<T>> getChildren() {
        return children;
    }

    public void setChildren(List<RadixNode<T>> children) {
        this.children = children;
    }

    public boolean isTerminal() {
        return isTerminal;
    }

    public void setTerminal(boolean isTerminalNew) {
        this.isTerminal = isTerminalNew;
    }

    /**
     * Root node is the node with a token string(empty String in our case)
     * as key.
     *
     * @return True if the node is root Node, False otherwise
     */
    public boolean isRoot(){
        return StringUtils.equals(key, "");
    }

    public Collection<T> getValues() {
        return Collections.unmodifiableCollection(values);
    }

    public void setValues(Collection<T> newValues) {
        values = new HashSet<T>();
        values.addAll(newValues);
    }

    public void addValue(T value){
        values.add(value);
    }

    public void removeValue(T value) {
        values.remove(value);
    }
    public void removeAll() {
        values.clear();
    }

    public boolean containsValue(T value){
        return values.contains(value);
    }

    public int getMatchLength(String input){
        int matchLength = 0;

        if (input == null){
            return 0;
        }

        while(matchLength < key.length()
                && matchLength < input.length()
                && input.charAt(matchLength) == key.charAt(matchLength)){
            matchLength += 1;
        }

        return matchLength;
    }


    /**
     * Finds the length of the match between node's key and input.
     *
     * It can do either a character by character match or a regular expression match(used to match a feed instance path
     * with feed location template). Only regular expressions allowed in the feed path are evaluated for matching.
     * @param input input string to be matched with the key of the node.
     * @param matcher A custom matcher algorithm to match node's key against the input. It is used when matching
     *                path of a Feed's instance to Feed's path template.
     * @return
     */
    public boolean matches(String input, FalconRadixUtils.INodeAlgorithm matcher) {
        if (input == null) {
            return false;
        }

        if (matcher == null) {
            return StringUtils.equals(getKey(), input);
        }

        return matcher.match(this.getKey(), input);
    }
}
