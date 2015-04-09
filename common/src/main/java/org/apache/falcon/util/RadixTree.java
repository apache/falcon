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
import org.apache.falcon.entity.store.FeedPathStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Formattable;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * A thread-safe Radix Tree implementation of the LocationStore.
 *
 *
 * A radix tree (also patricia trie or radix trie or compact prefix tree) is a space-optimized
 * trie data structure where each node with only one child is merged with its parent.
 *
 * For example the tree representation for the following (key,value) pairs -
 * [("key1", "value1"), ("key123", "Key was key123"), ("key124", "Key was key124"),
 * ("key2", "value2"), ("random", "random")] will be as below.
 *
 * |
 *    |-key
 *    |--1[[value1]]*
 *    |---2
 *    |----3[[Key was key123]]*
 *    |----4[[Key was key124]]*
 *    |--2[[value2]]*
 *    |-random[[random]]*
 *
 * For more details on Radix Tree please refer
 * <a href="http://en.wikipedia.org/wiki/Radix_tree">Radix Tree</a>
 * @param <T> Type of value being stored against the key.
 */
public class RadixTree<T> implements FeedPathStore<T>, Formattable {
    private static final Logger LOG = LoggerFactory.getLogger(RadixTree.class);

    protected RadixNode<T> root;

    private int size;

    public RadixTree(){
        root = new RadixNode<T>();
        root.setKey("");
        size = 0;
    }

    /**
     * Return the number of keys stored in the tree.
     *
     * Since all keys end in terminal nodes and duplicate keys are not allowed,
     * size is equal to the number of terminal nodes in the tree.
     * @return number of keys in the tree.
     */
    @Override
    public synchronized int getSize() {
        return size;
    }

    /**
     * Insert a <key, value> pair in the Radix Tree.
     *
     * @param key Key to be stored
     * @param value Value to be stored against that key
     */
    @Override
    public synchronized void insert(@Nullable String key, @Nonnull T value){
        if (key != null && !key.trim().isEmpty()){
            LOG.debug("Insert called for key: {} and value: {}", key.trim(), value);
            insertKeyRecursive(key.trim(), value, root);
        }
    }

    private void insertKeyRecursive(String remainingText, T value, RadixNode<T> currentNode){

        int currentMatchLength = currentNode.getMatchLength(remainingText);
        String newRemainingText = remainingText.substring(currentMatchLength, remainingText.length());

        // if root or current node key is subset of the input key GO DOWN
        if (currentNode.isRoot()
                || (currentMatchLength == currentNode.getKey().length()
                && currentMatchLength < remainingText.length())){

            // if a path to go down exists then go down that path
            boolean foundPath = false;
            for(RadixNode<T> child: currentNode.getChildren()){
                if (child.getKey().charAt(0) == newRemainingText.charAt(0)){
                    insertKeyRecursive(newRemainingText, value, child);
                    foundPath = true;
                    break;
                }
            }
            // else create a new node.
            if (!foundPath){
                RadixNode<T> node = new RadixNode<T>();
                node.setKey(newRemainingText);
                node.addValue(value);
                node.setTerminal(true);
                currentNode.getChildren().add(node);
                size += 1;
            }
        }else if (currentMatchLength == remainingText.length() && currentMatchLength < currentNode.getKey().length()){
            // if remainingText is subset of the current node key
            RadixNode<T> node = new RadixNode<T>();
            node.setChildren(currentNode.getChildren());
            node.setKey(currentNode.getKey().substring(currentMatchLength));
            node.setValues(currentNode.getValues());
            node.setTerminal(currentNode.isTerminal());

            currentNode.setChildren(new LinkedList<RadixNode<T>>());
            currentNode.getChildren().add(node);
            currentNode.setTerminal(true);
            currentNode.setKey(currentNode.getKey().substring(0, currentMatchLength));
            currentNode.removeAll();
            currentNode.addValue(value);

            size += 1;

        }else if (currentMatchLength < remainingText.length() && currentMatchLength < currentNode.getKey().length()){

            //add new Node and move all current node's children and value to it
            RadixNode<T> node = new RadixNode<T>();
            node.setChildren(currentNode.getChildren());
            node.setTerminal(currentNode.isTerminal());
            node.setValues(currentNode.getValues());
            node.setKey(currentNode.getKey().substring(currentMatchLength, currentNode.getKey().length()));

            // add node for the text
            RadixNode<T> node2 = new RadixNode<T>();
            node2.setKey(newRemainingText);
            node2.setTerminal(true);
            node2.addValue(value);

            //update current node to be new root
            currentNode.setTerminal(false);
            currentNode.setKey(currentNode.getKey().substring(0, currentMatchLength));
            currentNode.setChildren(new LinkedList<RadixNode<T>>());
            currentNode.getChildren().add(node);
            currentNode.getChildren().add(node2);

            size += 1;
        }else if (currentMatchLength == remainingText.length() && currentMatchLength == currentNode.getKey().length()){
            // if current node key and input key both match equally
            if (currentNode.isTerminal()){
                currentNode.addValue(value);
            }else {
                currentNode.setTerminal(true);
                currentNode.addValue(value);
            }
            size += 1;
        }
    }

    /**
     * Find the value for the given key if it exists in the tree, null otherwise.
     *
     * A key is said to exist in the tree if we can generate exactly that string
     * by going down from root to a terminal node. If a key exists we return the value
     * stored at the terminal node.
     *
     * @param key - input key to be searched.
     * @return T Value of the key if it exists, null otherwise
     */
    @Override
    @Nullable
    public synchronized Collection<T> find(@Nonnull String key, FalconRadixUtils.INodeAlgorithm algorithm) {
        if (key != null && !key.trim().isEmpty()) {
            if (algorithm == null) {
                algorithm = new FalconRadixUtils.StringAlgorithm();
            }
            return recursiveFind(key.trim(), root, algorithm);
        }
        return null;
    }

    @Nullable
    @Override
    public Collection<T> find(@Nonnull String key) {
        if (key != null && !key.trim().isEmpty()) {
            FalconRadixUtils.INodeAlgorithm algorithm = new FalconRadixUtils.StringAlgorithm();
            return recursiveFind(key.trim(), root, algorithm);
        }
        return null;
    }

    private Collection<T> recursiveFind(String key, RadixNode<T> currentNode,
        FalconRadixUtils.INodeAlgorithm algorithm){

        if (!algorithm.startsWith(currentNode.getKey(), key)){
            LOG.debug("Current Node key: {} is not a prefix in the input key: {}", currentNode.getKey(), key);
            return null;
        }

        if (algorithm.match(currentNode.getKey(), key)){
            if (currentNode.isTerminal()){
                LOG.debug("Found the terminal node with key: {} for the given input.", currentNode.getKey());
                return currentNode.getValues();
            }else {
                LOG.debug("currentNode is not terminal. Current node's key is {}", currentNode.getKey());
                return null;
            }
        }

        //find child to follow, using remaining Text
        RadixNode<T> newRoot = algorithm.getNextCandidate(currentNode, key);
        String remainingText = algorithm.getRemainingText(currentNode, key);

        if (newRoot == null){
            LOG.debug("No child found to follow for further processing. Current node key {}");
            return null;
        }else {
            LOG.debug("Recursing with new key: {} and new remainingText: {}", newRoot.getKey(), remainingText);
            return recursiveFind(remainingText, newRoot, algorithm);
        }
    }

    /**
     *  Deletes a given key,value pair from the Radix Tree.
     *
     * @param key key to be deleted
     * @param value value to be deleted
     */
    @Override
    public synchronized boolean delete(@Nonnull String key, @Nonnull T value) {
        if (key != null && !key.trim().isEmpty()){
            LOG.debug("Delete called for key:{}", key.trim());
            return recursiveDelete(key, null, root, value);
        }
        return false;
    }

    private boolean recursiveDelete(String key, RadixNode<T> parent, RadixNode<T> currentNode, T value){
        LOG.debug("Recursing with key: {}, currentNode: {}", key, currentNode.getKey());
        if (!key.startsWith(currentNode.getKey())){
            LOG.debug("Current node's key: {} is not a prefix of the remaining input key: {}",
                    currentNode.getKey(), key);
            return false;
        }

        if (StringUtils.equals(key, currentNode.getKey())){
            LOG.trace("Current node's key:{} and the input key:{} matched", currentNode.getKey(), key);
            if (currentNode.getValues().contains(value)){
                LOG.debug("Given value is found in the collection of values against the given key");
                currentNode.removeValue(value);
                size -= 1;
                if (currentNode.getValues().size() == 0){
                    LOG.debug("Exact match between current node's key: {} and remaining input key: {}",
                        currentNode.getKey(), key);
                    if (currentNode.isTerminal()){
                        //if child has no children & only one value, then delete and compact parent if needed
                        if (currentNode.getChildren().size() == 0){
                            Iterator<RadixNode<T>> it = parent.getChildren().iterator();
                            while(it.hasNext()){
                                if (StringUtils.equals(it.next().getKey(), currentNode.getKey())){
                                    it.remove();
                                    LOG.debug("Deleting the node");
                                    break;
                                }
                            }
                        }else if (currentNode.getChildren().size() > 1){
                            // if child has more than one children just mark non terminal
                            currentNode.setTerminal(false);
                        }else if (currentNode.getChildren().size() == 1){
                            // if child has only one child then compact node
                            LOG.debug("compacting node with child as node to be deleted has only 1 child");
                            RadixNode<T> child = currentNode.getChildren().get(0);
                            currentNode.setChildren(child.getChildren());
                            currentNode.setTerminal(child.isTerminal());
                            currentNode.setKey(currentNode.getKey() + child.getKey());
                            currentNode.setValues(child.getValues());
                        }

                        //parent can't be null as root will never match with input key as it is not a terminal node.
                        if (!parent.isTerminal() && !parent.isRoot()){
                            // if only one child left in parent and parent is not root then join parent
                            // and the only child key
                            if (parent.getChildren().size() == 1){
                                RadixNode<T> onlyChild = parent.getChildren().get(0);
                                String onlyChildKey = onlyChild.getKey();
                                LOG.debug("Compacting child: {} and parent: {}", onlyChildKey, parent.getKey());
                                parent.setKey(parent.getKey() + onlyChildKey);
                                parent.setChildren(onlyChild.getChildren());
                                parent.setTerminal(onlyChild.isTerminal());
                                parent.setValues(onlyChild.getValues());
                            }
                        }
                        return true;
                    }else{
                        LOG.debug("Key found only as a prefix and not at a terminal node");
                        return false;
                    }
                }
                return true;
            }else {
                LOG.debug("Current value is not found in the collection of values against the given key, no-op");
                return false;
            }
        }

        LOG.debug("Current node's key: {} is a prefix of the input key: {}", currentNode.getKey(), key);
        //find child to follow
        RadixNode<T> newRoot = null;
        String remainingKey = key.substring(currentNode.getMatchLength(key));
        for(RadixNode<T> el : currentNode.getChildren()){
            LOG.trace("Finding next child to follow. Current child's key:{}", el.getKey());
            if (el.getKey().charAt(0) == remainingKey.charAt(0)){
                newRoot = el;
                break;
            }
        }

        if (newRoot == null){
            LOG.debug("No child was found with common prefix with the remainder key: {}", key);
            return false;
        }else {
            LOG.debug("Found a child's key: {} with common prefix, recursing on it", newRoot.getKey());
            return recursiveDelete(remainingKey, currentNode, newRoot, value);
        }
    }


    /**
     * Useful for debugging.
     */
    @Override
    public void formatTo(Formatter formatter, int flags, int width, int precision) {
        formatNodeTo(formatter, 0, root);

    }

    private void formatNodeTo(Formatter formatter, int level, RadixNode<T> node){
        for (int i = 0; i < level; i++) {
            formatter.format(" ");
        }
        formatter.format("|");
        for (int i = 0; i < level; i++) {
            formatter.format("-");
        }

        if (node.isTerminal()){
            formatter.format("%s[%s]*%n", node.getKey(),  node.getValues());
        }else{
            formatter.format("%s%n", node.getKey());
        }

        for (RadixNode<T> child : node.getChildren()) {
            formatNodeTo(formatter, level + 1, child);
        }
    }

    /**
     * Find List of substring of keys which have given input as a prefix.
     *
     * @param key - Input string for which all Suffix Children should be returned
     * @param limit - Maximum Number of results. If limit is less than 0 then all nodes are returned.
     *              If limit is 0 then returns null.
     */
    @javax.annotation.Nullable
    public List<String> findSuffixChildren(String key, int limit){
        if (key == null || limit == 0){
            return null;
        }
        RadixNode<T> currentNode = root;
        String remainingText = key.trim();
        List<String> result = new LinkedList<String>();
        do{
            boolean flag = false;
            // find the child with common prefix
            for(RadixNode<T> child: currentNode.getChildren()){
                LOG.debug("Checking for child key: {} against remainingText: {}", child.getKey(), remainingText);
                if (child.getKey().charAt(0) == remainingText.charAt(0)){
                    LOG.debug("Child key: {} found to have overlap with the remainingText: {}", child.getKey(),
                            remainingText);
                    flag = true;

                    //if entire key doesn't match return null
                    if (!remainingText.startsWith(child.getKey())){
                        return null;
                    }

                    // if entire key equals remainingText - return it's children up to the specified limit
                    if (StringUtils.equals(child.getKey(), remainingText)){
                        int counter = 0;

                        for(RadixNode<T> suffixChild: child.getChildren()){
                            if (limit < 0 || counter < limit){
                                result.add(suffixChild.getKey());
                            }
                        }
                        return Collections.unmodifiableList(result);
                    }

                    //if entire key matches but it is not equal to entire remainingText - repeat
                    remainingText = remainingText.substring(child.getKey().length());
                    currentNode = child;
                    break;

                }
            }
            // if no child found with common prefix return null;
            if (!flag){
                return null;
            }
        }while (true);
    }
}
