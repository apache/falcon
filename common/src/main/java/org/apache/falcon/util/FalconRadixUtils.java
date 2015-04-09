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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.common.FeedDataPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Falcon specific utilities for the Radix Tree.
 */
public class FalconRadixUtils {

    /**
     * This interface implements the various algorithms to compare node's key with input based on whether you want
     * a regular expression based algorithm or a character by character matching algorithm.
     */
    public interface INodeAlgorithm {

        /**
         * Checks if the given key and input match.
         * @param key key of the node
         * @param input input String to be matched against key.
         * @return true if key and input match.
         */
        boolean match(String key, String input);

        boolean startsWith(String key, String input);

        /**
         * Finds next node to take for traversal among currentNode's children.
         * @param currentNode of RadixTree which has been matched.
         * @param input input String to be searched.
         * @return Node to be traversed next.
         */
        RadixNode getNextCandidate(RadixNode currentNode, String input);

        // for the given node and input key, finds the remainingText to be matched with child sub tree.
        String getRemainingText(RadixNode currentNode, String key);
    }

    /**
     * This Algorithm does a plain string comparison for all
     * type of operations on a node.
     */
    static class StringAlgorithm implements INodeAlgorithm {

        @Override
        public boolean match(String key, String input) {
            return StringUtils.equals(key, input);
        }

        @Override
        public boolean startsWith(String nodeKey, String inputKey) {
            return inputKey.startsWith(nodeKey);
        }

        @Override
        public RadixNode getNextCandidate(RadixNode currentNode, String input) {
            RadixNode newRoot = null;
            String remainingText = input.substring(currentNode.getKey().length());
            List<RadixNode> result = currentNode.getChildren();
            for(RadixNode child : result){
                if (child.getKey().charAt(0) == remainingText.charAt(0)){
                    newRoot = child;
                    break;
                }
            }
            return newRoot;
        }

        @Override
        public String getRemainingText(RadixNode currentNode, String key) {
            return key.substring(currentNode.getKey().length());
        }


    }

    /**
     * Regular Expression Algorithm for the radix tree.
     *
     * It traverses the radix tree and matches expressions like ${YEAR} etc. with their allowable values e.g. 2014
     */
    public static class FeedRegexAlgorithm implements INodeAlgorithm {

        /**
         * This function matches a feed path template with feed instance's path string.
         *
         * Key is assumed to be a feed's path template and inputString is assumed to be an instance's path string.
         * Variable/Regex parts of the feed's template are matched against the corresponding parts in inputString
         * using regular expression and for other parts a character by character match is performed.
         * e.g. Given templateString (/data/cas/${YEAR}/${MONTH}/${DAY}) and inputString (/data/cas/2014/09/09)
         * the function will return true.
         * @param templateString Node's key (Feed's template path)
         * @param inputString inputString String to be matched against templateString(instance's path)
         * @return true if the templateString and inputString match, false otherwise.
         */
        @Override
        public boolean match(String templateString, String inputString) {
            if (StringUtils.isBlank(templateString)) {
                return false;
            }
            // Divide the templateString and inputString into templateParts of regex and character matches
            List<String> templateParts = getPartsInPathTemplate(templateString);
            List<String> inputStringParts = getCorrespondingParts(inputString, templateParts);

            if (inputStringParts.size() != templateParts.size()) {
                return false;
            }

            int counter = 0;
            while (counter < inputStringParts.size()) {
                if (!matchPart(templateParts.get(counter), inputStringParts.get(counter))) {
                    return false;
                }
                counter++;
            }
            return true;
        }


        /**
         *
         * Finds if the current node's key is a prefix of the given inputString or not.
         *
         * @param inputTemplate inputTemplate String
         * @param inputString inputString to be checked
         * @return true if inputString starts with inputTemplate, false otherwise.
         */
        @Override
        public boolean startsWith(String inputTemplate, String inputString) {

            if (StringUtils.isBlank(inputString)) {
                return false;
            }
            if (StringUtils.isBlank(inputTemplate)) {
                return true;
            }

            // divide inputTemplate and inputString into corresponding templateParts of regex and character only strings
            List<String> templateParts = getPartsInPathTemplate(inputTemplate);
            List<String> remainingPattern = getCorrespondingParts(inputString, templateParts);

            if (templateParts.size() > remainingPattern.size()) {
                return false;
            }

            int counter = 0;
            // compare part by part till the templateParts end
            for (String templatePart : templateParts) {
                String part = remainingPattern.get(counter);
                if (!matchPart(templatePart, part)) {
                    return false;
                }
                counter++;
            }
            return true;
        }

        @Override
        public RadixNode getNextCandidate(RadixNode currentNode, String input) {
            RadixNode newRoot = null;
            // replace the regex with pattern's length
            String remainingText = input.substring(getPatternsEffectiveLength(currentNode.getKey()));
            List<RadixNode> result = currentNode.getChildren();
            for(RadixNode child : result) {
                String key = child.getKey();
                if (key.startsWith("${")) {
                    // get the regex
                    String regex = key.substring(0, key.indexOf("}") + 1);
                    // match the text and the regex
                    FeedDataPath.VARS var = getMatchingRegex(regex);
                    if (matchPart(regex, input.substring(0, var.getValueSize()))) {
                        newRoot = child; // if it matches then this is the newRoot
                        break;
                    }
                } else if (child.getKey().charAt(0) == remainingText.charAt(0)) {
                    newRoot = child;
                    break;
                }
            }
            return newRoot;
        }

        @Override
        public String getRemainingText(RadixNode currentNode, String inputString) {
            // find the match length for current inputString
            return inputString.substring(getPatternsEffectiveLength(currentNode.getKey()));
        }

        private int getPatternsEffectiveLength(String templateString) {
            if (StringUtils.isBlank(templateString)) {
                return 0;
            }

            // Since we are only interested in the length, can replace pattern with a random string
            for (FeedDataPath.VARS var : FeedDataPath.VARS.values()) {
                templateString = templateString.replace("${" + var.name() + "}",
                        RandomStringUtils.random(var.getValueSize()));
            }

            return templateString.length();
        }

        /**
         * Divide a given template string into parts of regex and character strings
         * e.g. /data/cas/${YEAR}/${MONTH}/${DAY} will be converted to
         * [/data/cas/, ${YEAR}, /, ${MONTH}, /, ${DAY}]
         * @param templateString input string representing a feed's path template
         * @return list of parts in input templateString which are either completely regex or normal string.
         */
        private List<String> getPartsInPathTemplate(String templateString) {
            //divide the node's templateString in parts of regular expression and normal string
            List<String> parts = new ArrayList<String>();
            Matcher matcher = FeedDataPath.PATTERN.matcher(templateString);
            int currentIndex = 0;
            while (matcher.find()) {
                parts.add(templateString.substring(currentIndex, matcher.start()));
                parts.add(matcher.group());
                currentIndex = matcher.end();
            }
            if (currentIndex != templateString.length()) {
                parts.add(templateString.substring(currentIndex));
            }
            return Collections.unmodifiableList(parts);
        }


        private FeedDataPath.VARS getMatchingRegex(String inputPart) {
            //inputPart will be something like ${YEAR}

            inputPart = inputPart.replace("${", "\\$\\{");
            inputPart = inputPart.replace("}", "\\}");

            for (FeedDataPath.VARS var : FeedDataPath.VARS.values()) {
                if (inputPart.equals("${" + var.name() + "}")) {
                    return var;
                }
            }
            return null;
        }


        /**
         * Divides a string into corresponding parts for the template to carry out comparison.
         * templateParts = [/data/cas/, ${YEAR}, /, ${MONTH}, /, ${DAY}]
         * inputString = /data/cas/2014/09/09
         * returns [/data/cas/, 2014, /, 09, /, 09]
         * @param inputString normal string representing feed instance path
         * @param templateParts parts of feed's path template broken into regex and non-regex units.
         * @return a list of strings where each part of the list corresponds to a part in list of template parts.
         */
        private List<String> getCorrespondingParts(String inputString, List<String> templateParts) {
            List<String> stringParts = new ArrayList<String>();
            int counter = 0;
            while (StringUtils.isNotBlank(inputString) && counter < templateParts.size()) {
                String currentTemplatePart = templateParts.get(counter);
                int length = Math.min(getPatternsEffectiveLength(currentTemplatePart), inputString.length());
                stringParts.add(inputString.substring(0, length));
                inputString = inputString.substring(length);
                counter++;
            }
            if (StringUtils.isNotBlank(inputString)) {
                stringParts.add(inputString);
            }
            return stringParts;
        }

        /**
         * Compare a pure regex or pure string part with a given string.
         *
         * @param template template part, which can either be a pure regex or pure non-regex string.
         * @param input input String to be matched against the template part.
         * @return true if the input string matches the template, in case of a regex component a regex comparison is
         * made, else a character by character comparison is made.
         */
        private boolean matchPart(String template, String input) {
            if (template.startsWith("${")) { // if the part begins with ${ then it's a regex part, do regex match
                template = template.replace("${", "\\$\\{");
                template = template.replace("}", "\\}");
                for (FeedDataPath.VARS var : FeedDataPath.VARS.values()) {//find which regex is this
                    if (StringUtils.equals(var.regex(), template)) {// regex found, do matching
                        //find part of the input string which should be matched against regex
                        String desiredPart = input.substring(0, var.getValueSize());
                        Pattern pattern = Pattern.compile(var.getValuePattern());
                        Matcher matcher = pattern.matcher(desiredPart);
                        if (!matcher.matches()) {
                            return false;
                        }
                        return true;
                    }
                }
                return false;
            } else {// do exact match with normal strings
                if (!input.startsWith(template)) {
                    return false;
                }
            }
            return true;
        }
    }
}
