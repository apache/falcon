#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="Python WordCount")
    
    # Read input and output path
    inputPath = sys.argv[1]
    print ('Path of input file ->' + inputPath)
    outputPath = sys.argv[2]
    print ('Path of output file ->' + outputPath)
    
    distFile = sc.textFile(inputPath)
    
    def flatMap(line):
        return line.split(",")
    
    def map(word):
        return (word,1)
    
    def reduce(a,b):
        return a+b
    
    
    counts = distFile.flatMap(flatMap).map(map).reduceByKey(reduce)
    
    counts.saveAsTextFile(outputPath)
