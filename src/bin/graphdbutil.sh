#!/bin/sh
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#


usage() {
  echo "usage: $0  operation java-home hadoop-home falcon-home falcon-common-jar input/out-dir"
  echo "  where operation is either export OR import"
  echo "        java-home is the java installation location"
  echo "        hadoop-home is the hadoop installation location"
  echo "        falcon-home is the falcon home installation location"
  echo "        falcon-common-jar is the falcon-common-<version>.jar location with GraphUtils"
  echo "        input/output dir is the directory for the graph data"
  exit 1
}

if [ $# != 6 ]; then
  usage
fi

operation=$1
java_home=$2
hadoop_home=$3
falcon_home=$4
falcon_common_jar=$5
util_dir=$6

export=0
import=0
keep_temp=Y

case $operation in
   import) import=1
           ;;
   export) export=1
           ;;
   *)     echo "Unknown operation $operation"
          usage
esac

if [ -d  $java_home -a -f $java_home/bin/java -a -f $java_home/bin/jar ] ; then
  :
else
  echo "Invalid java home directory $java_home"
  usage
fi

if [ -d  $hadoop_home -a -f $hadoop_home/bin/hadoop ] ; then
  :
else
  echo "Invalid hadoop home directory $hadoop_home"
  usage
fi

if [ -d  $falcon_home -a -f $falcon_home/bin/falcon ] ; then
  :
else
  echo "Invalid falcon home directory $falcon_home"
  usage
fi

falcon_war=$falcon_home/server/webapp/falcon.war
if [ ! -f $falcon_war ]; then
  echo "Falcon war file $falcon_war not available"
  usage
fi

if [ ! -f $falcon_common_jar ]; then
  echo "Falcon commons jar file $falcon_common_jar not available"
  usage
fi


util_tmpdir=/tmp/falcon-graphutil-tmp-$$
echo "Using $util_tmpdir as temporary directory"
trap "rm -rf $util.tmpdir" 0 2 3 15
rm -rf $util_tmpdir
mkdir -p $util_tmpdir

if [ ! -d $util_dir ]; then
   echo "Directory $util_dir does not exist"
   usage
fi

if [ x$import = x1 ]; then
   if [ ! -f $metadata_file ]; then
      echo "Directory $util_dir does not exist or $metadata_file not present"
      usage
   fi
fi

cd $util_tmpdir
jar -xf $falcon_war
rm ./WEB-INF/lib/jackson*  ./WEB-INF/lib/falcon-common*.jar ./WEB-INF/lib/slf4j* ./WEB-INF/lib/activemq*
cp $falcon_common_jar ./WEB-INF/lib/

JAVA_HOME=$java_home
export PATH=$JAVA_HOME/bin:$PATH
export CLASSPATH="$falcon_home/conf:./WEB-INF/lib/*:`$hadoop_home/bin/hadoop classpath`"
echo "Using classpath $CLASSPATH"
java -Dfalcon.log.dir=/tmp/ org.apache.falcon.metadata.GraphUpdateUtils $operation $util_dir

if [ x$keep_temp = xY ]; then
  :
else
  rm -rf $util_tmpdir
fi