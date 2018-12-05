#!/bin/bash
###############################################################################
#Script:        wordCount_mr.sh
#Author:        吕学文<2622478542@qq.com>
#Date:          2018-10-08
#Description:
#Usage:         wordCount_mr.sh
#Jira:
###############################################################################

#设置脚本运行环境和全局变量
function set_env(){
    cd `cd $(dirname $0)/../.. && pwd`
    source bin/init_context_env.sh day $1
}

#设置日、周、月的数据输入、输出路径
function init(){
    hdfs_test_input=/tmp/lvxw/words
    hdfs_test_output=/tmp/lvxw/out
}

function execute_mr(){
    $FLINK_INSTALL/bin/flink run \
        -m yarn-cluster \
        -yD fs.default-scheme=hdfs://artemis-02:9000/ \
        -yD fs.output.always-create-directory=true \
        -yD fs.overwrite-file=true \
        -ynm WordCount \
        -yn 2 -yjm 2048 -ytm 2048 -ys 2 \
        -c com.test.business.template.WordCount \
        jar/LearnFlink.jar \
        "{\"input_dir\":\"${hdfs_test_input}\", \
          \"output_dir\":\"${hdfs_test_output}\", \
          \"run_pattern\":\"${RUN_PATTERN}\"
        }"
}

set_env $1
init
execute_mr