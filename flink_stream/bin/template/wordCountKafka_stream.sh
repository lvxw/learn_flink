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
    topic=test-flink
    checkpoint_interval=5000
    checkpoint_mode=EXACTLY_ONCE
}

function execute_mr(){
    $FLINK_INSTALL/bin/flink run \
        -m yarn-cluster \
        -ynm WordCountKafkaStream \
        -yn 2 -yjm 1024 -ytm 1024 -ys 1 \
        -c com.test.business.template.WordCountKafkaStream \
        jar/LearnFlink.jar \
        "{\"topic\":\"${topic}\", \
          \"run_pattern\":\"${RUN_PATTERN}\", \
          \"checkpoint_interval\":\"${checkpoint_interval}\", \
          \"checkpoint_mode\":\"${checkpoint_mode}\" \
        }"
}

set_env $1
init
execute_mr