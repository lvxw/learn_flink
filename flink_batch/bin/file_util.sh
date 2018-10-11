#!/bin/bash
###############################################################################
#Script:        file_utils.sh
#Author:        吕学文<lvxw@fun.tv>
#Date:          2018-10-08
#Description:   操作文件相关的工具
#Usage:         工具shell，供其他shell脚本引入
#Jira:
###############################################################################

function make_local_dir(){
    [[ ! -d $1 ]] && mkdir -p $1
}

function make_hdfs_dir(){
    hdfs dfs -test -d $1
    [[ $? -ne 0 ]] && hdfs dfs -mkdir -p $1
}

function remove_local_dir_or_file(){
    [[ -e $1 ]] && rm -rf $1
}

function remove_hdfs_dir_or_file(){
    hdfs dfs -test -e $1
    [[ $? -eq 0 ]] && hdfs dfs -rm -r $1
}