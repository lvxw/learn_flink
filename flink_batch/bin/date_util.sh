#!/bin/bash
###############################################################################
#Script:        date_util.sh
#Author:        吕学文<lvxw@fun.tv>
#Date:          2018-10-08
#Description:   常用处理时间的函数
#Usage:         工具shell，供其他shell脚本引入
#Jira:
###############################################################################

#初始化脚本运行参数data_date日期后一天的 天（几号）、星期
function init_util(){
    week_day=`get_week_day $1`
    month_day=`get_month_day $1`
}

#获取传入时间的前后某n天的日期
#接收两个参数：第一个参数
#                      日期字符传，如 2018-02-02 或者 20180202 或者 2018/02/02
#            第二个参数
#                      过去或将来几天，负数表示过去，整数表示将来。例如 -2  1
function get_next_or_before_date(){
    local param_date=$1
    local days=$2
    local delimiter=${param_date:4:1}
    if [ ${delimiter} != '-' -a  ${delimiter} != '/' ]
    then
        delimiter=""
    fi

	local return_date=$(date -d @$((`date -d ${param_date} +%s` + 86400*${days})) +%Y${delimiter}%m${delimiter}%d)
	echo  $return_date
}

#获取传入时间的星期
#接收两个参数：第一个参数
#                      日期字符传，如 2018-02-02 或者 20180202 或者 2018/02/02
function get_week_day(){
    local param_date=$1
    local return_week=$(date -d @$((`date -d "$param_date" +%s`)) +%w)
    echo ${return_week}
}

#获取传入时间的几号
#接收两个参数：第一个参数
#                      日期字符传，如 2018-02-02 或者 20180202 或者 2018/02/02
function get_month_day(){
    local param_date=$1
    local delimiter=${param_date:4:1}
    if [ ${delimiter} != '-' -a  ${delimiter} != '/' ]
    then
        local return_month=${param_date:6:2}
    else
        local return_month=${param_date:8:2}
    fi
    echo ${return_month}
}