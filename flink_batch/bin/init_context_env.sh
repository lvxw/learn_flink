#!/bin/bash
###############################################################################
#Script:        init_context_env.sh
#Author:        吕学文<2622478542@qq.com>
#Date:          2018-10-08
#Description:   处理天数据时的base脚本
#Usage:         工具shell，供其他shell脚本引入
#Jira:
###############################################################################
if(( $# == 0 ))
then
    echo "There must be a parameter" >&2
    exit 1
else
    day_or_hour=$1
    if(( $1 == day ))
    then
         if(( $# == 2 ))
         then
            if [[ $2 == [1-3][0-9][0-9][0-9][0-1][0-9][0-3][0-9] ]]
            then
                 data_date=$2
            else
                echo "The second parameter default data_date is last day." >&2
                exit 1
            fi
         else
            data_date=`date -d "-1 days" +%Y%m%d`
         fi
    elif(( $1 == hour ))
    then
        if(( $# == 2 ))
         then
            if [[ $2 == [1-3][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9] ]]
            then
                 data_time=$2
            else
                echo "The second parameter default data_time is last hour." >&2
                exit 1
            fi
         else
            data_time=`date -d "-1 days" +%Y%m%d`
         fi
    else
        echo "The first parameter must be day or hour" >&2
    fi
fi



#设置脚本运行环境和全局变量
function set_base_env(){
    source etc/learn_flink.conf
}

#初始化当前脚本全局变量
function base_init(){
    if(( ${day_or_hour} == day ))
    then
        year=${data_date:0:4}                       #年
        month=${data_date:4:2}                      #月
        day=${data_date:6:2}                        #日
        week=`date -d ${data_date} +%w`             #星期
    else
        year=${data_time:0:4}                       #年
        month=${data_time:4:2}                      #月
        day=${data_time:6:2}                        #日
        hour=${data_time:8:2}                       #时
        week=`date -d ${data_time} +%w`             #星期
    fi
}

set_base_env
base_init