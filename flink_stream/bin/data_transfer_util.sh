#!/bin/bash
###############################################################################
#Script:        data_transfer_util.sh
#Author:        吕学文<2622478542@qq.com>
#Date:          2018-10-08
#Description:   数据迁移的相关操作
#Usage:         工具shell，供其他shell脚本引入
#Jira:
###############################################################################

#将hdfs结构化数据插入mysql
function hdfs_to_mysql(){
    table=$1
    columns=$2
    hdfs_dir=$3
    delimiter=$4

    ${SQOOP_INSTALL}/bin/sqoop export  \
     --connect \"${JDBC_CONNECT}\" \
     --username ${ARTEMIS_USER} \
     --password ${ARTEMIS_PWD} \
     --table ${table} \
     --m 1 \
     --export-dir ${hdfs_dir} \
     --input-fields-terminated-by \"${delimiter}\" \
     --columns=\"${columns}\"
}

#加载hdfs天数据到hive外部分区表
load_hdfs_day_data_to_hive(){
    hql_file_path=$1
    hive_db=$2
    hive_table=$3
    hdfs_log_base=$4
    echo "use ${hive_db};" > ${hql_file_path}
    echo "alter table ${hive_table} add if not exists partition(year=\"${year}\", month=\"${month}\", day=\"${day}\") LOCATION \"${hdfs_log_base}\";" >> ${hql_file_path}

    $HIVE_INSTALL/bin/hive -f ${hql_file_path}
}

#加载hdfs小时数据到hive外部分区表
load_hdfs_hour_data_to_hive(){
    hql_file_path=$1
    hive_db=$2
    hive_table=$3
    hdfs_log_base=$4
    echo "use ${hive_db};" > ${hql_file_path}
    for hour in $(seq -w 0 23)
    do
            echo "alter table ${hive_table} add if not exists partition(year=\"${year}\", month=\"${month}\", day=\"${day}\",hour=\"${hour}\") LOCATION \"${hdfs_log_base}/${hour}\";" >> ${hql_file_path}
    done

    $HIVE_INSTALL/bin/hive -f ${hql_file_path}
}