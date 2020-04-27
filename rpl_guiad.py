import pymysql
import  time
import  re
import  os


conn = pymysql.connect(
    host='192.168.221.130',
    user='root',
    password='root.2019',
    database='axe',
    port=3306,
    charset="utf8")
cur=conn.cursor()
conn.autocommit(1)

show_slave="show slave  status"
show_gtid_mode=" select @@gtid_mode ;"
get_pk = "select column_name,ordinal_position from information_schema.columns where table_schema='%s' and table_name='%s' and column_key='PRI';"
get_binlog ="mysqlbinlog  -v --base64-output=DECODE-ROWS -R --host=%s --port=%d --user='root' --password='root.2019' --start-position=%d --stop-position=%d   %s   | grep @$s  |head -n 1"
workers="SELECT  SERVICE_STATE, LAST_ERROR_NUMBER , LAST_ERROR_MESSAGE  FROM  performance_schema.`replication_applier_status_by_worker`   WHERE  last_error_number !=0 "
text=r"""
Worker 1 failed executing transaction '66995dc3-5eaa-11ea-a83a-0242ac110002:9' at master log mysql-bin.000005, end_log_pos 965;
 Could not execute Update_rows event on table (.*); Can't find record in 'meter',Error_code: 1032; 
 handler error HA_ERR_KEY_NOT_FOUND; the event's master (.*), end_log_pos (.*)
"""


def slect_eter(sql):
    # sql='show  processlist;'
    cur.execute(sql)
    return cur.fetchall()
    print(cur.fetchall())

def  get_gtid_mode():
    cur.execute(show_gtid_mode)
    a=cur.fetchall()
    print(a)
    if a[0][0]=='ON':
        return 1
    else:
        return  0

def get_tb_pk(db_name ,tb_name):
    cur.execute(get_pk%(db_name,tb_name))
    pk=cur.fetchall()
    print(pk)
    return pk

def get_workers(sql):
    cur.execute(sql)
    a=cur.fetchall()
    return a

def slave_status():
    cur.execute(show_slave)
    a=cur.fetchall()
    return  a


def  handler_1032(slave_status ,woker_mes):
    mes=slave_status
    master_host=mes[0][1]
    master_port=mes[0][3]
    master_log_file=mes[0][5]
    star_pos=mes[0][21]
    end_pos=mes[0][8]
    print(master_host,master_log_file ,master_port,star_pos, end_pos)
   # 这个地方用于获取replication_applier_status_by_worker 表中的error message  ，通过正则平赔获取相应的  db。table_name
    error_mes= woker_mes[2]
    #  a 通过正则获得 db.table_name  , log_file  , end pos   结果是一个集合
    a=re.search(text ,error_mes)
    db= a[0].split('.')[0]
    table_name=a[0].split('.')[1]
    # 获取主库上binlog的pk 值
    pkk=get_tb_pk(db,table_name)[0][0]
    ppk=get_tb_pk(db,table_name)[0][1]
    do_getlog= get_binlog%(master_host ,master_port ,star_pos ,end_pos , master_log_file ,ppk )
    pk_value = os.popen(do_getlog).readlines()[0].split("=", 2)[1].rstrip()
    # 这条sql 用于插入 从库中冲突的数据   值插入主键即可
    sql = ' insert into  %s ( %s )  values (%s) ' %(table_name , pkk ,pk_value)
    cur.execute("set session sql_log_bin=0;")
    cur.execute(sql)
    cur.execute("start slave sql_thread")


# 这个函数还没有改
def handler_1062(r, rpl):
    print(r['Last_SQL_Error'])
    p = re.compile(r1062)
    m = p.search(r['Last_SQL_Error'])
    db_table = m.group(1)
    pk_v = m.group(2)
    conn = get_conn()
    pk_col = get_tb_pk(db_table)[0]

    sql = "delete from %s where %s=%s" % (db_table, pk_col, pk_v)
    cursor = conn.cursor()
    cursor.execute("set session sql_log_bin=0;")
    cursor.execute(sql)
    # cursor.execute("set  session sql_log_bin=1")
    cursor.execute("start slave sql_thread")
    cursor.close()
    conn.commit()
    conn.close()
    return 0

def close():
    cur.close()
    conn.close()





if __name__ == '__main__':
    a=slave_status()
