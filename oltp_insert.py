import pymysql
from DBUtils.PooledDB import PooledDB
import  time
import  threading

pool = PooledDB(pymysql,maxconnections=50,host='192.168.221.130',user='root',passwd='root.2019',db='axe',port=3306 ,charset="utf8")
conn = pool.connection() #以后每次需要数据库连接就是用connection（）函数获取连接就好了
cur=conn.cursor(pymysql.cursors.DictCursor)
def inser_sql(lis):
    SQL = " insert into meter(name, path_name) values "
    li=list(lis)
    aa=','.join(li)
    for  i in li:
        if i == li[-1]:
            SQL=SQL+i
        else:
            SQL=SQL  + i +','
    print(SQL)

    cur.execute(SQL)
    conn.commit()

    # conn.commit()
def datalist(v):
    a=[]
    for i in range (v):
        data=( i , v)
        a.append(str(data))
    b = [a[i:i + 5000] for i in range(0, len(a), 5000)]
    # print(b)

    return b

def slect_eter( ):
    sql='show slave status ;'
    cur.execute(sql)
    print(cur.fetchall())

def close():
    cur.close()
    conn.close()


def main(num):
    # print('threadd-id:')
    start = time.time()
    a = datalist(num)
    for i in a:
        inser_sql(i)
        # slect_eter()
    time.sleep(2)
    end = time.time()
    use = end - start
    # print('耗时%s' % use)


if __name__ == '__main__':
    print('_主线程开始_' , threading.current_thread().name)
    threa_list=[]
    mutex = threading.Lock()
    t1=threading.Thread(target=main(10000))
    t2=threading.Thread(target=main(20000))
    threa_list.append(t1)
    threa_list.append(t2)
    start=time.time()
    for t in threa_list:
        t.start()
        t.join()
    end=time.time()
    print('主线程结束')
    print('耗时：' ,end-start)


    close()
