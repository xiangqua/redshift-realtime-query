import string
import time
import psycopg2

def sync_data(rs_host,rs_database,rs_user,rs_password,rs_port,curr_time,rs_table_name,pg_table_name):
    conn = psycopg2.connect(host=rs_host,port=rs_port,database=rs_database,user=rs_user,password=rs_password)
    cursor=conn.cursor()
    #sql='select 1'
    sql="insert into " + rs_table_name + " select * from pg." + pg_table_name + " where update_time > (SELECT MAX(update_time) FROM " + rs_table_name + ") and update_time <= " + str(curr_time) + "-120;"
    print(time.ctime() + ": " + sql)
    cursor.execute(sql)
    conn.commit()
    print(time.ctime() + ": " + "sync data sql exec completed")
    cursor.close()
    conn.close()

def delete_data(pg_host,pg_database,pg_user,pg_password,pg_port,curr_time,pg_table_name):
    conn = psycopg2.connect(host=pg_host,port=pg_port,database=pg_database,user=pg_user,password=pg_password)
    cursor=conn.cursor()
    sql="delete from " + pg_table_name + "where update_time < " + str(curr_time) + "-120;"
    print(time.ctime() + ": " + sql)
    cursor.execute(sql)
    conn.commit()
    print(time.ctime() + ": " + "delete data sql exec completed")
    cursor.close()
    conn.close()
    
def delete_table(pg_host,pg_database,pg_user,pg_password,pg_port,curr_time,pg_table_name):
    conn = psycopg2.connect(host=pg_host,port=pg_port,database=pg_database,user=pg_user,password=pg_password)
    cursor=conn.cursor()
    sql1 = "SELECT inhrelid::regclass FROM pg_catalog.pg_inherits WHERE inhparent = \'" + pg_table_name + "\'::regclass;"
    cursor.execute(sql1)
    tables = cursor.fetchall()
    for table in tables:
        if (int(str(table[0])[-10:]) < curr_time-3600):
            sql2 = "drop table "+ table[0]
            print(time.ctime() + ": " + sql2)
            cursor.execute(sql2)
    conn.commit()
    print(time.ctime() + ": " + "drop table sql exec completed")
    cursor.close()
    conn.close()

if __name__=='__main__':    
    pg_host = 'xx'
    pg_user = 'xx'
    pg_database = 'xx'
    pg_password = 'xx'
    pg_port = '5432'
    rs_host = 'xx.ap-southeast-1.redshift.amazonaws.com'
    rs_user = 'xx'
    rs_database = 'xx'
    rs_password = 'xx'
    rs_port = '5439'
    pg_table_name = "realtime_online"
    rs_table_name = "realtime_offline"
    t = time.time() 
    curr_time = int(t)
    try:
        sync_data(rs_host,rs_database,rs_user,rs_password,rs_port,curr_time,rs_table_name,pg_table_name)
        delete_table(pg_host,pg_database,pg_user,pg_password,pg_port,curr_time,pg_table_name)
        #delete_data(pg_host,pg_database,pg_user,pg_password,pg_port,curr_time,pg_table_name)

    except Exception as e:
        print(e)
