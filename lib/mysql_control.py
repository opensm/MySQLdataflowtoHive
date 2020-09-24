import pymysql

conn = pymysql.connect(host="10.255.50.85", port=3306, user="root", password="123456", db='test')
cursor = conn.cursor()
for i in range(10150, 100000000):
    # sql = "insert into `order` (order_id,amount) values ('%s','%s')" % (i, i/100)
    sql = "insert into tb_emp2(order_id,amount) values ('%s','%s')" % (i, i/100)
    cursor.execute(sql)
    conn.commit()
    print(1111111)
