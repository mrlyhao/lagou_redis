import json
import redis
from scrapy import settings
import pymysql
import pymysql.cursors
from twisted.enterprise import adbapi

def from_settings(cls,settings):#调用settings值的固定函数，调用方法和字典一样

    r = redis.Redis(host='192.168.1.112', port=6379, db=0)
    # 将参数字典化方便传入
    dbparms = dict(
        host = 'localhost',
        db = 'lyh',
        user = 'root',
        passwd = 'MYSQL_PASSWD',
        charset= 'utf8',
        cursorclass = pymysql.cursors.DictCursor,
        use_unicode = True,

    )
    # from twisted.enterprise import adbapi  Twisted为数据库提供的一个异步化接口。
    dbpool = adbapi.ConnectionPool('pymysql',**dbparms)#第一个是需要的函数名称，后边是不定长的字典参数
    while True:
        # process queue as FIFO, change `blpop` to `brpop` to process as LIFO

        source, data = r.blpop(["jobbole:items"])
        item = json.loads(data)
        query = dbpool.runInteraction(do_insert,item)
        query.addErrback(handle_error)#处理异常
    # 执行具体的插入
    # 根据不同的item 构建不同的sql语句并插入到mysql中
def do_insert(self, cursor, item):
    insert_sql = """
                insert into lagou_job(title, url, url_object_id, salary, job_city, work_years, degree_need,
                job_type, publish_time, job_advantage, job_desc, job_addr, company_name, company_url,
                tags, crawl_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE salary=VALUES(salary), job_desc=VALUES(job_desc)
            """
    params = (
        item["title"], item["url"], item["url_object_id"], item["salary"], item["job_city"],
        item["work_years"], item["degree_need"], item["job_type"],
        item["publish_time"], item["job_advantage"], item["job_desc"],
        item["job_addr"], item["company_name"], item["company_url"],
        item["job_addr"], item["crawl_time"].strftime('%Y-%m-%d %H:%M:%S'),
    )
    cursor.execute(insert_sql, params)

def handle_error(self,failure):
    #处理异步插入异常
    print(failure)


