from bs4 import BeautifulSoup
import requests
import pymysql
import parse
import time
import threading
import re
import traceback
from concurrent.futures import ThreadPoolExecutor


def synchronized(func):
    """
    线程安全的单例模式，线程访问创建类的实例时会拿到锁，就相当于独占该单例类
    其他线程无法创建该单例类的实例，使用完释放锁，其他线程才可以创建单例类的实例
    :param func:
    :return:
    """
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


def Singleton(cls):
    instances = {}
    """
    普通单例装饰器实现
    """
    @synchronized
    def get_instance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return get_instance

@Singleton
class con_sql:
    """
    将组装好的数据放入insertTwoColorBallInfo方法
    然后执行插入数据库动作
    """
    def __init__(self):
        self.config = {
            "host": "*******",
            "user": "*",
            "password": "*",
            "database": "*"
        }
        self.info_temp = list()
        self.sql_temp = ''
        self.__db = pymysql.connect(**self.config)
        self.__cursor = self.__db.cursor()

    def __del__(self):
        self.__cursor.close()
        self.__db.close()

    def insertTwoColorBallInfo(self,data):
        sql = "INSERT INTO runoob_tbl VALUES "
        info = list()
        info.append(data['periods'])
        info.append(data['ball_red'][0])
        info.append(data['ball_red'][1])
        info.append(data['ball_red'][2])
        info.append(data['ball_red'][3])
        info.append(data['ball_red'][4])
        info.append(data['ball_red'][5])
        info.append(data['ball_blue'])
        info.append(int(data['sales']))
        info.append(int(data['price_pool']))
        info.append(data['awarding_date'])
        info.append(data['first_price_price'])
        info.append(data['first_price_count'])
        info.append(data['second_price_price'])
        info.append(data['second_price_count'])
        info.append(data['third_price_price'])
        info.append(data['third_price_count'])
        info.append(data['fourth_price_price'])
        info.append(data['fourth_price_count'])
        info.append(data['fifth_price_price'])
        info.append(data['fifth_price_count'])
        info.append(data['sixth_price_price'])
        info.append(data['sixth_price_count'])
        self.info_temp.append(info)
        print(len(self.info_temp))
        if len(self.info_temp) >=30:
            for i in self.info_temp:
                self.sql_temp += str(tuple(i))+','
            #print(self.sql_temp)
            sql += self.sql_temp.strip(',')
            del self.info_temp[0:31]
            self.sql_temp = ''
            print(sql)
            try:
                self.__cursor.execute(sql)
                self.__db.commit()
            except Exception as e:
                self.__db.rollback()
                print(e)
                raise Exception('数据库保存数据异常')
class req:
    """
    访问需抓取的页面，抓取数据组装好给con_sql类进行写入数据库
    """
    def __init__(self):
        self.__url = "http://kaijiang.500.com/ssq.shtml"
        self.__seq_url = list()

    def get_url(self):
        #data = self.send_request(self.__url)
        data = requests.get(self.__url)
        data = data.text.encode(data.encoding).decode('gbk','ignore')
        soup = BeautifulSoup(data,'html.parser')
        a = soup.find_all(class_='iSelectList')
        for i in a:
            for h in i.find_all('a'):
                url = parse.parse("""<a href="{}">{}</a>""",str(h))
                self.__seq_url.append(url[0])

    def for_mat(self,value):
        try:
            return int(value)
        except ValueError:
            return 0

    def send_request(self,url):
        proxy_host = 'http-dynamic.xiaoxiangdaili.com'
        proxy_port = 10030
        proxy_username = '675906821210656768'
        proxy_pwd = 'UQg0wTC7'

        proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": proxy_host,
            "port": proxy_port,
            "user": proxy_username,
            "pass": proxy_pwd,
        }

        proxies = {
            'http': proxyMeta,
            'https': proxyMeta,
        }
        try:
            res = requests.get(url = url,proxies = proxies)
        except Exception as e:
            print(e)
        return res

    def get_info(self,value):
        #data = self.send_request(value)
        data = requests.get(value)   #18079      21013     11112        20111
        soup = BeautifulSoup(data.text,'html.parser')
        result = dict()
        award_info = []
        td = soup.find_all('tr',{'align':'center'})
        for i in range(2,len(td)):
            temp = ['name','count','price']
            temp1 = [i.get_text().encode(data.encoding).decode('gbk', 'ignore').strip() for i in td[i].find_all('td')]
            award_info.append(dict(zip(temp,temp1)))
        for i in award_info:
            if i['name'] == '一等奖':
                result['first_price_count'] = i['count']
                result['first_price_price'] = i['price'].replace(',','')
            if i['name'] == '二等奖':
                result['second_price_count'] = i['count']
                result['second_price_price'] = i['price'].replace(',','')
            if i['name'] == '三等奖':
                result['third_price_count'] = i['count']
                result['third_price_price'] = i['price'].replace(',','')
            if i['name'] == '四等奖':
                result['fourth_price_count'] = i['count']
                result['fourth_price_price'] = i['price'].replace(',','')
            if i['name'] == '五等奖':
                result['fifth_price_count'] = i['count']
                result['fifth_price_price'] = i['price'].replace(',','')
            if i['name'] == '六等奖':
                result['sixth_price_count'] = i['count']
                result['sixth_price_price'] = i['price'].replace(',','')
        tb = soup.find_all('table')
        try:
            awarding_date = str(tb[1].find_all('span', class_='span_right')[0].get_text()).encode(data.encoding).decode('gbk','ignore')
            awarding_date = re.compile("(?<=开奖日期：).*?(?=兑)").findall(awarding_date)[0].strip()
        except :
            awarding_date = "?"
        sales = str(tb[1].find_all('span', class_='cfont1')[0].get_text()).encode(data.encoding).decode("gbk",'ignore')
        sales = int(re.compile('.*?(?=元)').findall(sales)[0].replace(',',''))
        price_pool = str(tb[1].find_all('span',class_='cfont1')[1].get_text()).encode(data.encoding).decode('gbk','ignore')
        price_pool = int(re.compile('.*?(?=元)').findall(price_pool)[0].replace(',',''))
        periods = tb[1].find_all('strong')[0].get_text()
        ball_red = [i.get_text() for i in tb[2].find_all('li', class_='ball_red')]
        ball_blue = tb[2].find_all('li', class_='ball_blue')[0].get_text()
        result['awarding_date'] = awarding_date
        result['sales'] = sales
        result['price_pool'] = price_pool
        result['periods'] = periods
        result['ball_red'] = ball_red
        result['ball_blue'] = ball_blue
        con_sql().insertTwoColorBallInfo(result)
        time.sleep(2)
        print(result)

    def main(self):
        print(self.__seq_url)
        with ThreadPoolExecutor(max_workers=3)as t:
            for i in self.__seq_url:
                t.submit(self.get_info,i)


if __name__ == '__main__':
    #t1 = time.process_time()
    a= req()
    a.get_url()
    #a.get_info()
    #print(time.process_time()-t1)
    a.main()