# -*- coding: utf-8 -*-
import scrapy
import json
from urllib.parse import urlparse
from urllib.parse import urljoin
from scrapy.http import Request
from datetime import datetime
from env import HOST, DATABASE, USER, PASSWORD
import pymysql

class SuaramuhammadiyahSpider(scrapy.Spider):
    name = 'suaramuhammadiyah'
    #allowed_domains = ['suaramuhammadiyah.id']
    conn = pymysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
    cursor = conn.cursor()
    start_urls = ['http://www.suaramuhammadiyah.id/wp-json/wp/v2/posts?per_page=100&page=1',
                  'http://pwmu.co/wp-json/wp/v2/posts?per_page=100&page=1'
                 ]

    def parse(self, response):
        parsed_url = urlparse(response.url)
        total_post = int(response.headers['X-Wp-Total'].decode('UTF-8'))
        total_page = int(response.headers['X-Wp-Totalpages'].decode('UTF-8'))
        for i in range(1,total_page + 1):
            url = urljoin(response.url,'posts?per_page=100&page={}'.format(i))
            yield scrapy.Request(url, callback = self.parse_page)

    def parse_page(self,response):
        content = json.loads(response.body)
        for article in content:
            item = {
                  'title':article['title']['rendered'],
                  'link':article['link'],
                  'content':article['content']['rendered'],
                  'published_date':datetime.strptime(article['date_gmt'],'%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'),
             }
            self.insert_into_db('news',item)

    def insert_into_db(self, table, item, key=None):
        try:
            placeholder = ', '.join(["%s"] * len(item))
            statement = 'INSERT IGNORE INTO {table} ({columns}) VALUES ({values})'.format(
                table=table, columns=','.join(item.keys()), values=placeholder)
            self.cursor.execute(statement, list(item.values()))
            self.conn.commit()
            print('Item: {} inserted to {}'.format(item, table))
        except Exception as e:
            print('Error {} while inserting {}'.format(e, item))
            return False

