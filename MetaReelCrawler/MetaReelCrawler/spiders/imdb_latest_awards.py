import time

from scrapy.selector import Selector
import scrapy
import sys
from datetime import datetime
from scrapy.http import Request
from common_services import commonServices
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from db_services import dBServices

dBServiceObj = dBServices()


class imdbAwards(scrapy.Spider):
    name = 'imdb_latest_awards'

    def __init__(self, offset, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = []
        self.offset = offset

    def start_requests(self):
        time.sleep(5)
        yield Request('https://www.imdb.com/event/ev0000003/2022/1', self.parse)

    def parse(self, response, **kwargs):
        # print(response.url)
        time.sleep(5)
        hsx = Selector(response)
        # print(hsx.extract())
        data = hsx.xpath('//a[contains(@href,"/name/")]/@href').extract()
        # print(data)


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    offset = 0
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('imdb_latest_awards', offset)
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now())
