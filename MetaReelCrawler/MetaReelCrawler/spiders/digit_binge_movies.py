import random

import requests
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


class digitBingeDevotionalAndSportsMovies(scrapy.Spider):
    name = 'digit_binge_movies'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()

    def start_requests(self):
        user_agents = random.choice(self.commonService.userAgentsList())
        headers = {'USER_AGENT': user_agents}
        # pages = self.commonService.getDigitPages(
        #     'https://www.digitbinge.in/movies/page1/?contentTypes=%5B%22movies%22%5D&genres=%5B%22devotional%22%5D',
        #     headers)
        digitBingeMovies = {
            'devotional': "https://www.digitbinge.in/movies/page{pageNo}/?contentTypes=%5B%22movies%22%5D&genres=%5B%22devotional%22%5D",
            'sports': 'https://www.digitbinge.in/movies/page{pageNo}/?contentTypes=%5B%22movies%22%5D&genres=%5B%22sports%22%5D'
            }
        for j in digitBingeMovies:
            pages = self.commonService.getDigitPages(digitBingeMovies[j].format(pageNo=1), headers)
            for i in range(1, pages + 1):
                request = Request(digitBingeMovies[j].format(pageNo=i), callback=self.parse, headers=headers)
                request.headers['header'] = {
                    'User-Agent': user_agents}
                request.meta['genre'] = j
                yield request

    def parse(self, response, **kwargs):
        # print(response.url)
        hsx = Selector(response)
        results = hsx.xpath('//div[@class="results_items"]')
        genreType = response.meta['genre']
        try:
            for i in results:
                try:
                    movieName = commonServices.checkIfItemIsListType(
                        i.xpath('a[@class="digit-binge-gasend"]/span[2]/text()').extract(), 0)
                    movieType = ' '.join(i.xpath('a[@class="digit-binge-gasend"]/p[1]/strong/text()').extract())
                    releaseYear = ''.join(i.xpath('a[@class="digit-binge-gasend"]/p[1]/text()').extract()).strip()
                    whereToWatch = ' '.join(i.xpath('a[@class="digit-binge-gasend"]/p[2]/text()').extract())
                    if releaseYear:
                        releaseYear = releaseYear.split('|')[1].strip()
                    dBServiceObj.insertDigitBingeMovies(movieName, movieType, releaseYear, whereToWatch, genreType)
                except Exception as error:
                    print(error)
        except Exception as error:
            print(error)


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('digit_binge_movies')
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now())
