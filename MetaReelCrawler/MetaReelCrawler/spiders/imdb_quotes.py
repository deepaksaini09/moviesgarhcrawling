import time

import requests
from scrapy.selector import Selector
import scrapy
import sys
import random
from datetime import datetime
from scrapy.http import Request
from common_services import commonServices
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from db_services import dBServices

dBServiceObj = dBServices()


class imdbQuotes(scrapy.Spider):
    name = 'imdb_quotes'

    def __init__(self, offset, lastThreeDaysData, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = []
        self.offset = offset
        self.lastThreeDaysData = lastThreeDaysData

    def start_requests(self):
        while True:
            if self.lastThreeDaysData:
                imdbMovieID = dBServiceObj.getRecentlyReleaseMoviesImdbID(self.offset)
            else:
                imdbMovieID = dBServiceObj.getIMDBMovieID(self.offset)
            self.offset += 100
            user_agents = random.choice(self.commonService.userAgentsList())
            headers = {'USER_AGENT': user_agents}
            for imdbID in imdbMovieID:
                request = Request('https://www.imdb.com/title/{id}/quotes'.format(id=imdbID[0]), callback=self.parse,
                                  headers=headers)
                request.meta['imdbId'] = imdbID[0]
                request.meta['contentID'] = imdbID[1]
                request.headers['header'] = {
                    'User-Agent': user_agents}
                yield request
            if not imdbMovieID:
                break

    def parse(self, response, **kwargs):
        # print(response.url)
        imdbRefID = response.meta['imdbId']
        contentID = response.meta['contentID']
        hxs = Selector(response)
        quotesList = hxs.xpath('//div[@id="quotes_content"]/div[2]/div[contains(@class,"sodavote")]')
        quotesData = []
        for data in quotesList[0:2]:
            triviaOrQuotes = "".join((data.xpath('div[@class="sodatext"]//text()').getall())).strip()
            quotesData.append(triviaOrQuotes.replace(' ', ' ').replace('\n', ' '))
            # print(quotesData)
        if quotesData:
            dBServiceObj.insertIMDBQuotesData(imdbRefID, contentID, quotesData)
        if contentID:
            dBServiceObj.insertContentIDIntoQueue(contentID, 'synopsis')
            time.sleep(2)
            data = requests.get('https://api-metareel.91mobiles.com//updateSynopsisById/' + str(contentID), timeout=30)
            # print(data.text)


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    offset = 0
    args = sys.argv
    lastThreeDaysData = False
    if len(args) >= 2:
        # latestThreeDays quotes
        lastThreeDaysData = True
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('imdb_quotes', offset, lastThreeDaysData)
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now(), lastThreeDaysData)
