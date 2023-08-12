import json
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
from constants import constants

dBServiceObj = dBServices()
constantsObj = constants()


class justWatchMoviesPaginationUrl(scrapy.Spider):
    name = 'just_watch_movies_pagination_url'

    def __init__(self, offset, createdByAcc, updateForSingleID, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = []
        self.offset = offset
        self.createdByAcc = createdByAcc
        self.updateForSingleID = updateForSingleID

    def start_requests(self):
        while True:
            if self.updateForSingleID:
                tmdbData = self.updateForSingleID
                if self.offset < 0:
                    break
                self.offset = -200
            elif self.createdByAcc:
                tmdbData = dBServiceObj.getContentIDByTMDBByCreateAt(self.offset)
            else:
                tmdbData = dBServiceObj.getContentsTMDBID(self.offset)
            self.offset += 100
            if not tmdbData:
                break
            for data in tmdbData:
                try:
                    user_agents = random.choice(self.commonService.userAgentsList())
                    headers = {'USER_AGENT': user_agents}
                    movieType = data[1]
                    movieOrShow = 'movie'
                    if movieType == 'show':
                        movieOrShow = 'tv'
                    if data[0]:
                        tmdbUrlResponse = requests.get(
                            'https://api.themoviedb.org/3/{movieType}/{tmdbID}/watch/providers?api_key'
                            '=a58d307ceb9c21df004500e16beb5cc4'.format(tmdbID=data[0], movieType=movieOrShow),
                            headers=headers)
                        jsonData = json.loads(tmdbUrlResponse.text)
                        tmdbContentUrl = jsonData['results']['IN']['link']
                        # print(tmdbContentUrl)
                        request = Request(tmdbContentUrl, callback=self.parse, dont_filter=True)
                        request.meta['tmdbId'] = data[0]
                        request.meta['movieType'] = movieOrShow
                        request.headers['header'] = {
                            'User-Agent': user_agents}
                        yield request
                except Exception as error:
                    print(error)

    def parse(self, response, **kwargs):
        # print(response.url)
        try:
            tmdbMovieID = response.meta['tmdbId']
            movieType = response.meta['movieType']
            hxs = Selector(response)
            justWatchUrl = hxs.xpath('//a[@title="Visit JustWatch"]/@href').extract()[0]
            response = requests.get(justWatchUrl)
            filterType = '$Movie:'
            if movieType == 'tv':
                filterType = '$Show:'
            justWatchMovieID = (response.text.split(filterType, 1))[1].split('.')[0]
            updatedAt = datetime.now()
            dBServiceObj.insertMovieUrlForPaginationUrlForJustWatch(justWatchUrl, justWatchMovieID, updatedAt,
                                                                    tmdbMovieID, movieType)
        except Exception as error:
            print(error)


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    args = sys.argv
    acc = False
    updateForSingleID = False
    if len(args) >= 2:
        acc = True
    if len(args) == 3:
        updateForSingleID = [[int(sys.argv[2]), sys.argv[1]]]
    offset = 0
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('just_watch_movies_pagination_url', offset, acc, updateForSingleID)
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now(), acc)
