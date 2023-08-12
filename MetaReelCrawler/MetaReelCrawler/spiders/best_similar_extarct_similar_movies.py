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


class bestSimilarMovieForExtractSimilarMovies(scrapy.Spider):
    name = 'best_similar_extract_similar_movies'

    def __init__(self, Url, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = []
        self.Url = Url

    def start_requests(self):
        for url in self.Url:
            request = Request(url[2], callback=self.parse)
            request.meta['contentID'] = url[1]
            yield request

    def parse(self, response, **kwargs):
        # print(response.url)
        movieID = response.meta.get('contentID')
        hxs = Selector(response)
        story = ''.join(hxs.xpath(
            '//div[contains(@class,"item item-big clearfix")]//div[contains(@class,"attr attr-story")]/span[2]/text()').extract()).strip()
        style = ''.join(hxs.xpath(
            '//div[contains(@class,"item item-big clearfix")]//div[contains(@class,"attr attr-tag attr-tag-group-3")]/span[2]/text()').extract()).strip()
        updatedAt = datetime.now()
        dBServiceObj.insertMoviesStoryAndStyle(movieID, story, style, updatedAt)
        similarMovies = hxs.xpath('//div[@id="movie-rel-list-widget"]//div[@class="name-c"]')
        count = 0
        for i in similarMovies:
            movieName = commonServices.checkIfItemIsListType(i.xpath('a/text()').extract(), 0)
            similarity = i.xpath('//span[@class="smt-value"]/text()').extract()
            movieUrl = 'https://bestsimilar.com' + str(
                commonServices.checkIfItemIsListType(i.xpath('a/@href').extract(), 0))
            movieName, movieYear = self.commonService.getMovieAndYear(movieName)
            updatedAt = datetime.now()
            contentID = dBServiceObj.getMovieContentID(movieName, movieYear, 'movie')
            if contentID:
                dBServiceObj.insertMostSimilarMovies(movieID, contentID, updatedAt, similarity, count)
            count += 1


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    bestSimilarMoviesData = dBServiceObj.getBestSimilarMoviesUrl()
    if bestSimilarMoviesData:
        settings = get_project_settings()
        process = CrawlerProcess(settings)
        process.crawl('best_similar_extract_similar_movies', bestSimilarMoviesData)
        print('star process--')
        process.start(stop_after_crawl=True)
        print('endTime: ', datetime.now(), 'jio')
