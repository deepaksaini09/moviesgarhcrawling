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


class bestSimilarMovieForPaginationSpiderSpider(scrapy.Spider):
    name = 'best_similar'

    def __init__(self, pageNumber, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = []
        self.pageNumber = pageNumber

    def start_requests(self):
        for page in range(1, self.pageNumber + 1):
            request = Request('https://bestsimilar.com/movies?page={pageNumber}'.format(pageNumber=page),
                              callback=self.parse)
            yield request

    def parse(self, response, **kwargs):
        # print(response.url)
        hxs = Selector(response)
        # 'mix-movie-list'
        moviesList = hxs.xpath('//div[@id="mix-movie-list"]//div[@class="name-c"]')
        for i in moviesList:
            try:
                movieName = commonServices.checkIfItemIsListType(i.xpath('a/text()').extract(), 0)
                # print(movieName)
                movieUrl = 'https://bestsimilar.com' + str(
                    commonServices.checkIfItemIsListType(i.xpath('a/@href').extract(), 0))
                if not dBServiceObj.checkIfMovieExistInBestSimilar(movieUrl):
                    movieName, movieYear = self.commonService.getMovieAndYear(movieName)
                    contentID = dBServiceObj.getMovieContentID(movieName, movieYear, 'movie')
                    if contentID:
                        updatedAt = datetime.now()
                        dBServiceObj.insertMovieDataIntoBestSimilar(movieName, movieUrl, contentID, updatedAt)

                        # print('NOT EXIST MOVIE', '*********************************************************')
            except Exception as error:
                print(error)


if __name__ == "__main__":
    # first initialize it to ten after that it count depend on website page it may be increase
    print('startTime: ', datetime.now())
    pageNumber = 10
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('best_similar', pageNumber)
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now())
