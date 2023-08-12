import datetime
import math
import sys
from MetaReelCrawler.MetaReelCrawler.items import PaginationUrlForSong
from scrapy.selector import Selector
import scrapy
from scrapy.http import Request
from common_services import commonServices
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

commonServicesObj = commonServices()


class PaginationUrlForSongSpider(scrapy.Spider):
    name = 'pagination_url_for_song'

    def __init__(self, upcomingType=None, **kwargs):
        super().__init__(**kwargs)
        self.dBServiceObj = commonServices()
        self.moviesUrl = []
        self.moviesType = None
        self.comingType = upcomingType

    def start_requests(self):
        if self.comingType == 'upcoming':
            typeMovies = {'tamil': 0, 'telugu': 0, 'bollywood': 0, 'hollywood': 0, 'web-series': 0}
            urlList = 'https://www.gadgets360.com/entertainment/upcoming-{movieType}-movies?page='
            findPageUrl = 'https://www.gadgets360.com/entertainment/upcoming-{movieType}-movies?page=1'
        else:
            typeMovies = {'tamil': 0, 'telugu': 0, 'hindi': 0, 'english': 0, 'web-series': 0}
            urlList = 'https://www.gadgets360.com/entertainment/new-{movieType}-movies?page='
            findPageUrl = 'https://www.gadgets360.com/entertainment/new-{movieType}-movies?page=1'

        for ty in typeMovies:
            typeMovies[ty] = int(math.ceil(int(commonServicesObj.findPaginationNumber(
                findPageUrl.format(
                    movieType=ty), ty)) / 30)) + 2
            self.moviesType = ty.capitalize()
            for j in range(1, typeMovies[ty]):
                if ty == 'web-series':
                    url = (urlList.format(movieType=ty) + str(j)).replace('-movies', '')
                else:
                    url = urlList.format(movieType=ty) + str(j)
                yield Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        hxs = Selector(response)
        # print(hxs.response)
        data = hxs.xpath('//div[@id="all_movies"]/div')
        for i in data:
            items = PaginationUrlForSong()
            items['url'] = self.dBServiceObj.checkIfItemIsListType(i.xpath('div[@class="thumb"]/a/@href').extract(), 0)
            items['url_type'] = response.url.split('-')[1]
            items['released_type'] = self.comingType
            yield items


if __name__ == "__main__":
    print('startTime :', datetime.datetime.now())
    args = sys.argv
    upcoming = None
    if len(args) == 2:
        upcoming = args[1]
        settings = get_project_settings()
        process = CrawlerProcess(settings)
        process.crawl('pagination_url_for_song', upcoming)
        process.start(stop_after_crawl=True)
        print("endTime: ", datetime.datetime.now(), upcoming)
