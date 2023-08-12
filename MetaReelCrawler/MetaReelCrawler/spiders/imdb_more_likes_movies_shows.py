from datetime import datetime

from scrapy.selector import Selector
import scrapy
from scrapy.http import Request
from common_services import commonServices
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from db_services import dBServices

dBServiceObj = dBServices()


class imdbMoreLikesMoviesShows(scrapy.Spider):
    name = 'imdb_more_likes_movies_shows'

    def __init__(self, offset, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = []
        self.offset = offset

    def start_requests(self):
        data = dBServiceObj.getJustWatchPopularIMDBID()
        for imdbIDContent in data:
            url = 'https://www.imdb.com/title/' + str(imdbIDContent[0])
            request = Request(url, self.parse)
            request.meta['imdbID'] = imdbIDContent[0]
            request.meta['contentType'] = imdbIDContent[1]
            yield request

    def parse(self, response, **kwargs):
        # print(response.url)
        contentType = response.meta.get('contentType')
        imdbId = response.meta.get('imdbID')
        contentID = dBServiceObj.getContentIDByImdbID(imdbId, contentType)
        hsx = Selector(response)
        htmlExtractData = hsx.xpath('//section[@data-testid="MoreLikeThis"]//div[contains(@class,'
                                    '"ipc-poster-card--base")]')
        contentIDRelatedData = []
        popularity = 1
        for data in htmlExtractData:
            imdbRating = self.commonService.checkIfItemIsListType(data.xpath('div[2]/span[contains(@class,'
                                                                             '"ipc-rating-star ipc-rating-star--base '
                                                                             'ipc-rating-star--imdb")]/text()').extract(),
                                                                  0)

            imdbIDRelated = self.commonService.checkIfItemIsListType(data.xpath('a[contains(@class,'
                                                                                '"ipc-poster-card__title")]/@href').extract(

            ), 0).split('/?')[0].replace('/title/', '').strip()
            if imdbIDRelated and imdbRating:
                relContentID = dBServiceObj.getContentIDByImdbID(imdbIDRelated, contentType)
                if relContentID:
                    contentIDRelatedData.append([relContentID, popularity])
                    popularity += 1
        if contentIDRelatedData:
            # print(contentIDRelatedData)
            dBServiceObj.insertSimilarMoviesShowsToContentSimilar(contentID, contentIDRelatedData, 1)


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    offset = 0
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('imdb_more_likes_movies_shows', offset)
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now())
