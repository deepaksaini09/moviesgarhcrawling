from scrapy.selector import Selector
import scrapy
import sys
from datetime import datetime
from scrapy.http import Request
from common_services import commonServices
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from db_services import dBServices
# from send_mail import sendMailFOrIfMovieNotFound

dBServiceObj = dBServices()


class updateBMSUrl_gadget360(scrapy.Spider):
    name = 'BMS_URLS'

    def __init__(self, Urls, **kwargs):
        super().__init__(**kwargs)
        self.urls = Urls
        self.notFoundMovies = []

    def start_requests(self):
        # print(self.urls)
        for url in self.urls:
            # print(url[1], '888')
            request = Request(url['url'], callback=self.parse)
            request.meta['id'] = url['id']
            yield request
        # sendMailFOrIfMovieNotFound(self.notFoundMovies, None)

    def parse(self, response, **kwargs):
        hxs = Selector(response)
        movieName = commonServices.checkIfItemIsListType(hxs.xpath('//h1[@class="_hd1"]/text()').extract(), 0)
        year = commonServices.checkIfItemIsListType(hxs.xpath('//span[@class="_liinfo"]/text()').extract(), 0)
        # print(year)
        urlID = response.meta['id']
        imdbRating = None
        imdbRef = None
        rottenTomatoesRating = None
        rottenTomatoesRef = None
        createdAt = datetime.now()
        updatedAt = createdAt
        moviesRating = hxs.xpath('//div[contains(@class,"_rtcol _flx daffUrl")]')
        for i in moviesRating:
            ratingSource = commonServices.checkIfItemIsListType(
                i.xpath('@data-redirect').extract(), 0)

            rating = commonServices.checkIfItemIsListType(
                i.xpath('span[3]/b/text()').extract(), 0)

            if ratingSource.count('imdb'):
                imdbRating = rating
                imdbRef = ratingSource.split('https://www.imdb.com/title/')[1].replace('/', '')
                # print(imdbRef)
                if imdbRef.count('?'):
                    imdbRef = imdbRef.split('?')[0]
                    # print(imdbRef)

        try:
            year = int((year.strip())[-4:])
        except Exception as error:
            print(error)
            year = None
        contentID, contentType = dBServiceObj.checkIfMovieExistOrNot(movieName, year)
        if not contentID and imdbRef:
            contentID = dBServiceObj.getContentIDByImdbID(imdbRef)
        if contentID:
            streamingOrBookTicketUrl = None
            streamingOrBookTicketUrl = hxs.xpath(
                '//span[contains(@class,"_mvwtch _flx daffUrl")]/@data-url').extract()
            title = hxs.xpath(
                '//span[contains(@class,"_mvwtch _flx daffUrl")]/@title').extract()
            index = 0
            for url in streamingOrBookTicketUrl:
                if url.count('in.bookmyshow.com') and title[index].count('Ticket Online'):
                    streamingOrBookTicketUrl = url
                    title = title[index]
                    break
                else:
                    streamingOrBookTicketUrl = ''
                    title = ''
                index += 1
            createdAt = datetime.now()
            updatedAt = datetime.now()
            if len(streamingOrBookTicketUrl):
                streamingOrBookTicketUrl = streamingOrBookTicketUrl.split('redirect=')[1]
                # print(streamingOrBookTicketUrl)
                urlTitle = title
                if len(streamingOrBookTicketUrl):
                    streamingOrBookTicketUrl = streamingOrBookTicketUrl.replace('%3A', ':').replace('%2F', '/')
                    dBServiceObj.insertStreamingOrBMSUrl(title, streamingOrBookTicketUrl, updatedAt,
                                                         createdAt, contentID, urlID)
                    # print('update url for this url', streamingOrBookTicketUrl)
            else:
                dBServiceObj.insertStreamingOrBMSUrl(None, None, updatedAt,
                                                     createdAt, contentID, urlID)
                # print('update url for this url', streamingOrBookTicketUrl)

        else:
            self.notFoundMovies.append(movieName)


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    urls = dBServiceObj.fetchGadget360Urls()
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('BMS_URLS', urls)
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now())
