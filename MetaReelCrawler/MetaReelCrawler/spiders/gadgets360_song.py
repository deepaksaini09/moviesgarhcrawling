import time

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


class JustWatchMovieSpiderSpider(scrapy.Spider):
    name = 'gadgets360song'

    def __init__(self, Url, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = []
        self.url = Url

    def start_requests(self):
        for url in self.url:
            request = Request(url[1], callback=self.parse)
            request.meta['urlType'] = url[2]
            yield request

    def parse(self, response, **kwargs):
        # print(response.url)
        paginationContentType = response.meta['urlType']
        hxs = Selector(response)
        Flag = False
        albumID = None
        movieName = commonServices.checkIfItemIsListType(hxs.xpath('//h1[@class="_hd1"]/text()').extract(), 0)
        movieTitle = movieName
        year = commonServices.checkIfItemIsListType(hxs.xpath('//span[@class="_liinfo"]/text()').extract(), 0)
        try:
            # print(year)
            year = int((year.strip())[-4:])
        except Exception as error:
            print(error)
            year = None
        if paginationContentType == 'web':
            movieName = movieName.split('Season')[0].strip()
        contentID, contentType = dBServiceObj.checkIfMovieExistOrNot(movieName, year, paginationContentType)
        contentSeasonID = None
        if paginationContentType == 'web' and not contentID:
            return
        if contentType == 'show':
            seasonNumber = hxs.xpath('//div[contains(@class,"_flx _ttlinfo")]/span/text()').extract()
            try:
                for s in seasonNumber:
                    if s.count('Season'):
                        seasonNumber = int(s.split('Season')[1].strip())
                        break
            except Exception as error:
                print(error)
            try:
                if not isinstance(seasonNumber, int):
                    getSeasonNumber = movieTitle.split('Season')
                    if len(getSeasonNumber) == 2:
                        seasonNumber = int(getSeasonNumber[1])
            except Exception as error:
                print(error)
            contentSeasonID = dBServiceObj.getContentSeasonID(contentID, seasonNumber)
        # ------------------------------------------------- Rating ---------------------------------
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

            if ratingSource.count('rottentomatoes'):
                rottenTomatoesRef = ratingSource.split('https://www.rottentomatoes.com/')[1]
                rottenTomatoesRating = rating.replace('%', '')
        if not contentID and imdbRef:
            contentID = dBServiceObj.getContentIDByImdbID(imdbRef)
        if contentID:
            albumID = dBServiceObj.insertRatingOfMovieIntoContentAlbum(contentID, imdbRating, createdAt, updatedAt,
                                                                       rottenTomatoesRating, imdbRef, rottenTomatoesRef,
                                                                       contentSeasonID
                                                                       )

        if contentID and paginationContentType != 'web':
            songExistOrNot = hxs.xpath('//ul[contains(@class,"_flx _fxdiv _mvoptb _pdtbs _stk")]//a/text()').extract()
            if 'Songs' in songExistOrNot:
                checkMusicDirector = hxs.xpath('//div[@class="_mvdtl"]/ul//li')
                createdAt = datetime.now()
                updatedAt = createdAt
                for musicDir in checkMusicDirector:
                    if 'Music' in musicDir.xpath('strong/text()').extract():
                        Flag = True
                        musicDirector = musicDir.xpath('span/text()').extract()
                        if len(musicDirector):
                            musicDirector = musicDirector[0].split(',')
                            for directorName in musicDirector:
                                firstName, lastName = self.commonService.findFirstAndLastName(directorName)
                                dBServiceObj.insertMusicDirData(firstName, lastName, createdAt, updatedAt, contentID)

                if Flag:
                    Flag = False
                    songsData = hxs.xpath('//table[contains(@class ,"table _songtbl __scrldv ")]/tbody//tr')
                    for songs in songsData:
                        createdAt = datetime.now()
                        updatedAt = datetime.now()
                        songTitle = commonServices.checkIfItemIsListType(songs.xpath('td[2]/text()').extract(),
                                                                         0).strip()
                        songArtist = songs.xpath('td[3]/text()').extract()
                        if len(songArtist):
                            songArtist = songArtist[0].split(',')
                        songDuration = commonServices.checkIfItemIsListType(songs.xpath('td[4]/text()').extract(), 0)
                        songDurationInMinute = int(songDuration.split(':')[0])
                        songDurationInSecond = int(songDuration.split(':')[1])
                        listenUrl = None
                        try:
                            listenUrl = commonServices.checkIfItemIsListType(
                                songs.xpath('td[5]/span/@data-url').extract(), 0)
                            listenUrl = commonServices.checkIfItemIsListType(listenUrl, 0).split('redirect=')[1]
                        except Exception as error:
                            print(error)
                            listenUrl = commonServices.checkIfItemIsListType(
                                songs.xpath('td[5]/@data-embed-id').extract(), 0)
                        # if len(listenUrl):
                            # print(listenUrl, songDuration, songArtist, songTitle)
                        dBServiceObj.insertSongsData(contentID, songTitle, songDurationInMinute, songDurationInSecond,
                                                     createdAt, updatedAt, listenUrl, albumID, songArtist)

                    # -------------------------------------------------- songs Provider -------------------------
                    providerList = []
                    songsProvider = hxs.xpath('//div[@class=" _mvsottw"]/ul//li')
                    if len(songsProvider):
                        createdAt = datetime.now()
                        updatedAt = datetime.now()
                        for provider in songsProvider:
                            songsPr = commonServices.checkIfItemIsListType(
                                provider.xpath('span/span/@data-url').extract(), 0)
                            songsPr = commonServices.checkIfItemIsListType(songsPr.split('redirect='), 1)
                            providerList.append(songsPr)
                        dBServiceObj.insertDataIntoSongsAlbum(providerList, albumID, createdAt, updatedAt)

                # ---------------------------------- For Streaming Url Or Ticket Book Url---------------------------
                try:
                    streamingOrBookTicketUrl = commonServices.checkIfItemIsListType(hxs.xpath(
                        '//span[contains(@class,"_mvwtch _flx daffUrl")]/@data-url').extract(), 0)
                    if len(streamingOrBookTicketUrl):
                        streamingOrBookTicketUrl = streamingOrBookTicketUrl.split('redirect=')[1]
                        # print(streamingOrBookTicketUrl)
                        title = commonServices.checkIfItemIsListType(hxs.xpath(
                            '//span[contains(@class,"_mvwtch _flx daffUrl")]/@title').extract(), 0)
                        urlTitle = title
                        createdAt = datetime.now()
                        updatedAt = datetime.now()
                        if len(streamingOrBookTicketUrl):
                            streamingOrBookTicketUrl = streamingOrBookTicketUrl.replace('%3A', ':').replace('%2F', '/')
                            # dBServiceObj.insertStreamingUrl(contentID, streamingOrBookTicketUrl, urlTitle, createdAt,
                            #                                 updatedAt)
                            # print(streamingOrBookTicketUrl)
                        # print(streamingOrBookTicketUrl)
                except Exception as error:
                    print(error)
            else:
                if response.meta.get('urlType') != 'web':
                    # print('not found song')
                    dBServiceObj.insertUrlIfNotHavingSong(response.url, contentID, response.meta.get('urlType'))
        if contentID:
            dBServiceObj.insertContentIDIntoQueue(contentID, 'song')
            time.sleep(2)
            response = requests.get('https://api-metareel.91mobiles.com//updateMusicById/'+str(contentID), timeout=30)
            # print(response.text)


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    args = sys.argv
    songsUrl = None
    releasedType = args[1]
    if len(args) == 3:
        args = args[1]
        # songsUrl = dBServiceObj.fetchGadget360DataNotHavingSongs()
        # print('not found song')
    else:
        songsUrl = dBServiceObj.fetchGadget360Urls(releasedType)
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('gadgets360song', songsUrl)
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now())

