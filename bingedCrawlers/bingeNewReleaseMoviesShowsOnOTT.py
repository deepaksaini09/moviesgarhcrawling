import json
import time

import requests
from common_services import commonServices
from db_services import dBServices
from send_mail import sendMailFOrIfMovieNotFound, sendMailForOTTReleaseChangedDate
from datetime import datetime
from urllib.parse import urlparse
dBServiceObj = dBServices()


class bingedMoviesOrShowNewReleased:
    def __init__(self):
        self.commonService = commonServices()
        self.movies = []
        self.body = "Verify Name and add Offers details from binged.com "
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/600.2.5 (KHTML, like Gecko) Version/7.1.2 Safari/537.85.11",
            "Accept": '*/*',
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            'Content-Type': 'application/json; charset=UTF-8',
            'access-control-allow-headers': 'Authorization, X-WP-Nonce, Content-Disposition, Content-MD5, Content-Type'
        }

    def updateOTTReleaseDateOfUpcomingMovies(self):
        try:
            changeOttReleaseMovies = []
            for j in range(1, 3):
                try:
                    data = requests.get(
                        'https://www.binged.com/wp-json/binged-api/v1/movies?page={pageNo}&mode=streaming-soon'.format(
                            pageNo=j),
                        headers=self.headers)
                    print(data)
                    print(data.headers)
                    jsonData = json.loads(data.text)
                    respData = jsonData['data']
                    for k in respData:
                        title = k['title']
                        streamingDate = k['streaming-date']
                        streamingDate = str(datetime.strptime(streamingDate, '%d %b %Y')).replace('00:00:00',
                                                                                                  '').strip()
                        contentUrl = k['link']
                        releaseYear = k['theatrical-year']
                        contentType = 'show' if k['type'] == 'Tv show' else 'movie'
                        seasonNo = None
                        if contentType == 'show':
                            if title.count('Season'):
                                seasonInfo = title.split('Season')
                                title, seasonNo = seasonInfo[0].strip(), seasonInfo[1].strip()
                            else:
                                seasonNo = 1
                        if releaseYear:
                            tmdbRefID, contentID = dBServiceObj.getTmdbRefIDByContentTypeAndTitle(contentType, title,
                                                                                                  releaseYear)
                            print(contentID, '------------------------------------------------------')
                            if tmdbRefID:
                                # newOTTDate, oldTTDate
                                if contentType == 'movie':
                                    updateOTTDate = dBServiceObj.updateOTTReleaseDateByContentID(streamingDate, contentID)
                                    data = requests.get(
                                        'https://api-metareel.91mobiles.com//updateOTTDateById/' + str(contentID),
                                            timeout=30)
                                    dBServiceObj.insertContentIDIntoQueue(contentID, 'ottReleaseDate')
                                    print(data.text)
                                    if updateOTTDate:
                                        updateOTTDate.extend([title, contentType, '' if contentType == 'movie' else seasonNo])
                                        changeOttReleaseMovies.append(updateOTTDate)
                                        print(changeOttReleaseMovies)
                            else:
                                self.movies.append(title + ' , ' + (str(
                                    seasonNo) if contentType == 'show' else 'movie') + ' ,' + str(
                                    releaseYear)+' ,'+str(contentUrl))
                except Exception as error:
                    print(error)
            if changeOttReleaseMovies:
                sendMailForOTTReleaseChangedDate(changeOttReleaseMovies)
            if self.movies:
                sendMailFOrIfMovieNotFound(self.movies, self.body)

        except Exception as error:
            print(error)

    def getAllStreamingPlatForm(self, bingedID):
        try:
            bingeDetailPageApiUrl = 'https://www.binged.com/wp-json/binged-api/v1/movie/' + str(bingedID)
            print(bingeDetailPageApiUrl)
            detailPageData = requests.get(bingeDetailPageApiUrl, headers=self.headers)
            jsonData = json.loads(detailPageData.text)
            respData = jsonData['platform_logos']
            resultData = []
            for i in respData:
                resultData.append(i['ref_url'])
            resultData = set(resultData)
            resultData = list(resultData)
            sourceMap = {}
            for j in resultData:
                try:
                    parseurl = urlparse(j)
                    source = parseurl.netloc
                    if source:
                        providerName = dBServiceObj.getStreamProviderName(source)
                        if providerName not in sourceMap:
                            sourceMap[providerName] = j
                except Exception as error:
                    print('may be not found urlparse module--------------------------------------', error)

            return sourceMap
        except Exception as error:
            print(error)
            return False

    def getNewReleaseContent(self):
        try:
            self.updateOTTReleaseDateOfUpcomingMovies()
        except Exception as error:
            print(error)
        for j in range(1, 6):
            try:
                data = requests.get('https://www.binged.com/wp-json/binged-api/v1/movies?page=' + str(j),
                                    headers=self.headers)
                print(data)
                print(data.headers)
                jsonData = json.loads(data.text)
                respData = jsonData['data']
                for k in respData:
                    offers = []
                    title = k['title']
                    bingedID = k['id']
                    detailPageUrl = k['link']
                    streamingDate = k['streaming-date']
                    streamingDate = str(datetime.strptime(streamingDate, '%d %b %Y')).replace('00:00:00', '').strip()
                    releaseYear = k['theatrical-year']
                    contentType = 'show' if k['type'] == 'Tv show' else 'movie'
                    seasonNo = None
                    if contentType == 'show':
                        if title.count('Season'):
                            seasonInfo = title.split('Season')
                            title, seasonNo = seasonInfo[0].strip(), seasonInfo[1].strip()
                        else:
                            seasonNo = 1
                    sourceMap = self.getAllStreamingPlatForm(bingedID)
                    tmdbRefID, contentID = dBServiceObj.getTmdbRefIDByContentTypeAndTitle(contentType, title,
                                                                                          releaseYear)
                    if tmdbRefID:
                        if contentType == 'movie':
                            dBServiceObj.updateOTTReleaseDateByContentID(streamingDate, contentID)
                            time.sleep(2)
                            data = requests.get(
                                'https://api-metareel.91mobiles.com//updateOTTDateById/' + str(contentID), timeout=30)
                            print(data.text)
                            dBServiceObj.insertContentIDIntoQueue(contentID, 'ottReleaseDate')
                        if sourceMap:
                            for source in sourceMap:
                                offersMap = {'offerType': 'HD', 'availableFor': 'flatrate', 'retailPriceValue': None,
                                             'standardWebURL': sourceMap[source], 'providerName': source,
                                             'updatedAt': datetime.now(), 'monetizationType': 'FLATRATE'}
                                offers.append(offersMap)
                                dBServiceObj.insertOttMoviesAndShows(title, detailPageUrl, releaseYear, None,
                                                                     contentType,
                                                                     contentID, 'binged.com', seasonNo, streamingDate
                                                                     , sourceMap[source], source
                                                                     )
                            dBServiceObj.insertMoviesOffersForJustWatch(offers, tmdbRefID,
                                                                        'tv' if contentType == 'show' else 'movie',
                                                                        seasonNo)

                    else:
                        self.movies.append(title + ' , ' + (str(
                            seasonNo) if contentType == 'show' else 'movie') + ' ,' + str(
                            releaseYear)+' ,'+str(detailPageUrl))
            except Exception as error:
                print(error)
        if self.movies:
            sendMailFOrIfMovieNotFound(self.movies, self.body)


if __name__ == '__main__':
    obj = bingedMoviesOrShowNewReleased()
    obj.getNewReleaseContent()
