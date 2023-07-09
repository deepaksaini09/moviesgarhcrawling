import json
import random
import requests
from lxml import html
from common_services import commonServices
from db_services import dBServices
from send_mail import sendMailFOrIfMovieNotFound
from datetime import date
dBServiceObj = dBServices()


class latestShowAndMovies:
    def __init__(self):
        self.url = None
        self.movies = []
        self.body = None
        self.commonService = commonServices()

    def getReleaseYearByJustWatchDetailPage(self, justWatchDetailPageUrl):
        try:
            user_agents = random.choice(self.commonService.userAgentsList())
            headers = {'USER_AGENT': user_agents}
            responseData = requests.get(justWatchDetailPageUrl, headers=headers)
            htmlData = responseData.text
            hsx = html.fromstring(htmlData)
            return hsx.xpath('//span[@class="text-muted"]/text()')

        except Exception as error:
            print(error)

    def getlistPageDataOfLatestShowAndMovies(self):
        try:
            self.body = "Add offers URL from Justwatch"
            dateRange = date.today()
            print(dateRange)

            providerName = ['nfx', 'hst', 'prv', 'zee', 'snl']
            providerNameSource = {'nfx': 'netflix', 'hst': 'hotstar', 'prv': 'prime video', 'zee': 'zee5',
                                  'snl': 'sony liv'
                                  }
            for pvdName in providerName:
                try:
                    query = """ 
                            query GetNewTitles($country: Country!, $date: Date!, $language: Language!, $filter: TitleFilter, $after: String, $first: Int! = 10, $profile: PosterProfile, $format: ImageFormat, $priceDrops: Boolean!, $platform: Platform!, $bucketType: NewDateRangeBucket, $pageType: NewPageType! = NEW, $showDateBadge: Boolean!, $availableToPackages: [String!]) {
                                  newTitles(
                                    country: $country
                                    date: $date
                                    filter: $filter
                                    after: $after
                                    first: $first
                                    priceDrops: $priceDrops
                                    bucketType: $bucketType
                                    pageType: $pageType
                                  ) {
                                    totalCount
                                    edges {
                                      ...NewTitleGraphql
                                      __typename
                                    }
                                    pageInfo {
                                      endCursor
                                      hasPreviousPage
                                      hasNextPage
                                      __typename
                                    }
                                    __typename
                                  }
                                }
                                
                                fragment NewTitleGraphql on NewTitlesEdge {
                                  cursor
                                  newOffer(platform: $platform) {
                                    id
                                    standardWebURL
                                    package {
                                      packageId
                                      clearName
                                      shortName
                                      __typename
                                    }
                                    retailPrice(language: $language)
                                    retailPriceValue
                                    lastChangeRetailPrice(language: $language)
                                    lastChangeRetailPriceValue
                                    lastChangePercent
                                    currency
                                    presentationType
                                    monetizationType
                                    newElementCount
                                    __typename
                                  }
                                  node {
                                    id
                                    objectId
                                    objectType
                                    content(country: $country, language: $language) {
                                      title
                                      fullPath
                                      scoring {
                                        imdbScore
                                        __typename
                                      }
                                      posterUrl(profile: $profile, format: $format)
                                      ... on SeasonContent {
                                        seasonNumber
                                        __typename
                                      }
                                      upcomingReleases @include(if: $showDateBadge) {
                                        releaseDate
                                        package {
                                          shortName
                                          __typename
                                        }
                                        releaseCountDown(country: $country)
                                        __typename
                                      }
                                      isReleased
                                      __typename
                                    }
                                    availableTo(
                                      country: $country
                                      platform: $platform
                                      packages: $availableToPackages
                                    ) @include(if: $showDateBadge) {
                                      availableCountDown(country: $country)
                                      package {
                                        shortName
                                        __typename
                                      }
                                      availableToDate
                                      __typename
                                    }
                                    ... on Movie {
                                      likelistEntry {
                                        createdAt
                                        __typename
                                      }
                                      dislikelistEntry {
                                        createdAt
                                        __typename
                                      }
                                      seenlistEntry {
                                        createdAt
                                        __typename
                                      }
                                      watchlistEntry {
                                        createdAt
                                        __typename
                                      }
                                      __typename
                                    }
                                    ... on Season {
                                      show {
                                        id
                                        objectId
                                        objectType
                                        content(country: $country, language: $language) {
                                          title
                                          __typename
                                        }
                                        likelistEntry {
                                          createdAt
                                          __typename
                                        }
                                        dislikelistEntry {
                                          createdAt
                                          __typename
                                        }
                                        watchlistEntry {
                                          createdAt
                                          __typename
                                        }
                                        seenState(country: $country) {
                                          progress
                                          __typename
                                        }
                                        __typename
                                      }
                                      __typename
                                    }
                                    __typename
                                  }
                                  __typename
                                }
        
                                    """
                    variables = {
                        "first": 100,
                        "pageType": "NEW",
                        "date": str(dateRange),
                        "filter": {
                            "ageCertifications": [],
                            "excludeGenres": [],
                            "excludeProductionCountries": [],
                            "genres": [],
                            "objectTypes": [],
                            "productionCountries": [],
                            "packages": [
                                pvdName
                            ],
                            "excludeIrrelevantTitles": False,
                            "presentationTypes": [],
                            "monetizationTypes": []
                        },
                        "language": "en",
                        "country": "IN",
                        "priceDrops": False,
                        "platform": "WEB",
                        "showDateBadge": False,
                        "availableToPackages": [
                            pvdName
                        ]
                    }
                    print(pvdName)
                    response = requests.post(
                        'https://apis.justwatch.com/graphql?operationName=GetUrlTitleDetails',
                        json={"query": query, "variables": variables}
                        )
                    jsonData = json.loads(response.text)
                    contentData = jsonData['data']['newTitles']['edges']
                    for data in contentData:
                        contentType = data['node']['objectType']
                        movieOrShowName = None
                        seasonNumber = None
                        print(movieOrShowName)
                        justWatchUrlForMovieOrShow = None
                        justWatchID = data['node']['id']
                        justWatchUrl = 'https://www.justwatch.com' + str(data['node']['content']['fullPath'])
                        providerUrl = data['newOffer']['standardWebURL']
                        imdbScore = data['node']['content']['scoring']['imdbScore']
                        if contentType == 'MOVIE':
                            movieOrShowName = data['node']['content']['title']
                            contentType = 'movie'
                        else:
                            movieOrShowName = data['node']['show']['content']['title']
                            seasonNumber = data['node']['content']['seasonNumber']
                            contentType = 'web'
                        if contentType == 'web':
                            justWatchUrlForMovieOrShow = justWatchUrl.split('/season')[0]
                        else:
                            justWatchUrlForMovieOrShow = justWatchUrl
                        releaseYear = self.getReleaseYearByJustWatchDetailPage(justWatchUrlForMovieOrShow)
                        if releaseYear:
                            releaseYear = self.commonService.checkIfItemIsListType(releaseYear, 0)
                            releaseYear = releaseYear.strip().replace('(', '').replace(')', '')
                        source = providerNameSource[pvdName]
                        contentID, contentType1 = dBServiceObj.checkIfMovieExistOrNot(movieOrShowName,
                                                                                      releaseYear,
                                                                                      contentType)
                        print(source, movieOrShowName, 'found ------------------------------------')
                        tmdbRefID = dBServiceObj.getTmdbRefIDByContentID(contentID)
                        if contentID and tmdbRefID:
                            dBServiceObj.insertIntoJustWatchComingTodayForOTT(movieOrShowName, justWatchID,
                                                                              justWatchUrl, providerUrl,
                                                                              imdbScore, seasonNumber,
                                                                              releaseYear,
                                                                              contentType1, source, contentID
                                                                              , tmdbRefID, dateRange
                                                                              )
                        else:
                            print('miss----', source, movieOrShowName)
                            self.movies.append(movieOrShowName + ' , ' + (str(
                                seasonNumber) if contentType == 'web' else 'movie') + ' ,' + str(
                                releaseYear)+' ,'+str(justWatchUrl))
                except Exception as error:
                    print(error)
            if self.movies:
                sendMailFOrIfMovieNotFound(self.movies, self.body)

        except Exception as error:
            print(error)


obj = latestShowAndMovies()
obj.getlistPageDataOfLatestShowAndMovies()
