import json
from db_services import dBServices
import requests
import random
from common_services import commonServices
from datetime import datetime


class justNowMovies:
    def __init__(self, startCursor, movieType, popularCount):
        self.fullpath = None
        self.startCursor = startCursor
        self.count = None
        self.dBServiceObj = dBServices()
        self.movieType = movieType
        self.commonServiceObj = commonServices()
        self.justWatchMovieID = None
        self.popularCounting = popularCount

    def findListPageGenreDetails(self, count):
        try:
            self.count = count
            query = """
                        query GetPopularTitles($country: Country!, $popularTitlesFilter: TitleFilter, $watchNowFilter: WatchNowOfferFilter!, $popularAfterCursor: String, $popularTitlesSortBy: PopularTitlesSorting! = POPULAR, $first: Int! = 40, $language: Language!, $platform: Platform! = WEB, $sortRandomSeed: Int! = 0, $profile: PosterProfile, $backdropProfile: BackdropProfile, $format: ImageFormat) {
                          popularTitles(
                            country: $country
                            filter: $popularTitlesFilter
                            after: $popularAfterCursor
                            sortBy: $popularTitlesSortBy
                            first: $first
                            sortRandomSeed: $sortRandomSeed
                          ) {
                            totalCount
                            pageInfo {
                              startCursor
                              endCursor
                              hasPreviousPage
                              hasNextPage
                              __typename
                            }
                            edges {
                              ...PopularTitleGraphql
                              __typename
                            }
                            __typename
                          }
                         }
    
                          fragment PopularTitleGraphql on PopularTitlesEdge {
                          cursor
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
                              ... on ShowContent {
                                backdrops(profile: $backdropProfile, format: $format) {
                                  backdropUrl
                                  __typename
                                }
                                __typename
                              }
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
                            watchNowOffer(country: $country, platform: $platform, filter: $watchNowFilter) {
                              id
                              standardWebURL
                              package {
                                packageId
                                clearName
                                __typename
                              }
                              retailPrice(language: $language)
                              retailPriceValue
                              lastChangeRetailPriceValue
                              currency
                              presentationType
                              monetizationType
                              availableTo
                              __typename
                            }
                            ... on Movie {
                              seenlistEntry {
                                createdAt
                                __typename
                              }
                              __typename
                            }
                            ... on Show {
                              seenState(country: $country) {
                                seenEpisodeCount
                                progress
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
                "popularTitlesSortBy": "POPULAR",
                "first": 1,
                "platform": "WEB",
                "sortRandomSeed": 0,
                "popularAfterCursor": self.startCursor,
                "popularTitlesFilter": {
                    "ageCertifications": [],
                    "excludeGenres": [],
                    "excludeProductionCountries": [],
                    "genres": [],
                    "objectTypes": [self.movieType],
                    "productionCountries": [],
                    "packages": [],
                    "excludeIrrelevantTitles": False,
                    "presentationTypes": [],
                    "monetizationTypes": []
                },
                "watchNowFilter": {
                    "packages": [],
                    "monetizationTypes": []
                },
                "language": "en",
                "country": "IN"
            }

            response = requests.post('https://apis.justwatch.com/graphql?operationName=GetPopularTitles',
                                     json={"query": query, "variables": variables}
                                     )
            print(json.loads(response.text)['data'])
            popularTitles = json.loads(response.text)['data']['popularTitles']
            data = popularTitles['edges'][0]
            fullpath = data['node']['content']['fullPath']
            movieTitle = data['node']['content']['title']
            justWatchMovieID = data['node']['id']
            self.startCursor = popularTitles['pageInfo']['startCursor']
            print(movieTitle, fullpath)
            imdbID, imdbVotes, imdbRating = self.findDetailsPageInfo(fullpath)
            print(fullpath, movieTitle, self.startCursor, imdbID, imdbVotes, imdbRating, self.count)
            imdbIDUrl = 'https://www.imdb.com/title/' + str(imdbID)
            print(imdbIDUrl)
            try:
                if imdbID:
                    user_agents = random.choice(self.commonServiceObj.userAgentsList())
                    headers = {'USER_AGENT': user_agents}
                    data = requests.get(imdbIDUrl, headers=headers)
                    imdbIDUrl = data.url
                    imdbID = imdbIDUrl.split('https://www.imdb.com/title/')[1].replace('/', '')
            except Exception as error:
                print(error)
            justWatchUrl = 'https://www.justwatch.com' + str(fullpath)
            # offers = self.findMovieOffers(justWatchMovieID)
            self.popularCounting = self.dBServiceObj.insertJustWatchGenreData(self.popularCounting, str(movieTitle),
                                                                              str(imdbIDUrl), str(justWatchUrl),
                                                                              str(imdbID), imdbVotes, imdbRating,
                                                                              self.movieType)
            return self.startCursor, self.popularCounting
        except Exception as error:
            print('findListPageGenreDetails: error occurred', error)

    def findMovieOffers(self, justWatchMovieID):
        self.justWatchMovieID = justWatchMovieID
        try:
            offers = []
            typesMoviesOffers = {'Best Offers': 'SD', 'HD': 'HD', '4K': '_4K', "SD": 'SD'}
            for i in typesMoviesOffers:
                offerType = i
                query = """
                                                 query GetTitleOffers($nodeId: ID!, $country: Country!, $language: Language!, $filterFlatrate: OfferFilter!, $filterBuy: OfferFilter!, $filterRent: OfferFilter!, $filterFree: OfferFilter!, $platform: Platform! = WEB) {
                                  node(id: $nodeId) {
                                    ... on MovieOrShowOrSeasonOrEpisode {
                                      offerCount(country: $country, platform: $platform)
                                      flatrate: offers(
                                        country: $country
                                        platform: $platform
                                        filter: $filterFlatrate
                                      ) {
                                        ...TitleOffer
                                        __typename
                                      }
                                      buy: offers(country: $country, platform: $platform, filter: $filterBuy) {
                                        ...TitleOffer
                                        __typename
                                      }
                                      rent: offers(country: $country, platform: $platform, filter: $filterRent) {
                                        ...TitleOffer
                                        __typename
                                      }
                                      free: offers(country: $country, platform: $platform, filter: $filterFree) {
                                        ...TitleOffer
                                        __typename
                                      }
                                      __typename
                                    }
                                    __typename
                                  }
                                }
                                
                                fragment TitleOffer on Offer {
                                  id
                                  presentationType
                                  monetizationType
                                  retailPrice(language: $language)
                                  retailPriceValue
                                  currency
                                  lastChangeRetailPriceValue
                                  type
                                  package {
                                    packageId
                                    clearName
                                    __typename
                                  }
                                  standardWebURL
                                  elementCount
                                  availableTo
                                  deeplinkRoku: deeplinkURL(platform: ROKU_OS)
                                  __typename
                                }
    
                        """
                variables1 = {
                    "platform": "WEB",
                    "nodeId": self.justWatchMovieID,
                    "country": "IN",
                    "language": "en",
                    "filterBuy": {
                        "monetizationTypes": [
                            "BUY"
                        ],
                        "bestOnly": True,
                        "presentationTypes": [
                            typesMoviesOffers[i]
                        ]
                    },
                    "filterFlatrate": {
                        "monetizationTypes": [
                            "FLATRATE",
                            "FLATRATE_AND_BUY",
                            "ADS",
                            "FREE"
                        ],
                        "presentationTypes": [
                            typesMoviesOffers[i]
                        ],
                        "bestOnly": True
                    },
                    "filterRent": {
                        "monetizationTypes": [
                            "RENT"
                        ],
                        "presentationTypes": [
                            typesMoviesOffers[i]
                        ],
                        "bestOnly": True
                    },
                    "filterFree": {
                        "monetizationTypes": [
                            "ADS",
                            "FREE"
                        ],
                        "presentationTypes": [
                            typesMoviesOffers[i]
                        ],
                        "bestOnly": True
                    }
                }
                variables2 = {
                    "platform": "WEB",
                    "nodeId": self.justWatchMovieID,
                    "country": "IN",
                    "language": "en",
                    "filterBuy": {
                        "monetizationTypes": [
                            "BUY"
                        ],
                        "bestOnly": True

                    },
                    "filterFlatrate": {
                        "monetizationTypes": [
                            "FLATRATE",
                            "FLATRATE_AND_BUY",
                            "ADS",
                            "FREE"
                        ],
                        "bestOnly": True
                    },
                    "filterRent": {
                        "monetizationTypes": [
                            "RENT"
                        ],
                        "bestOnly": True
                    },
                    "filterFree": {
                        "monetizationTypes": [
                            "ADS",
                            "FREE"
                        ],
                        "bestOnly": True
                    }
                }
                if i == 'Best Offers':
                    variables = variables2
                else:
                    variables = variables1
                response = requests.post('https://apis.justwatch.com/graphql?operationName=GetUrlTitleDetails',
                                         json={"query": query, "variables": variables}
                                         )
                jsonData = json.loads(response.text)
                print(jsonData)
                offersOption = ['flatrate', 'buy', 'rent', 'free']
                buyData = jsonData['data']['node']
                updatedAt = datetime.now()
                for availableFor in offersOption:
                    for offersData in buyData[availableFor]:
                        offersMap = {}
                        retailPriceValue = offersData['retailPriceValue']
                        standardWebURL = offersData['standardWebURL']
                        providerName = offersData['package']['clearName']
                        offersMap['offerType'] = offerType
                        offersMap['availableFor'] = availableFor
                        offersMap['retailPriceValue'] = retailPriceValue
                        offersMap['standardWebURL'] = standardWebURL
                        offersMap['providerName'] = providerName
                        offersMap['updatedAt'] = updatedAt
                        offers.append(offersMap)
                        print(offers)
            return offers
        except Exception as error:
            print(error)

    def findDetailsPageInfo(self, fullpath):
        try:
            self.fullpath = fullpath
            query = """
                     query GetUrlTitleDetails($fullPath: String!, $country: Country!, $language: Language!, $episodeMaxLimit: Int, $platform: Platform! = WEB) {
                      url(fullPath: $fullPath) {
                        id
                        metaDescription
                        metaKeywords
                        metaRobots
                        metaTitle
                        heading1
                        heading2
                        htmlContent
                        node {
                          id
                          ... on MovieOrShowOrSeason {
                            objectType
                            objectId
                            offerCount(country: $country, platform: $platform)
                            offers(country: $country, platform: $platform) {
                              monetizationType
                              package {
                                packageId
                                __typename
                              }
                              __typename
                            }
                            promotedBundles(country: $country, platform: $platform) {
                              promotionUrl
                              __typename
                            }
                            availableTo(country: $country, platform: $platform) {
                              availableToDate
                              package {
                                shortName
                                __typename
                              }
                              __typename
                            }
                            content(country: $country, language: $language) {
                              backdrops {
                                backdropUrl
                                __typename
                              }
                              clips {
                                externalId
                                __typename
                              }
                              videobusterClips: clips(providers: [VIDEOBUSTER]) {
                                externalId
                                __typename
                              }
                              externalIds {
                                imdbId
                                __typename
                              }
                              fullPath
                              genres {
                                shortName
                                __typename
                              }
                              posterUrl
                              runtime
                              scoring {
                                imdbScore
                                imdbVotes
                                tmdbPopularity
                                tmdbScore
                                __typename
                              }
                              shortDescription
                              title
                              originalReleaseYear
                              originalReleaseDate
                              upcomingReleases(releaseTypes: DIGITAL) {
                                releaseDate
                                label
                                package {
                                  packageId
                                  shortName
                                  __typename
                                }
                                __typename
                              }
                              ... on MovieOrShowContent {
                                ageCertification
                                credits {
                                  role
                                  name
                                  characterName
                                  personId
                                  __typename
                                }
                                productionCountries
                                __typename
                              }
                              ... on SeasonContent {
                                seasonNumber
                                __typename
                              }
                              __typename
                            }
                            __typename
                          }
                          ... on MovieOrShow {
                            watchlistEntry {
                              createdAt
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
                            __typename
                          }
                          ... on Movie {
                            permanentAudiences
                            seenlistEntry {
                              createdAt
                              __typename
                            }
                            __typename
                          }
                          ... on Show {
                            permanentAudiences
                            totalSeasonCount
                            seenState(country: $country) {
                              progress
                              seenEpisodeCount
                              __typename
                            }
                            seasons(sortDirection: DESC) {
                              id
                              objectId
                              objectType
                              availableTo(country: $country, platform: $platform) {
                                availableToDate
                                package {
                                  shortName
                                  __typename
                                }
                                __typename
                              }
                              content(country: $country, language: $language) {
                                posterUrl
                                seasonNumber
                                fullPath
                                upcomingReleases(releaseTypes: DIGITAL) {
                                  releaseDate
                                  package {
                                    shortName
                                    __typename
                                  }
                                  __typename
                                }
                                __typename
                              }
                              show {
                                id
                                objectId
                                objectType
                                watchlistEntry {
                                  createdAt
                                  __typename
                                }
                                content(country: $country, language: $language) {
                                  title
                                  __typename
                                }
                                __typename
                              }
                              __typename
                            }
                            recentEpisodes: episodes(
                              sortDirection: DESC
                              limit: 3
                              releasedInCountry: $country
                            ) {
                              id
                              objectId
                              content(country: $country, language: $language) {
                                title
                                shortDescription
                                episodeNumber
                                seasonNumber
                                upcomingReleases {
                                  releaseDate
                                  label
                                  __typename
                                }
                                __typename
                              }
                              seenlistEntry {
                                createdAt
                                __typename
                              }
                              __typename
                            }
                            __typename
                          }
                          ... on Season {
                            totalEpisodeCount
                            episodes(limit: $episodeMaxLimit) {
                              id
                              objectType
                              objectId
                              seenlistEntry {
                                createdAt
                                __typename
                              }
                              content(country: $country, language: $language) {
                                title
                                shortDescription
                                episodeNumber
                                seasonNumber
                                upcomingReleases(releaseTypes: DIGITAL) {
                                  releaseDate
                                  label
                                  package {
                                    packageId
                                    __typename
                                  }
                                  __typename
                                }
                                __typename
                              }
                              __typename
                            }
                            show {
                              id
                              objectId
                              objectType
                              totalSeasonCount
                              content(country: $country, language: $language) {
                                title
                                ageCertification
                                fullPath
                                credits {
                                  role
                                  name
                                  characterName
                                  personId
                                  __typename
                                }
                                productionCountries
                                externalIds {
                                  imdbId
                                  __typename
                                }
                                upcomingReleases(releaseTypes: DIGITAL) {
                                  releaseDate
                                  __typename
                                }
                                backdrops {
                                  backdropUrl
                                  __typename
                                }
                                posterUrl
                                __typename
                              }
                              seenState(country: $country) {
                                progress
                                __typename
                              }
                              watchlistEntry {
                                createdAt
                                __typename
                              }
                              dislikelistEntry {
                                createdAt
                                __typename
                              }
                              likelistEntry {
                                createdAt
                                __typename
                              }
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
                    }     
                 """
            variables = {
                "platform": "WEB",
                "fullPath": self.fullpath,
                "language": "en",
                "country": "IN",
                "episodeMaxLimit": 20
            }

            response = requests.post('https://apis.justwatch.com/graphql?operationName=GetUrlTitleDetails',
                                     json={"query": query, "variables": variables}
                                     )
            jsonData = json.loads(response.text)
            print(jsonData)
            content = jsonData['data']['url']['node']['content']
            imdbRating = content['scoring']['imdbScore']
            imdbVotes = content['scoring']['imdbVotes']
            imdbID = content['externalIds']['imdbId']
            return imdbID, imdbVotes, imdbRating
        except Exception as error:
            print('findDetailsPageInfo: error occurred ', error)
