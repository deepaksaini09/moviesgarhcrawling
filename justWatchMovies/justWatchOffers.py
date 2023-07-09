import sys
from datetime import datetime
import json
from db_services import dBServices

import requests

dBServicesObj = dBServices()


def getAllSeasonJustWatchID(JustWatchMainID, justWatchURL):
    try:
        print(JustWatchMainID)
        justWatchStandardUrl = justWatchURL.split('https://www.justwatch.com')[1].strip()
        justWatchSeasonMap = {}
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
        }

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
                          availableCountDown(country: $country)
                          availableToDate
                          package {
                            shortName
                            __typename
                          }
                          __typename
                        }
                        fallBackClips: content(country: "US", language: "en") {
                          videobusterClips: clips(providers: [VIDEOBUSTER]) {
                            externalId
                            provider
                            __typename
                          }
                          dailymotionClips: clips(providers: [DAILYMOTION]) {
                            externalId
                            provider
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
                            provider
                            __typename
                          }
                          videobusterClips: clips(providers: [VIDEOBUSTER]) {
                            externalId
                            provider
                            __typename
                          }
                          dailymotionClips: clips(providers: [DAILYMOTION]) {
                            externalId
                            provider
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
                          isReleased
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
                            releaseCountDown(country: $country)
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
                            originalTitle
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
                            isReleased
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
                          fallBackClips: content(country: "US", language: "en") {
                            videobusterClips: clips(providers: [VIDEOBUSTER]) {
                              externalId
                              provider
                              __typename
                            }
                            dailymotionClips: clips(providers: [DAILYMOTION]) {
                              externalId
                              provider
                              __typename
                            }
                            __typename
                          }
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
                            isReleased
                            videobusterClips: clips(providers: [VIDEOBUSTER]) {
                              externalId
                              provider
                              __typename
                            }
                            dailymotionClips: clips(providers: [DAILYMOTION]) {
                              externalId
                              provider
                              __typename
                            }
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
            "fullPath": str(justWatchStandardUrl),
            "language": "en",
            "country": "IN",
            "episodeMaxLimit": 20
        }

        response = requests.post('https://apis.justwatch.com/graphql',
                                 json={"query": query, "variables": variables}, headers=headers
                                 )
        jsonData = json.loads(response.text)
        print(jsonData)
        data = jsonData['data']
        justWatchUrlsData = data['url']['node']['seasons']
        for season in justWatchUrlsData:
            seasonNumber = int(season['content']['seasonNumber'])
            justWatchSeasonMap[seasonNumber] = season['id']
        return justWatchSeasonMap

    except Exception as error:
        print(error)


def getMoviesAndSeasonOffersByJustWatchID(IDS, seasonNumber=None):
    try:
        justWatchMovieID = IDS[0]
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
                "nodeId": justWatchMovieID,
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
                "nodeId": justWatchMovieID,
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
                    offersMap['providerName'] = 'Jio Cinema' if providerName.lower().count('jio') else providerName
                    offersMap['updatedAt'] = updatedAt
                    offersMap['monetizationType'] = offersData['monetizationType']
                    offers.append(offersMap)
                    print(offers)
        if not offers:
            dBServicesObj.insertMoviesNotHavingOffers(IDS[1], IDS[2], seasonNumber)
        dBServicesObj.insertMoviesOffersForJustWatch(offers, IDS[1], IDS[2], seasonNumber)
    except Exception as error:
        print(error)


def findMovieOffers(justWatchMoviesIDs):
    try:
        for IDS in justWatchMoviesIDs:
            justWatchUrl = IDS[3]
            if IDS[2] == 'tv':
                seasonsData = getAllSeasonJustWatchID(IDS[0], justWatchUrl)
                for i in seasonsData:
                    data = [seasonsData[i], IDS[1], IDS[2], IDS[3]]
                    getMoviesAndSeasonOffersByJustWatchID(data, i)

            else:
                getMoviesAndSeasonOffersByJustWatchID(IDS, None)

    except Exception as error:
        print(error)

