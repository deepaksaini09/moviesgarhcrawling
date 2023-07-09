import sys
from datetime import datetime
import json
from db_services import dBServices

import requests

dBServicesObj = dBServices()


def getMoviesAndSeasonOffersByJustWatchID(IDS, seasonNumber=None):
    try:
        justWatchMovieID = IDS[2]
        offers = []
        tmdbRefID = IDS[4]
        contentType = 'tv' if IDS[0] == 'show' else IDS[0]
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
                    offersMap['providerName'] = providerName
                    offersMap['updatedAt'] = updatedAt
                    offers.append(offersMap)
                    print(offers)
        if not offers:
            dBServicesObj.insertMoviesNotHavingOffers(tmdbRefID, contentType, seasonNumber)
        dBServicesObj.insertMoviesOffersForJustWatch(offers, tmdbRefID, contentType, seasonNumber)
    except Exception as error:
        print(error)


def findMovieOffers():
    try:
        # content_type, content_id, just_watch_id, seasonNumber
        justWatchMoviesIDs = dBServicesObj.getContentOTTMoviesOrShowOfNewReleaseFromJustWatch()
        for IDS in justWatchMoviesIDs:
            getMoviesAndSeasonOffersByJustWatchID(IDS, IDS[3])

    except Exception as error:
        print(error)


findMovieOffers()
