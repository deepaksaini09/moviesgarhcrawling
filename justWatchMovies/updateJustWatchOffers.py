import sys
import time
from datetime import datetime

import requests

from db_services import dBServices

dBServicesObj = dBServices()


class updateJustWatchOffers:
    def __init__(self, dataUpdateDaily, offset, tmdbAndContentType=None):
        self.dataUpdateDaily = dataUpdateDaily
        self.offset = offset
        self.tmdbAndContentType = tmdbAndContentType

    def updateJustWatchOffersDetails(self):
        try:
            while True:
                print(self.dataUpdateDaily, self.offset)
                offersListData = None
                print(offersListData)
                if self.tmdbAndContentType:
                    if self.offset < 0:
                        return
                    offersListData = dBServicesObj.getOffersListData(None, self.offset, self.tmdbAndContentType)
                elif self.dataUpdateDaily == 'jio':
                    offersListData = dBServicesObj.getOffersListData('jio', self.offset, self.tmdbAndContentType)
                elif self.dataUpdateDaily:
                    offersListData = dBServicesObj.getOffersOfnewReleaseMovieAndShows(self.offset)
                else:
                    offersListData = dBServicesObj.getOffersListData(None, self.offset, self.tmdbAndContentType)
                print(len(offersListData))
                self.offset += 1000
                providerMap = {'Amazon Video': 'Prime Video',
                               'Amazon Prime Video': 'Prime Video',
                               'Google Play Movies': 'Google Play',
                               'Apple TV': 'Apple TV Plus',
                               'Best Offers': 'BP',
                                'Jio Cinema': 'Jio'
                               }
                if offersListData:
                    for i in offersListData:
                        oId = i[0]
                        offer_type = i[1]
                        tmdb_ref_id = i[2]
                        offer_provider_url = i[3]
                        offer_provider_name = i[4].strip()
                        available_for = i[5]
                        offers_price = i[6]
                        # created_at = i[7]
                        # updated_at = i[8]
                        # offer_updated_at = i[9]
                        contentTypeOf = None
                        print(contentTypeOf)
                        contentTypeOf = 'show' if i[10] == 'tv' else 'movie'
                        availableFor = 'stream' if available_for == 'flatrate' else available_for
                        seasonNumber = i[11]
                        offerPrice = offers_price
                        offerProviderName = providerMap[
                            offer_provider_name] if offer_provider_name in providerMap else offer_provider_name
                        offerType = 'BP' if offer_type == 'Best Offers' else offer_type
                        contentData = dBServicesObj.getContentIDByTMDBID(tmdb_ref_id, contentTypeOf)
                        contentID = contentData[0]
                        contentType = 'M' if contentData[1] == 'movie' else 'SE'
                        originalLanguageId = contentData[2]
                        title = contentData[3]
                        vPID = False
                        moniID = False
                        currency = 21
                        status = 'publish'
                        monetizationType = i[12]
                        created_at = datetime.now()
                        updated_at = datetime.now()
                        if contentID:
                            seasonEpisodeData = [(1, 1)]
                            if contentType == 'SE':
                                seasonEpisodeData = dBServicesObj.getContentCountSeasonAndEpisode(contentID, seasonNumber)
                                if not seasonEpisodeData:
                                    continue
                            streamID = dBServicesObj.getStreamProviderID(offerProviderName)
                            if streamID:
                                if offerType:
                                    vPID = dBServicesObj.getOffersID(offerType)
                                if availableFor:
                                    try:
                                        moniID = dBServicesObj.getMoniID('Ads' if monetizationType.lower() == 'ads' else availableFor)
                                    except Exception as error:
                                        moniID = dBServicesObj.getMoniID(available_for)
                                        print(error)

                                if vPID and moniID:
                                    try:
                                        contentType1 = 'E' if contentType == 'SE' else contentType
                                        for j in seasonEpisodeData:
                                            seasonHavingTotalEpisode = j[1] if contentType == 'SE' else None
                                            seasonNumber = j[0] if contentType == 'SE' else None
                                            if contentType == 'SE':
                                                dBServicesObj.insertOffersDataForContentOffers(
                                                    title,
                                                    contentID,
                                                    'SE',
                                                    streamID,
                                                    moniID,
                                                    vPID,
                                                    originalLanguageId,
                                                    offerPrice,
                                                    currency,
                                                    offer_provider_url,
                                                    status,
                                                    seasonNumber,
                                                    seasonHavingTotalEpisode,
                                                    None,
                                                    created_at,
                                                    updated_at
                                                )
                                            for k in range(1, j[1] + 1):
                                                episodeNumber = k if contentType == 'SE' else None
                                                dBServicesObj.insertOffersDataForContentOffers(title,
                                                                                               contentID,
                                                                                               contentType1,
                                                                                               streamID,
                                                                                               moniID,
                                                                                               vPID,
                                                                                               originalLanguageId,
                                                                                               offerPrice,
                                                                                               currency,
                                                                                               offer_provider_url,
                                                                                               status,
                                                                                               seasonNumber,
                                                                                               seasonHavingTotalEpisode,
                                                                                               episodeNumber,
                                                                                               created_at,
                                                                                               updated_at
                                                                                               )

                                        dBServicesObj.updateJustWatchCrawlersOffers(oId, updated_at)
                                        time.sleep(5)
                                        dBServicesObj.insertContentIDIntoQueue(contentID, 'offer')
                                        time.sleep(2)
                                        data = requests.get(
                                            'https://api-metareel.91mobiles.com//updateOffersById/' + str(contentID),
                                            timeout=30)
                                        print(data.text)
                                    except Exception as error:
                                        print(error)
                else:
                    break

        except Exception as error:
            print(error)


