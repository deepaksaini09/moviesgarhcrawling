import os
import sys

from db_services import dBServices
from justWatchMovies.justWatchOffers import findMovieOffers
from MetaReelCrawler.MetaReelCrawler.spiders import just_watch_movies_pagination_url
import subprocess
from constants import constants
from justWatchMovies.updateJustWatchOffers import updateJustWatchOffers

constantsObj = constants()
dBServicesObj = dBServices()
currentPath = os.getcwd()
if __name__ == '__main__':
    contentID = sys.argv[1]
    tmdbID, contentType = dBServicesObj.getContentTypeAndTmdbID(contentID)
    justWatchMoviesOffersDetails = dBServicesObj.checkIfOffersUlrExistOrNot(tmdbID,
                                                                            'tv' if contentType == 'show' else contentType)
    if justWatchMoviesOffersDetails:
        findMovieOffers(justWatchMoviesOffersDetails)
        updateJustWatchOffersObj = updateJustWatchOffers('updateOffersByID', 0,
                                                         [tmdbID, 'tv' if contentType == 'show' else contentType])
        updateJustWatchOffersObj.updateJustWatchOffersDetails()
    else:
        print('Not found JustwatchUrl getting justwatch url from tmdb and updating offers.....')
        constantsObj.TMDB_DETAILS = [tmdbID, contentType]
        subprocess.run(
            (["sh", str(currentPath).replace('/justWatchMovies', '') + '/allShFiles/justWatchOffersUpdate.sh',
              str(contentType), str(tmdbID)]))
        justWatchMoviesOffersDetails = dBServicesObj.checkIfOffersUlrExistOrNot(tmdbID,
                                                                                'tv' if contentType == 'show' else contentType)
        if justWatchMoviesOffersDetails:
            findMovieOffers(justWatchMoviesOffersDetails)
            updateJustWatchOffersObj = updateJustWatchOffers('updateOffersByID', 0, [tmdbID, contentType])
            updateJustWatchOffersObj.updateJustWatchOffersDetails()
        else:
            print('not exist offers')
