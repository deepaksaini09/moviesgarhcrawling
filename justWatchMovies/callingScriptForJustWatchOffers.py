import sys
from db_services import dBServices
from justWatchMovies.justWatchOffers import findMovieOffers

dBServicesObj = dBServices()
args = sys.argv
acc = 'movie'
if len(args) >= 2:
    acc = 'tv'
justWatchMoviesID = dBServicesObj.getJustWatchMovieID(acc)
findMovieOffers(justWatchMoviesID)
