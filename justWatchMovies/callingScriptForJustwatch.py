import sys

from moviesDetailsForJustwatch import justNowMovies
from db_services import dBServices

dBServicesObj = dBServices()


class callingJustNowScriptForMovieDetails:
    def __init__(self, movieType):
        self.jusNowGenreObj = None
        self.startCursor = ""
        self.movieType = movieType
        self.popularCount = 1

    def startProcess(self, movieTypeFact):
        dBServicesObj.deListOldRecordsForJustWatch(movieTypeFact)
        for count in range(1, 1991):
            try:
                self.jusNowGenreObj = justNowMovies(self.startCursor, self.movieType, self.popularCount)
                self.startCursor, self.popularCount = self.jusNowGenreObj.findListPageGenreDetails(1)
            except Exception as error:
                print('process end due to error occurred', error)


args = sys.argv
movieType = args[1]
if movieType == 'm':
    movieType = 'MOVIE'
else:
    movieType = 'SHOW'
moviesDetailsObj = callingJustNowScriptForMovieDetails(movieType)
moviesDetailsObj.startProcess(movieType)
