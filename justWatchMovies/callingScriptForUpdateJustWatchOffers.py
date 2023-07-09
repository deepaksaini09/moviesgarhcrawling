import sys

from justWatchMovies.updateJustWatchOffers import updateJustWatchOffers

args = sys.argv
offset = 0
if len(args) >= 2:
    if sys.argv[1] == 'jio':
        offersListObj = updateJustWatchOffers('jio', offset)
    else:
        offersListObj = updateJustWatchOffers(True, offset)
    offersListObj.updateJustWatchOffersDetails()
else:
    offersListObj = updateJustWatchOffers(False, offset)
    offersListObj.updateJustWatchOffersDetails()
