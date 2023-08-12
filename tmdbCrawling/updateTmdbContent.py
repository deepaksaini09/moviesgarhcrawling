import datetime
import json
import requests
from db_services import dBServices
from db_connections import dBConnection

dbServicesObj = dBServices()
dBConnectionObj = dBConnection()


def getFirstAndLastName(name):
    firstName, lastName = None, None
    firstAndLastNames = name.split(' ', 1)
    if len(firstAndLastNames) > 1:
        firstName, lastName = firstAndLastNames[0], firstAndLastNames[1]
    else:
        firstName, lastName = firstAndLastNames[0], None
    return firstName, lastName


def updateCastCrewInformation(conn, cursor, i, contentID, sessionID):
    try:
        peopleTmdbID = i['id']
        known_for_department = i['known_for_department']
        realName = i['original_name']
        popularity = i['popularity']
        profile_path = i['profile_path']
        character = None
        order = None
        try:
            order = i['order']
            character = i['character']
        except Exception as error:
            character = i['job']
            order = None
            print('this is crew member', error)
        firstName, lastName = getFirstAndLastName(realName)
        peopleDetails = {"peopleTmdbID": peopleTmdbID,
                         'realName': realName,
                         'firstName': firstName,
                         'lastName': lastName,
                         'popularity': popularity,
                         'profile_path': profile_path
                         }
        personID = dbServicesObj.insertCastAndCrew(conn, cursor, peopleDetails)
        roleID = dbServicesObj.getPeopleRole(conn, cursor, known_for_department)
        contentROle = dbServicesObj.insertIntoContentRole(conn, cursor, contentID, sessionID, personID, roleID,
                                                          datetime.datetime.now(), datetime.datetime.now(),
                                                          order, character,
                                                          'publish', None)
        if contentROle:
            print('inserted ---')
        else:
            print('not inserted ----')
    except Exception as error:
        print(error)


def peopleData():
    try:
        contentType = 'MOVIE'
        apiContentType = 'movie'
        if contentType != 'MOVIE':
            apiContentType = 'tv'
        tmdbID = dbServicesObj.getTmdbID(contentType)
        conn, cursor = dBConnectionObj.dBConnectionForStreamA2Z()
        # tmdbID = [{'tmdb_ref_id': 284053}]
        for i in tmdbID:
            try:
                url = f"https://api.themoviedb.org/3/{apiContentType}/{i['tmdb_ref_id']}/credits?language=en-US"
                headers = {
                    "accept": "application/json",
                    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
                }
                contentID = i['id']
                response = requests.get(url, headers=headers)
                data = json.loads(response.text)
                castData = data['cast']
                crewData = data['crew']
                sessionID = None
                if contentType == 'MOVIE':
                    sessionID = None
                for i in castData:
                    try:
                        updateCastCrewInformation(conn, cursor, i, contentID, sessionID)
                    except Exception as error:
                        print(error)
                for i in crewData[:10]:
                    try:
                        updateCastCrewInformation(conn, cursor, i, contentID, sessionID)
                    except Exception as error:
                        print(error)
            except Exception as error:
                print(error)
    except Exception as error:
        print(error)


def updateMoviesAndShows():
    try:
        url = "https://api.themoviedb.org/3/movie/movie_id/videos?language=en-US"

        headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
        }

        response = requests.get(url, headers=headers)

        print(response.text)
    except Exception as error:
        print(error)


def commonFunctionForUpdatingImages(cursor, conn, i, contentID, sessionID, backdropsID):
    try:
        imagePath = i['file_path']
        imageID = dbServicesObj.updateImagesOfContents(cursor, conn, imagePath, backdropsID)
        dbServicesObj.updateImagesIntoContentImageMapping(cursor, conn, imageID, contentID, sessionID)
    except Exception as error:
        print(error)


def updateImages(contentType):
    try:
        contentType = contentType
        tmdbID = dbServicesObj.getTmdbID(contentType)
        conn, cursor = dBConnectionObj.dBConnectionForStreamA2Z()
        for moviesDetails in tmdbID:
            try:
                sessionID = None
                contentID = moviesDetails['id']
                apiContentType = 'movie'
                if contentType != 'MOVIE':
                    apiContentType = 'tv'
                url = f"https://api.themoviedb.org/3/{apiContentType}/{moviesDetails['tmdb_ref_id']}/images"
                headers = {
                    "accept": "application/json",
                    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
                }
                response = requests.get(url, headers=headers)
                data = json.loads(response.text)
                backdrops = data['backdrops'][:10]
                posters = data['posters'][:10]
                backdropsID = None
                # if dbServicesObj.checkIfContentImagesAlreadyExist(cursor, conn, contentID, sessionID, imageType):
                try:
                    imageType = 6
                    if not dbServicesObj.checkIfContentImagesAlreadyExist(cursor, conn, contentID, sessionID, imageType):
                        for i in backdrops:
                            try:
                                commonFunctionForUpdatingImages(cursor, conn, i, contentID, sessionID, imageType)
                            except Exception as error:
                                print(error)
                except Exception as error:
                    print(error)
                try:
                    imageType = 7
                    if not dbServicesObj.checkIfContentImagesAlreadyExist(cursor, conn, contentID, sessionID,
                                                                          imageType):
                        for i in posters:
                            try:
                                commonFunctionForUpdatingImages(cursor, conn, i, contentID, sessionID, imageType)
                            except Exception as error:
                                print(error)
                except Exception as error:
                    print(error)
            except Exception as error:
                print(error)
        conn.close()

    except Exception as error:
        print(error)


def updateImagesForShowBackDropsImages():
    try:
        contentType = 'show'
        tmdbID = dbServicesObj.getTmdbID(contentType)
        conn, cursor = dBConnectionObj.dBConnectionForStreamA2Z()
        for moviesDetails in tmdbID:
            try:
                sessionID = None
                contentID = moviesDetails['id']
                apiContentType = 'movie'
                if contentType != 'MOVIE':
                    apiContentType = 'tv'
                url = f"https://api.themoviedb.org/3/{apiContentType}/{moviesDetails['tmdb_ref_id']}/images"
                headers = {
                    "accept": "application/json",
                    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
                }
                response = requests.get(url, headers=headers)
                data = json.loads(response.text)
                backdrops = data['backdrops'][:10]
                posters = data['posters'][:10]
                backdropsID = None
                if dbServicesObj.checkIfContentImagesAlreadyExist(cursor, conn, contentID, sessionID):
                    continue
                for i in backdrops:
                    try:
                        backdropsID = 6
                        commonFunctionForUpdatingImages(cursor, conn, i, contentID, sessionID, backdropsID)
                    except Exception as error:
                        print(error)
                for i in posters:
                    try:
                        backdropsID = 7
                        commonFunctionForUpdatingImages(cursor, conn, i, contentID, sessionID, backdropsID)
                    except Exception as error:
                        print(error)
            except Exception as error:
                print(error)
        conn.close()

    except Exception as error:
        print(error)


def updateImagesForShowsForPoster():
    try:
        contentType = 'show'
        tmdbID = dbServicesObj.getTmdbID(contentType)
        conn, cursor = dBConnectionObj.dBConnectionForStreamA2Z()
        for moviesDetails in tmdbID:
            try:
                sessionID = None
                contentID = moviesDetails['id']
                apiContentType = 'movie'
                if contentType != 'MOVIE':
                    apiContentType = 'tv'
                for seasonNumber in range(1, moviesDetails['no_of_seasons'] + 1):
                    sessionID = dbServicesObj.getSessionID(contentID, seasonNumber)
                    if not sessionID:
                        continue
                    url = f"https://api.themoviedb.org/3/{apiContentType}/{moviesDetails['tmdb_ref_id']}/season/{seasonNumber}/images"
                    headers = {
                        "accept": "application/json",
                        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
                    }
                    response = requests.get(url, headers=headers)
                    data = json.loads(response.text)
                    # backdrops = data['backdrops'][:10]
                    posters = data['posters'][:10]
                    backdropsID = None
                    if dbServicesObj.checkIfContentImagesAlreadyExist(cursor, conn, contentID, sessionID):
                        continue
                    # for i in backdrops:
                    #     try:
                    #         backdropsID = 6
                    #         commonFunctionForUpdatingImages(cursor, conn, i, contentID, sessionID, backdropsID)
                    #     except Exception as error:
                    #         print(error)
                    for i in posters:
                        try:
                            backdropsID = 7
                            commonFunctionForUpdatingImages(cursor, conn, i, contentID, sessionID, backdropsID)
                        except Exception as error:
                            print(error)
            except Exception as error:
                print(error)
        conn.close()

    except Exception as error:
        print(error)


def peopleDataForShowsSeason():
    try:
        contentType = 'show'
        apiContentType = 'movie'
        if contentType != 'MOVIE':
            apiContentType = 'tv'
        tmdbID = dbServicesObj.getTmdbID(contentType)
        conn, cursor = dBConnectionObj.dBConnectionForStreamA2Z()
        # tmdbID = [{'tmdb_ref_id': 284053}]
        for i in tmdbID:
            contentID = i['id']
            for seasonNumber in range(1, i['no_of_seasons'] + 1):
                sessionID = dbServicesObj.getSessionID(contentID, seasonNumber)
                if not sessionID:
                    continue
                try:
                    url = f"https://api.themoviedb.org/3/{apiContentType}/{i['tmdb_ref_id']}/season/{seasonNumber}/credits"
                    print(url)
                    headers = {
                        "accept": "application/json",
                        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
                    }
                    response = requests.get(url, headers=headers)
                    data = json.loads(response.text)
                    castData = data['cast']
                    crewData = data['crew']
                    if contentType == 'MOVIE':
                        sessionID = None
                    try:
                        for k in castData:
                            try:
                                updateCastCrewInformation(conn, cursor, k, contentID, sessionID)
                            except Exception as error:
                                print(error)
                    except Exception as error:
                        print(error)
                    try:
                        for li in crewData[:10]:
                            try:
                                updateCastCrewInformation(conn, cursor, li, contentID, sessionID)
                            except Exception as error:
                                print(error)
                    except Exception as error:
                        print(error)
                except Exception as error:
                    print(error)
    except Exception as error:
        print(error)
