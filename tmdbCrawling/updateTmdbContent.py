import datetime
import json
import requests
from db_services import dBServices

dbServicesObj = dBServices()


def getFirstAndLastName(name):
    firstName, lastName = None, None
    firstAndLastNames = name.split(' ', 1)
    if len(firstAndLastNames) > 1:
        firstName, lastName = firstAndLastNames[0], firstAndLastNames[1]
    else:
        firstName, lastName = firstAndLastNames[0], None
    return firstName, lastName


def updateCastCrewInformation(i, contentID, sessionID):
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
        personID = dbServicesObj.insertCastAndCrew(peopleDetails)
        roleID = dbServicesObj.getPeopleRole(known_for_department)
        contentROle = dbServicesObj.insertIntoContentRole(contentID, sessionID, personID, roleID,
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
        contentID = 1
        tmdbID = dbServicesObj.getTmdbID(contentType)
        # tmdbID = [{'tmdb_ref_id': 284053}]
        for i in tmdbID:
            url = f"https://api.themoviedb.org/3/{apiContentType}/{i['tmdb_ref_id']}/credits?language=en-US"
            headers = {
                "accept": "application/json",
                "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
            }
            response = requests.get(url, headers=headers)
            data = json.loads(response.text)
            castData = data['cast']
            crewData = data['crew']
            sessionID = None
            if contentType == 'MOVIE':
                sessionID = None
            for i in castData:
                updateCastCrewInformation(i, contentID, sessionID)
            for i in crewData[:10]:
                updateCastCrewInformation(i, contentID, sessionID)
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


def commonFunctionForUpdatingImages(i, contentID, sessionID, backdropsID):
    try:
        imagePath = i['file_path']
        if not dbServicesObj.checkIfContentImagesAlreadyExist(contentID, sessionID):
            imageID = dbServicesObj.updateImagesOfContents(imagePath, backdropsID)
            dbServicesObj.updateImagesIntoContentImageMapping(imageID, contentID, sessionID)
        else:
            print('images already exist')
    except Exception as error:
        print(error)


def updateImages():
    try:
        tmdbMovieId = 1
        sessionID = None
        contentID = 1
        contentType = 'MOVIE'
        apiContentType = 'movie'
        if contentType != 'MOVIE':
            apiContentType = 'tv'
        url = f"https://api.themoviedb.org/3/{apiContentType}/{tmdbMovieId}/images"
        headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI1MTlmZThiZGZhOGYxNDM4YTljNTRmNzNhOGYxNTM5YiIsInN1YiI6IjYzM2I2MDY1ZjEwYTFhMDA4MTA3MzdjOCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.n_-VoXG5hr2IDuZ0B1oe0vHiJI4lde17uZorqTG5ZqI"
        }
        response = requests.get(url, headers=headers)
        data = json.loads(response.text)
        backdrops = data['backdrops']
        posters = data['posters']
        backdropsID = None
        for i in backdrops:
            backdropsID = 6
            commonFunctionForUpdatingImages(i, contentID, sessionID, backdropsID)
        for i in posters:
            backdropsID = 7
            commonFunctionForUpdatingImages(i, contentID, sessionID, backdropsID)



    except Exception as error:
        print(error)
