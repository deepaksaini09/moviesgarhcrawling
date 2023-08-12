from db_services import dBServices
from common_services import commonServices
from constants import constants

dBServicesObj = dBServices()
commonServicesObj = commonServices()
constantsObj = constants()


def generateSeoDataForRanking():
    try:
        count = 0
        contentType = "show"
        pageType = 'S'
        data = dBServicesObj.getMoviesNameForGenerateSeoData(contentType)
        for i in data:
            try:
                contentTitle = i['title']
                contentID = i['id']
                webContentType = 'web series'
                count += 1
                cleanString = commonServicesObj.removeSpecialCharacters(contentTitle)
                firstDigit = str(contentID)[0]
                lastDigit = str(contentID)[-1]
                seoUrl = contentType + '-' + str(firstDigit) + str(contentID) + str(
                    lastDigit) + '-' + cleanString.replace(
                    ' ', '-')
                print(seoUrl, ',', cleanString, ',', i['title'])
                metaDescription = f"Discover Where to Watch {contentTitle} {contentType if contentType == 'movie' else webContentType} | Explore the Trailer, Cast, Crew, and Gallery | Online on {constantsObj.domainName}"
                heading = contentTitle
                metaPageTitle = f"Watch online {contentTitle} " + str(
                    contentType if contentType == 'movie' else webContentType)
                dBServicesObj.insertDataIntoSeoTable(contentID, seoUrl.lower(), metaDescription.title(),
                                                     heading.title(), metaPageTitle.title(), pageType)


            except Exception as error:
                print(error)
    except Exception as error:
        print(error)


def generateSeoShowDataForRanking():
    try:
        count = 0
        contentType = "show"
        pageType = 'SE'
        data = dBServicesObj.getShowDataForGenerateSeoData(contentType)
        for i in data:
            try:
                seasonNumber = i['season_number']
                contentTitle = i['title'] + " season number " + str(seasonNumber)
                contentID = i['id']
                commonTitle = i['title'] + " web series " + " season number " + str(seasonNumber)
                cleanString = commonServicesObj.removeSpecialCharacters(contentTitle)
                firstDigit = str(contentID)[0]
                lastDigit = str(contentID)[-1]
                seoUrl = contentType + '-' + str(firstDigit) + str(contentID) + str(
                    lastDigit) + '-' + cleanString.replace(
                    ' ', '-')
                print(seoUrl, ',', cleanString, ',', i['title'])
                metaDescription = f"Discover Where to Watch {commonTitle} | Explore the Trailer, Cast, Crew, and Gallery | Online on {constantsObj.domainName}"
                heading = contentTitle
                metaPageTitle = f"Watch online {commonTitle} "
                dBServicesObj.insertDataIntoSeoTable(contentID, seoUrl.lower(), metaDescription.title(),
                                                     heading.title(), metaPageTitle.title(), pageType)

            except Exception as error:
                print(error)
    except Exception as error:
        print(error)


generateSeoDataForRanking()
