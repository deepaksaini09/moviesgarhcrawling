from db_connections import dBConnection
from common_services import commonServices
from constants import constants
import time
from datetime import datetime, timedelta


class dBServices:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.dbConnectionObj = dBConnection()
        self.commonServiceObj = commonServices()
        self.constantObj = constants()

    def checkIfMoviesIFExistInNotHavingOffers(self, tmdbID, contentType, seasonNumber):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from  crawling_just_watch_not_having_offres where tmdb_ref_id=%s and content_type=%s """
            if seasonNumber:
                SQL += """  and seasonNumber=""" + str(seasonNumber)
            self.cursor.execute(SQL, (tmdbID, contentType))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertMoviesNotHavingOffers(self, tmdbID, contentType, seasonNumber):
        try:
            existOffers = self.checkIfMoviesIFExistInNotHavingOffers(tmdbID, contentType, seasonNumber)
            updatedAt = datetime.now()
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existOffers:
                # SQL
                SQL = """ insert into crawling_just_watch_not_having_offres(tmdb_ref_id, seasonNumber, content_type, 
                updated_at) values (%s,%s,%s,%s)
                      """
                self.cursor.execute(SQL, (tmdbID, seasonNumber, contentType, updatedAt))
                self.conn.commit()
            else:
                # SQL
                SQL = """ update crawling_just_watch_not_having_offres set updated_at=%s where id=%s """
                self.cursor.execute(SQL, (updatedAt, existOffers))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def getContentCountSeasonAndEpisode(self, contentID, seasonNumber):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select season_number,max_number_of_episodes from content_seasons where content_id=%s and 
                      season_number=%s """
            self.cursor.execute(SQL, (contentID, seasonNumber))
            allSeasonData = self.cursor.fetchall()
            if allSeasonData:
                self.conn.close()
                return allSeasonData
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def updateJustWatchCrawlersOffers(self, oId, updated_at):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ update crawling_just_watch_offers set offer_updated_at=%s where id=%s"""
            self.cursor.execute(SQL, (updated_at, oId))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def updateOffersPriceForContentOffers(self, offerPrice,
                                          contentType, title, offerProviderUrl, existID, totalEpisodeCount):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ update content_offers set retail_price=%s, updated_at=%s,title=%s, standard_web_url=%s,
                                                total_episodes_count=%s,updated_from=%s where id=%s                                                                                                
                  """
            print(contentType, title, offerProviderUrl, totalEpisodeCount, existID)
            self.cursor.execute(SQL,
                                (offerPrice, datetime.now(), title, offerProviderUrl, totalEpisodeCount, 1, existID))
            print(self.cursor.statement)
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def insertOffersDataForContentOffers(self, title,
                                         contentID,
                                         contentType,
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
                                         updated_at):
        try:
            existID = self.checkIfOffersAlreadyExistOnContentOffers(contentID, moniID, streamID, vPID, contentType,
                                                                    seasonNumber, episodeNumber)
            if not existID:
                self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                # SQL
                SQL = """ insert into content_offers(title, content_id, content_type, streaming_provider, monetization,
                               video_presentation, audio_language, retail_price, currency, standard_web_url, status,
                               crawl_episode_number,crawl_season_number,season_number,episode_number,total_episodes_count, 
                               created_at, updated_at,updated_from)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                      """
                self.cursor.execute(SQL, (title, contentID, contentType, streamID, moniID, vPID, originalLanguageId,
                                          offerPrice, currency, offer_provider_url, status, episodeNumber, seasonNumber,
                                          seasonNumber, episodeNumber, seasonHavingTotalEpisode, created_at, updated_at,
                                          1))
                self.conn.commit()
                self.conn.close()
            else:
                self.updateOffersPriceForContentOffers(offerPrice,
                                                       contentType, title, offer_provider_url, existID,
                                                       seasonHavingTotalEpisode
                                                       )
        except Exception as error:
            print(error)

    def checkIfOffersAlreadyExistOnContentOffers(self, contentID, moniID, streamID, vPID, contentType,
                                                 seasonNumber=None, episodeNumber=None):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ SELECT CA.id as contentOffersId  FROM content_offers CA WHERE content_id =%s and content_type=%s
                                                   and streaming_provider = %s and 
                                                    monetization = %s and 
                                                    video_presentation = %s and 
                                                    status=%s 
                                                    
                """
            if seasonNumber:
                SQL += """ and season_number=""" + str(seasonNumber)
            if episodeNumber:
                SQL += """ and  episode_number=""" + str(episodeNumber)
            self.cursor.execute(SQL, (contentID, contentType, streamID, moniID, vPID, 'publish'))
            existOffersID = self.cursor.fetchone()
            if existOffersID:
                self.conn.close()
                return existOffersID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getMoniID(self, availableFor):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ SELECT CND.id as mID  FROM cnd CND 
                        WHERE cnd_name=%s and cnd_type=%s"""
            self.cursor.execute(SQL, (availableFor, 'STREAMING_TYPE'))
            moniID = self.cursor.fetchone()
            if moniID:
                self.conn.close()
                return moniID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getOffersID(self, offerType, ):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ SELECT CND.id as vPID  FROM cnd CND WHERE cnd_code =%s and cnd_type=%s"""
            self.cursor.execute(SQL, (offerType, 'RESOLUTIONS_TYPE'))
            offersID = self.cursor.fetchone()
            if offersID:
                self.conn.close()
                return offersID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getStreamProviderID(self, offerProviderName):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ SELECT A.id as stramID  FROM streaming_provider A WHERE provider =%s """
            self.cursor.execute(SQL, (offerProviderName,))
            streamProviderID = self.cursor.fetchone()
            if streamProviderID:
                self.conn.close()
                return streamProviderID[0]
            self.conn.close()
            return False

        except Exception as error:
            print(error)

    def getContentIDByTMDBID(self, tmdbRefID, contentType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ SELECT C.id as contentId, C.content_type,C.original_language_id,C.title 
                        FROM contents C where  C.content_type=%s and C.tmdb_ref_id=%s """
            self.cursor.execute(SQL, (contentType, tmdbRefID))
            contentData = self.cursor.fetchone()
            if contentData:
                self.conn.close()
                return contentData
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getOffersListData(self, providerName, offset, tmdbAndContentType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """   SELECT id,
                               offer_type,
                               tmdb_ref_id,
                               offer_provider_url,
                               offer_provider_name,
                               available_for,
                               offers_price,
                               created_at,
                               updated_at,
                               offer_updated_at,
                               content_type,
                               season_number,
                               monetization_type
                        FROM crawling_just_watch_offers 
            """
            if tmdbAndContentType:
                SQL += """ where tmdb_ref_id=""" + str(tmdbAndContentType[0]) + """  and content_type='""" + str(
                    tmdbAndContentType[1]) + """'"""
            else:
                if providerName:
                    SQL += """ where offer_provider_name like '%""" + str(providerName) + """%' """
                SQL += """  order by updated_at desc limit 1000 offset """ + str(offset)
            self.cursor.execute(SQL)
            offersList = self.cursor.fetchall()
            if offersList:
                self.conn.close()
                return offersList
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getOffersOfnewReleaseMovieAndShows(self, offset):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                    select o.id,
                            o.offer_type,
                            o.tmdb_ref_id,
                            o.offer_provider_url,
                            o.offer_provider_name,
                            o.available_for,
                            o.offers_price,
                            o.created_at,
                            o.updated_at,
                            o.offer_updated_at,
                            o.content_type,
                            o.season_number,
                            o.monetization_type
                            from 
                             crawling_just_watch_offers o where cast(o.created_at as date)=cast(now() as date)
                             limit 1000 offset %s
                  """
            self.cursor.execute(SQL, (offset,))
            offersList = self.cursor.fetchall()
            if offersList:
                self.conn.close()
                return offersList
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkJustWatchUrlUsingImdbID(self, imdbID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                              select id from just_watch_crawlers where imdb_id = %s and latest_crawl=0 and 
                                                                cast(updated_at as date)<>cast(now() as date );
                  """
            self.cursor.execute(SQL, (imdbID,))
            movieID = self.cursor.fetchone()
            if movieID:
                return movieID[0]
            return False
        except Exception as error:
            print(error)

    def checkJustWatchUrl(self, justWatchUrl):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                  select id, cast(updated_at as date)  from just_watch_crawlers where just_watch_url = %s ;
                  """
            self.cursor.execute(SQL, (justWatchUrl,))
            movieIDs = self.cursor.fetchone()
            print(movieIDs)
            updatedDate = datetime.now().date()
            updateOrNot = False
            if movieIDs:
                movieID = movieIDs[0]
                print(str(movieIDs[1]), str(updatedDate))
                self.conn.close()
                if str(movieIDs[1]) != str(updatedDate):
                    updateOrNot = True
                return movieID, updateOrNot
            self.conn.close()
            return False, False
        except Exception as error:
            print(error)

    def deListOldRecordsForJustWatch(self, movieType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                          update just_watch_crawlers set latest_crawl = 0 where movieType=%s;
                 """
            self.cursor.execute(SQL, (movieType,))
            self.conn.commit()
        except Exception as error:
            print(error)

    def insertJustWatchGenreData(self, serial, movieTitle, imdbIDUrl, justWatchUrl, imdbID, imdbVotes,
                                 imdbRating, movieType):
        try:
            movieID = None
            print(movieID)
            if imdbID == "None":
                imdbID = None
            movieID, updateOrNot = self.checkJustWatchUrl(justWatchUrl)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not movieID:
                # SQL
                SQL = """insert into just_watch_crawlers(name, 
                                                        just_watch_popular_id, 
                                                        just_watch_url, 
                                                        imdb_url, 
                                                        imdb_id, 
                                                        imdb_rating,
                                                        imdb_votes,
                                                        movieType,
                                                        latest_crawl, updated_at)  values (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s);
                      """
                self.cursor.execute(SQL, (movieTitle, serial, justWatchUrl, imdbIDUrl, imdbID, imdbRating,
                                          imdbVotes, movieType, 1, datetime.now()))
                self.conn.commit()
                serial += 1
                movieID = self.cursor.lastrowid
            elif updateOrNot:
                # SQL
                SQL = """
                     update just_watch_crawlers set just_watch_popular_id =%s,imdb_rating=%s,imdb_votes=%s, 
                                                    updated_at=%s,latest_crawl=%s where id=%s
                      """
                self.cursor.execute(SQL, (serial, imdbRating, imdbVotes, datetime.now(), 1, movieID))
                self.conn.commit()
                serial += 1
            self.conn.close()
            return serial
        except Exception as error:
            print(error)

    def checkIfAlreadyExistMovieOffers(self, tmdbID, contentType, seasonNumber):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_just_watch_offers where tmdb_ref_id =%s and content_type=%s"""
            if seasonNumber:
                SQL += """ and season_number=""" + str(seasonNumber)
            self.cursor.execute(SQL, (tmdbID, contentType))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return True
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def commonInsertQueryForJustWatchOffers(self, i, tmdbID, contentType, seasonNumber=None):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   insert into crawling_just_watch_offers(offer_type, tmdb_ref_id, offer_provider_url, 
                                        offer_provider_name, available_for, offers_price , updated_at,content_type, 
                                        season_number, monetization_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                 """
            self.cursor.execute(SQL, (i['offerType'], tmdbID, i['standardWebURL'], i['providerName'],
                                      i['availableFor'], i['retailPriceValue'], i['updatedAt'], contentType,
                                      seasonNumber, i['monetizationType']))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def updateJustWatchOffers(self, price, Id, updatedAt, standardUrl, monetizationType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                       update crawling_just_watch_offers set updated_at=%s, offer_provider_url=%s, monetization_type=%s  
                  """
            if price:
                SQL += """ , offers_price=""" + str(price)
            SQL += """  where id= """ + str(Id)
            self.cursor.execute(SQL, (updatedAt, standardUrl, monetizationType))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def checkIfParticularMovieOffersExist(self, i, tmdbID, contentType, seasonNumber=None):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_just_watch_offers where  offer_type=%s and tmdb_ref_id=%s
                                                                and offer_provider_name=%s 
                                                                and available_for=%s 
                                                                and content_type=%s
                                                                
                 """
            if seasonNumber:
                SQL += """  and season_number= """ + str(seasonNumber)
            self.cursor.execute(SQL, (i['offerType'], tmdbID, i['providerName'], i['availableFor'], contentType))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            else:
                self.conn.close()
                return False
        except Exception as error:
            print(error)

    def insertMoviesOffersForJustWatch(self, offers, tmdbID, contentType, seasonNumber):
        try:
            updatedAt = datetime.now()
            offersExistOrNot = self.checkIfAlreadyExistMovieOffers(tmdbID, contentType, seasonNumber)
            if not offersExistOrNot:
                for i in offers:
                    try:
                        self.commonInsertQueryForJustWatchOffers(i, tmdbID, contentType, seasonNumber)
                    except Exception as error:
                        print(error)
            else:
                for j in offers:
                    existID = self.checkIfParticularMovieOffersExist(j, tmdbID, contentType, seasonNumber)
                    if not existID:
                        self.commonInsertQueryForJustWatchOffers(j, tmdbID, contentType, seasonNumber)
                    else:
                        self.updateJustWatchOffers(j['retailPriceValue'], existID, j['updatedAt'], j['standardWebURL'],
                                                   j['monetizationType'])
        except Exception as error:
            print(error)

    def checkIfPaginationUrlForJustWatchExistOrNot(self, tmdbID, movieType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_just_watch_list_page where tmdb_ref_id =%s and content_type=%s"""
            self.cursor.execute(SQL, (tmdbID, movieType))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertMovieUrlForPaginationUrlForJustWatch(self, url, movieID, updatedAt, tmdbID, movieType):
        try:
            existMovieID = self.checkIfPaginationUrlForJustWatchExistOrNot(tmdbID, movieType)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existMovieID:
                # SQL
                SQL = """ insert into crawling_just_watch_list_page(just_watch_url, just_watch_movie_id, 
                            updated_at, tmdb_ref_id, content_type)values(%s,%s,%s,%s,%s)"""
                self.cursor.execute(SQL, (url, movieID, updatedAt, tmdbID, movieType))
                self.conn.commit()
            else:
                # SQL
                SQL = """ update crawling_just_watch_list_page set updated_at =%s where id =%s"""
                self.cursor.execute(SQL, (updatedAt, existMovieID))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def getContentIDByTMDBByCreateAt(self, offset):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select tmdb_ref_id, content_type from contents order by created_at desc limit 100 offset %s"""
            self.cursor.execute(SQL, (offset,))
            imdbData = self.cursor.fetchall()
            self.conn.close()
            return imdbData
        except Exception as error:
            print(error)

    def getContentsTMDBID(self, offset):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select  c.tmdb_ref_id,c.content_type
                        from seo s
                                 join contents c on s.entity_id = c.id
                        order by s.popularity_score desc limit 100 offset %s """
            self.cursor.execute(SQL, (offset,))
            imdbData = self.cursor.fetchall()
            self.conn.close()
            return imdbData
        except Exception as error:
            print(error)

    def getJustWatchMovieID(self, accType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select just_watch_movie_id, tmdb_ref_id,content_type,just_watch_url 
                        from crawling_just_watch_list_page  where content_type=%s order by id """
            self.cursor.execute(SQL, (accType,))
            moviesID = self.cursor.fetchall()
            if moviesID:
                return moviesID
        except Exception as error:
            print(error)

    # ------------------------------------------------Gadget360Song-----------------------------------------------------

    def checkIfPaginationUrlExistOrNot(self, url):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                  select id from crawling_gadget360_list_page where url = %s
                  """
            self.cursor.execute(SQL, (url,))
            urlID = self.cursor.fetchone()
            if urlID:
                self.conn.close()
                return urlID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertPaginationUrl(self, items):
        try:
            updated_at = datetime.now()
            exist = self.checkIfPaginationUrlExistOrNot(items['url'])
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not exist:
                # SQL
                SQL = """
                         insert into crawling_gadget360_list_page(url_type, url, released_type, updated_at) values(%s,%s,%s,%s);
                      """
                self.cursor.execute(SQL, (items['url_type'], items['url'], items['released_type'], updated_at))
                self.conn.commit()
                self.conn.close()
            else:
                SQL = """ update crawling_gadget360_list_page set updated_at=%s,released_type=%s where id=%s """
                self.cursor.execute(SQL, (updated_at, items['released_type'], exist))
                self.conn.commit()
                self.conn.close()
                print(items['url'], 'this url already exist')
        except Exception as error:
            print(error)

    def fetchGadget360Urls(self, releasedType=None):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   select id, url,url_type from crawling_gadget360_list_page
                  """
            if releasedType:
                SQL += """  where released_type= '""" + str(releasedType) + """'"""
            else:
                SQL += """ where url_type <> 'web' """
            self.cursor.execute(SQL)
            songsUrl = self.cursor.fetchall()
            print(len(songsUrl))
            return songsUrl
        except Exception as error:
            print(error)

    # def fetchGadget360DataNotHavingSongs(self):
    #     try:
    #         self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
    #         # SQL
    #         SQL = """
    #               Select id, url, url_type from pagination_url_for_not_found_song where archived_at is  null;
    #               """
    #         self.cursor.execute(SQL)
    #         songsUrl = self.cursor.fetchall()
    #         print(len(songsUrl))
    #         return songsUrl
    #     except Exception as error:
    #         print(error)

    def checkIfMovieExistOrNot(self, movieName, year, contentType=None):
        try:
            # SQL
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """
                   select id,content_type from contents where title = %s 
                  """
            if year and contentType != 'show':
                SQL += """ and release_year =  """ + str(year)
            if contentType == 'web' or contentType == 'show':
                SQL += """ and content_type = 'show' """
            else:
                SQL += """ and content_type = 'movie' """
            self.cursor.execute(SQL, (movieName,))
            movieID = self.cursor.fetchone()
            if movieID:
                self.conn.close()
                return movieID[0], movieID[1]
            self.conn.close()
            return False, False
        except Exception as error:
            print(error)

    def findRatingSourceName(self, sourceName):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                    select id from scoring where score_name = %s;
                  """
            self.cursor.execute(SQL, (sourceName,))
            scoreID = self.cursor.fetchone()
            if scoreID:
                self.conn.close()
                return scoreID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertRatingDataIntoScoreMapping(self, contentID, scoreID, rating, status, createdAt, updatedAt,
                                         contentSeasonID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                insert into content_scoring_mapping (content_id, score_id, score_value, status, created_at, 
                                                    updated_at, season_id)values (%s,%s,%s,%s,%s,%s,%s);            
                  """
            self.cursor.execute(SQL, (contentID, scoreID, rating, status, createdAt, updatedAt, contentSeasonID))
            self.conn.commit()
            self.conn.close()

        except Exception as error:
            print(error)

    def checkSourceRatingExistOrNot(self, contentID, scoreID, contentSeasonID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                  select id from content_scoring_mapping where content_id = %s and score_id = %s
                  """
            if contentSeasonID:
                SQL += """ and season_id= """ + str(contentSeasonID)
            self.cursor.execute(SQL, (contentID, scoreID))

            contentScoreID = self.cursor.fetchone()
            if contentScoreID:
                self.conn.close()
                return contentScoreID[0]
            self.conn.close()
            return False

        except Exception as error:
            print(error)

    def updateSourceRatingInContentSourceMapping(self, rating, upDatedAt, mappingScoreID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """
                    update content_scoring_mapping set score_value = %s , updated_at = %s where id = %s;
                 """
            self.cursor.execute(SQL, (rating, upDatedAt, mappingScoreID))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def checkSourceRatingRefIDExistOrNot(self, contentID, contentSeasonID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   select id,imdb_ref_id, rotten_tomato_ref_id from content_album where content_id=%s
                  """
            if contentSeasonID:
                SQL += """ and season_id =""" + str(contentSeasonID)
            self.cursor.execute(SQL, (contentID,))
            IDs = self.cursor.fetchone()
            if IDs:
                self.conn.close()
                return IDs
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertRatingOfMovieIntoContentAlbum(self, contentID, imdbRating, createdAt, updatedAt, rottenTomatoesRating,
                                            imdbRef, rottenTomatoesRef, contentSeasonID):
        try:
            if imdbRating:
                scoreIDForImdb = self.findRatingSourceName('IMDB Rating')
                contentScoreMappingID = self.checkSourceRatingExistOrNot(contentID, scoreIDForImdb, contentSeasonID)
                if contentScoreMappingID:
                    self.updateSourceRatingInContentSourceMapping(imdbRating, updatedAt, contentScoreMappingID)
                else:
                    self.insertRatingDataIntoScoreMapping(contentID, scoreIDForImdb, round(float(imdbRating), 1),
                                                          'publish', createdAt, updatedAt, contentSeasonID)
            if rottenTomatoesRating:
                scoreIDForRotten = self.findRatingSourceName('RottenTomatoes Rating')
                contentScoreMappingID = self.checkSourceRatingExistOrNot(contentID, scoreIDForRotten, contentSeasonID)
                if contentScoreMappingID:
                    self.updateSourceRatingInContentSourceMapping(rottenTomatoesRating, updatedAt,
                                                                  contentScoreMappingID)
                else:
                    self.insertRatingDataIntoScoreMapping(contentID, scoreIDForRotten, int(rottenTomatoesRating),
                                                          'publish', createdAt, updatedAt, contentSeasonID)

            existRefID = self.checkSourceRatingRefIDExistOrNot(contentID, contentSeasonID)
            albumID = None
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existRefID:
                SQL = """ insert into content_album(content_id, imdb_ref_id,  rotten_tomato_ref_id, created_at, 
                                                     updated_at, status, season_id)  values (%s,%s,%s,%s,%s,%s,%s);
                       """
                self.cursor.execute(SQL, (contentID, imdbRef, rottenTomatoesRef, createdAt, updatedAt, 'publish',
                                          contentSeasonID))
                self.conn.commit()
                albumID = self.cursor.lastrowid
            else:
                albumID = existRefID[0]
                refImdb = existRefID[1]
                refRotten = existRefID[2]
                if not refImdb:
                    SQL = """
                           update content_album set imdb_ref_id = %s ,updated_at = %s where id = %s;
                          """
                    self.cursor.execute(SQL, (imdbRef, updatedAt, albumID))
                    self.conn.commit()
                if not refRotten:
                    SQL = """
                             update content_album set imdb_ref_id = %s ,updated_at = %s where id = %s;
                          """
                    self.cursor.execute(SQL, (rottenTomatoesRef, updatedAt, albumID))
                    self.conn.commit()
            if not albumID:
                print(albumID)
            self.conn.close()
            return albumID
        except Exception as error:
            print(error)

    def findSourceProviderID(self, listenUrl):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            songSource = 'https://' + self.commonServiceObj.findSouceName(listenUrl) + '/'
            if songSource.count('amazon'):
                return 1
            # SQL
            SQL = """
                    select id from audio_provider where official_website = %s;
                  """
            self.cursor.execute(SQL, (songSource,))
            songProviderID = self.cursor.fetchone()[0]
            self.conn.close()
            return songProviderID
        except Exception as error:
            print(error)

    def checkIfArtistPeopleExistOrNot(self, songTitle):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                  select id from song where name=%s;
                  """
            self.cursor.execute(SQL, (songTitle,))
            self.conn.commit()
            songId = self.cursor.fetchone()
            if songId:
                return True
            return False
        except Exception as error:
            print(error)

    def checkSongExistOrNot(self, songTitle, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   select id from song where name = %s and content_id=%s;
                  """
            self.cursor.execute(SQL, (songTitle, contentID))
            songID = self.cursor.fetchone()
            if songID:
                self.conn.close()
                return songID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertPeopleSongRoleData(self, songID, roleID, peopleID, createdAt, updatedAt, status):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   insert into song_role_people(song_id, role_id, people_id, created_at, updated_at, status) 
                   values(%s,%s,%s,%s,%s,%s) 
                  """
            self.cursor.execute(SQL, (songID, roleID, peopleID, createdAt, updatedAt, status))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def checkSongRolePeopleExistOrNot(self, peopleID, roleID, songID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                  select id from song_role_people where song_id=%s and role_id=%s and people_id= %s;
                  """
            self.cursor.execute(SQL, (songID, roleID, peopleID))
            songPeopleRoleID = self.cursor.fetchone()
            if songPeopleRoleID:
                self.conn.close()
                return songPeopleRoleID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkSongUrlExistOrNot(self, songID, songProviderID, albumId):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   select id from song_source where song_id=%s  and source_provider_id=%s;
                  """
            if albumId:
                SQL += """ and album_id=""" + str(albumId)
            self.cursor.execute(SQL, (songID, songProviderID))
            songSourceId = self.cursor.fetchone()
            if songSourceId:
                self.conn.close()
                return songSourceId[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    # def updateSongUrlTableSongExist(self, contentID):
    #     try:
    #         self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
    #         # SQL
    #         SQL = """
    #                update pagination_url_for_not_found_song set archived_at = now() where content_id =%s;
    #               """
    #         self.cursor.execute(SQL, (contentID,))
    #         self.conn.commit()
    #         self.conn.close()
    #     except Exception as error:
    #         print(error)

    def insertSongsData(self, contentID, songTitle, songDurationInMinute, songDurationInSecond,
                        createdAt, updatedAt, listenUrl, albumId, songArtist):
        try:
            # exist = self.checkIfArtistPeopleExistOrNot(songTitle)
            # ------------------- check song exist or not--------------------------------
            songID = self.checkSongExistOrNot(songTitle.strip(), contentID)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not songID:
                # SQL
                SQL = """
                      insert into song(content_id, name, runtime_in_minutes, runtime_in_seconds, created_at, updated_at,
                     status) values(%s, %s,%s,%s,%s,%s,%s);
    
                      """
                self.cursor.execute(SQL, (contentID, songTitle, songDurationInMinute, songDurationInSecond,
                                          createdAt, updatedAt, 'publish'))
                self.conn.commit()
                songID = self.cursor.lastrowid
            else:
                # SQL
                SQL = """ update song set updated_at=%s where id=%s"""
                self.cursor.execute(SQL, (updatedAt, songID))
                self.conn.commit()
                self.conn.close()
            # -----------------------------------------insert song Artist----------------------------------------------
            for person in songArtist:
                firstName, lastName = self.commonServiceObj.findFirstAndLastName(person)
                peopleID = self.findPeople(firstName, lastName)
                if not peopleID:
                    peopleID = self.insertPeopleData(firstName, lastName, 'Done', 'publish', createdAt, updatedAt)
                    self.insertPeopleSongRoleData(songID, 18, peopleID, createdAt, updatedAt, 'publish')
                else:
                    exist = self.checkSongRolePeopleExistOrNot(peopleID, 18, songID)
                    if not exist:
                        self.insertPeopleSongRoleData(songID, 18, peopleID, createdAt, updatedAt, 'publish')

            # ------------------------- find source provider id and insert into song source data ----------------------
            if listenUrl:
                try:
                    songProviderID = self.findSourceProviderID(listenUrl)
                    exist = self.checkSongUrlExistOrNot(songID, songProviderID, albumId)
                    self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                    if not exist:
                        # SQL
                        SQL = """
                                    insert into song_source(song_id, album_id, source_provider_id, 
                                    track_source_url, created_at, updated_at, status) values(%s,%s,%s,%s,%s,%s,%s) 
                              """
                        self.cursor.execute(SQL, (songID, albumId, songProviderID, listenUrl,
                                                  createdAt, updatedAt, 'publish'))
                        self.conn.commit()
                    else:
                        # SQL
                        SQL = """ update song_source set updated_at=%s, album_id=%s where id=%s"""
                        self.cursor.execute(SQL, (updatedAt, albumId, exist))
                        self.conn.commit()
                    self.conn.close()
                    # self.updateSongUrlTableSongExist(contentID)
                except Exception as error:
                    print(error)

        except Exception as error:
            print(error)

    def checkAlbumSongUrlExistOrNot(self, albumID, songProviderID, songUrl):
        try:
            # SQL
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """
                  select id from album_source where album_id=%s and album_source_provider_id =%s and album_source_url =%s;
                  """
            self.cursor.execute(SQL, (albumID, songProviderID, songUrl))
            songAlbumID = self.cursor.fetchone()
            if songAlbumID:
                return songAlbumID[0]
            return False
        except Exception as error:
            print(error)

    def insertDataIntoSongsAlbum(self, providerList, albumID, createdAt, updatedAt):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            for songUrl in providerList:
                print(type(songUrl), songUrl)
                songProviderID = self.findSourceProviderID(songUrl)
                exist = self.checkAlbumSongUrlExistOrNot(albumID, songProviderID, songUrl)
                if not exist:
                    # SQL
                    SQL = """
                         insert into album_source(album_id, album_source_provider_id, 
                                 album_source_url, created_at, updated_at, status)values(%s,%s,%s,%s,%s,%s);
                         """
                    self.cursor.execute(SQL, (albumID, songProviderID, songUrl, createdAt, updatedAt, 'publish'))
                    self.conn.commit()
        except Exception as error:
            print(error)

    # def checkStreamingOrTicketBookUrlExistOrNot(self, contentID, streamingOrBookTicketUrl):
    #     try:
    #         self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
    #         # SQL
    #         SQL = """
    #                select id from gadget360_streaming_on_crawlers where content_id=%s and href=%s;
    #               """
    #         self.cursor.execute(SQL, (contentID, streamingOrBookTicketUrl))
    #         urlID = self.cursor.fetchone()
    #         if urlID:
    #             return urlID[0]
    #         return False
    #     except Exception as error:
    #         print(error)

    # def insertStreamingUrl(self, contentID, streamingOrBookTicketUrl, urlTitle, createdAt, updatedAt):
    #     try:
    #         exist = self.checkStreamingOrTicketBookUrlExistOrNot(contentID, streamingOrBookTicketUrl)
    #         if not exist:
    #             self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
    #             # SQL
    #             SQL = """
    #                 insert into gadget360_streaming_on_crawlers(content_id, href_title, href, created_at, updated_at)
    #                                                            values(%s,%s,%s,%s,%s) ;
    #
    #                   """
    #             self.cursor.execute(SQL, (contentID, urlTitle, streamingOrBookTicketUrl, createdAt, updatedAt))
    #             self.conn.commit()
    #             self.conn.close()
    #
    #     except Exception as error:
    #         print(error)

    def findPeople(self, firstName='', lastName=''):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                        select id from people where first_name = %s and last_name = %s;
                  """
            self.cursor.execute(SQL, (firstName, lastName))
            peopleID = self.cursor.fetchone()
            if peopleID:
                self.conn.close()
                return peopleID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkPeopleRoleContent(self, contentID, peopleID, roleID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   select id from content_role_people where content_id=%s and person_id=%s and role_id=%s;
                  """
            self.cursor.execute(SQL, (contentID, peopleID, roleID))
            contentRoleID = self.cursor.fetchone()
            if contentRoleID:
                self.conn.close()
                return True
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertPeopleRoleData(self, contentID, peopleID, roleId, createdAt, updatedAt, status):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                    insert into content_role_people(content_id, person_id, role_id, created_at, updated_at,  status) 
                    values (%s,%s,%s,%s,%s,%s);
                  """
            self.cursor.execute(SQL, (contentID, peopleID, roleId, createdAt, updatedAt, status))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def insertPeopleData(self, firstName, lastName, updatedStatus, status, updatedAt, createdAt):
        try:
            # SQL
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """
                    insert into people (first_name,last_name,update_status,status,updated_at,created_at)
                                  values (%s,%s,%s,%s,%s,%s);
                  """
            self.cursor.execute(SQL, (firstName, lastName, updatedStatus, status, updatedAt, createdAt))
            self.conn.commit()
            peopleID = self.cursor.lastrowid
            self.conn.close()
            return peopleID
        except Exception as error:
            print(error)

    def insertMusicDirData(self, firstName, lastName, createdAt, updatedAt, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            peopleID = self.findPeople(firstName, lastName)
            if not peopleID:
                peopleID = self.insertPeopleData(firstName, lastName, 'Done', 'publish', updatedAt, createdAt)
                self.insertPeopleRoleData(contentID, peopleID, 16, createdAt, updatedAt, 'publish')
            else:
                peopleRoleExistOrNot = self.checkPeopleRoleContent(contentID, peopleID, 16)
                if not peopleRoleExistOrNot:
                    self.insertPeopleRoleData(contentID, peopleID, 16, createdAt, updatedAt, 'publish')
            self.conn.close()
        except Exception as error:
            print(error)

    # def checkIfSongExistInNotFoundPaginationUrl(self, contentID):
    #     try:
    #         self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
    #         # SQL
    #         SQL = """select id from pagination_url_for_not_found_song where content_id =%s """
    #         self.cursor.execute(SQL, (contentID,))
    #         if self.cursor.fetchall():
    #             self.conn.close()
    #             return True
    #         self.conn.close()
    #         return False
    #
    #     except Exception as error:
    #         print(error)

    # def insertUrlIfNotHavingSong(self, url, contentID, urlType):
    #     try:
    #         urlExist = self.checkIfSongExistInNotFoundPaginationUrl(contentID)
    #         if not urlExist:
    #             self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
    #             # SQL
    #             SQL = """
    #                   insert into pagination_url_for_not_found_song(content_id, url, url_type) values (%s,%s,%s);
    #                   """
    #             self.cursor.execute(SQL, (contentID, url, urlType))
    #             self.conn.commit()
    #             self.conn.close()
    #
    #     except Exception as error:
    #         print(error)

    def getContentSeasonID(self, contentID, seasonNumber):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                  select id from content_seasons where content_id=%s;
                  """
            if seasonNumber:
                SQL += """ and season_number=%s """
            self.cursor.execute(SQL, (contentID, seasonNumber))
            contentSeasonID = self.cursor.fetchone()
            if contentSeasonID:
                self.conn.close()
                return contentSeasonID[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    # def checkIfStreamingOrBMSURLExistOrNot(self, contentID):
    #     try:
    #         self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
    #         SQL = """
    #               select id from gadget360_streaming_on_crawlers where content_id= %s;
    #               """
    #         self.cursor.execute(SQL, (contentID,))
    #         streamID = self.cursor.fetchone()
    #         if streamID:
    #             self.conn.close()
    #             return streamID[0]
    #         self.conn.close()
    #         return False
    #     except Exception as error:
    #         print(error)

    def insertStreamingOrBMSUrl(self, title, streamingOrBookTicketUrl, updatedAt, createdAt, contentID, urldID):
        try:
            print(createdAt)
            bmsStatus = 0
            if title:
                bmsStatus = 1
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ 
                  update crawling_gadget360_list_page set href_title=%s, bms_link=%s, bms_updated_at=%s,content_id=%s, bms_status=%s where id=%s
                  """
            self.cursor.execute(SQL, (title, streamingOrBookTicketUrl, updatedAt, contentID, bmsStatus, urldID))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def getUrlForBMS(self):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                   select content_id from crawling_gadget360_list_page where bms_status=1;
                  """
            self.cursor.execute(SQL)
            urls = self.cursor.fetchall()
            if urls:
                self.conn.close()
                return urls
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    # -----------------------------------------------For GaViews-------------------------------------------------------

    def insertGaViewsForEntertainment(self, results, timeframe, pageType):
        self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
        ts = time.time()
        if results.get(self.constantObj.ROWS) is not None:
            for row in results.get(self.constantObj.ROWS):
                try:
                    # SQL
                    SQL = """
                           insert into ga_views(pagePath, pageViews, uniquePageViews, timeframe, latestcrawl) 
                           values (%s,%s,%s,%s,%s)
                          """
                    self.cursor.execute(SQL, (row[0], row[1], row[2], timeframe, self.constantObj.ONE))
                    self.conn.commit()
                    self.conn.close()
                except Exception as error:
                    print("Problem in Insert", error)

    def truncateOldGaViewsTimeFrameWise(self, timeFrame):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """
                  update ga_views set latestcrawl=0 where timeframe=%s 
                  """
            self.cursor.execute(SQL, (timeFrame,))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    # ------------------------------------------------------ IMDB -----------------------------------------------------
    def insertDataIntoImdbRew(self, awardName, year, imdb_ref_id, people, people_imdb_id, performance_award, awardType,
                              award_text):
        try:
            award_text = award_text.strip()
            awardId = self.movieHavingAlreadyAward(imdb_ref_id, people_imdb_id, performance_award, awardType,
                                                   awardName, year, award_text)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not awardId:
                SQL = """
                insert into crawling_awards(awards_name, year, people, people_imdb_id, performance_award, 
                award_type, award_text, imdb_ref_id) 
                values(%s,%s,%s,%s,%s,%s,%s,%s)
                      """
                self.cursor.execute(SQL, (awardName, year, people, people_imdb_id, performance_award,
                                          awardType, award_text, imdb_ref_id))
                self.conn.commit()
            else:
                SQL = """ update crawling_awards set updated_at=%s,award_type=%s where id=%s"""
                self.cursor.execute(SQL, (datetime.now(), awardType, awardId))
                self.conn.commit()
            self.conn.close()

        except Exception as error:
            print(error)

    def insertDataIntoImdbReward(self, award_name, year, awardType, award_text, peopleWithAward, imdbID):
        try:
            award_text = award_text.strip()
            for i in peopleWithAward:
                bestPerformances = i[0]
                peoples = i[1]
                if peoples:
                    for people in peoples:
                        awardId = self.movieHavingAlreadyAward(imdbID, peoples[people], bestPerformances, awardType,
                                                               award_name, year, award_text)
                        self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                        # SQL
                        if not awardId:
                            SQL = """
                            insert into crawling_awards(awards_name, year, people, people_imdb_id, performance_award, 
                            award_type, award_text, imdb_ref_id) 
                            values(%s,%s,%s,%s,%s,%s,%s,%s)
                                  """
                            self.cursor.execute(SQL, (award_name, year, people, peoples[people], bestPerformances,
                                                      awardType, award_text, imdbID))
                            self.conn.commit()
                        else:
                            SQL = """ update crawling_awards set updated_at=%s,award_type=%s where id=%s"""
                            self.cursor.execute(SQL, (datetime.now(), awardType, awardId))
                            self.conn.commit()
                        self.conn.close()
                else:
                    awardId = self.movieHavingAlreadyAward(imdbID, '', bestPerformances, awardType, award_name, year,
                                                           award_text)
                    print(imdbID)
                    self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                    # SQL
                    if not awardId:
                        peoples = None
                        # SQL
                        SQL = """
                               insert into crawling_awards(awards_name, year, people, people_imdb_id, performance_award, 
                            award_type, award_text,imdb_ref_id) 
                            values(%s,%s,%s,%s,%s,%s,%s,%s)  
                              """
                        self.cursor.execute(SQL, (award_name, year, peoples, '', bestPerformances, awardType,
                                                  award_text, imdbID))
                        self.conn.commit()
                    else:
                        SQL = """ update crawling_awards set updated_at=%s where id=%s"""
                        self.cursor.execute(SQL, (datetime.now(), awardId))
                        self.conn.commit()
                    self.conn.close()
            self.conn.close()

        except Exception as error:
            print(error)

    def getIMDBMovieIDOfLatestYear(self, offset):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select distinct imdb_id from crawling_imdb_awards_year_wise limit 1000 offset %s """
            self.cursor.execute(SQL, (offset,))
            imdbID = self.cursor.fetchall()
            if imdbID:
                self.conn.close()
                return imdbID
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getIMDBMovieID(self, offset):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select  c.imdb_ref_id,c.id
                        from seo s
                                 join contents c on s.entity_id = c.id
                        order by s.popularity_score desc limit 1000 offset %s;
                  """
            self.cursor.execute(SQL, (offset,))
            imdbID = self.cursor.fetchall()
            if imdbID:
                self.conn.close()
                return imdbID
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def movieHavingAlreadyAward(self, imdbID, peopleImdbId, performanceAward, awardType, awardName, year, award_text):
        try:
            print(awardType)
            peopleImdbId = peopleImdbId.split('?')[0]
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """select id from crawling_awards where imdb_ref_id = %s  and 
                            performance_award =%s  and year=%s and awards_name=%s and award_text=%s"""
            SQL += """ and people_imdb_id like '%""" + str(peopleImdbId) + """%'"""
            self.cursor.execute(SQL, (imdbID, performanceAward, year, awardName, award_text))
            data = self.cursor.fetchone()
            self.conn.close()
            if data:
                return data[0]
            return False

        except Exception as error:
            print(error)

    def checkIfTriviaExistOrNot(self, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from content_trivia where content_id=%s """
            self.cursor.execute(SQL, (contentID,))
            existOrNot = self.cursor.fetchone()
            if existOrNot:
                self.conn.close()
                return True
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertIMDBTriviaData(self, imdbRefID, contentID, triviaData):
        try:
            notExistTrivia = self.checkIfTriviaExistOrNot(contentID)
            updatedAt = datetime.now()
            print(imdbRefID)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not notExistTrivia:
                for data in triviaData:
                    # SQL
                    SQL = """ insert into content_trivia(content_trivia, content_id, updated_at, status)
                            values(%s,%s,%s,%s)"""
                    self.cursor.execute(SQL, (data, contentID, updatedAt, 'publish'))
                    self.conn.commit()
            else:
                # SQL
                SQL = """ update content_trivia set updated_at=%s where content_id=%s"""
                self.cursor.execute(SQL, (updatedAt, contentID))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def checkIfIMDBQuotesExist(self, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from content_dialogue where content_id=%s """
            self.cursor.execute(SQL, (contentID,))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return True
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertIMDBQuotesData(self, imdbRefID, contentID, quotesData):
        try:
            updatedAt = datetime.now()
            print(imdbRefID)
            existIMDBQuotesData = self.checkIfIMDBQuotesExist(contentID)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existIMDBQuotesData:
                for data in quotesData:
                    print(data)
                    # SQL
                    SQL = """ insert into content_dialogue(content_dialogue, updated_at, content_id, status)
                                values(%s,%s,%s,%s)"""
                    self.cursor.execute(SQL, (data, updatedAt, contentID, 'publish'))
                    self.conn.commit()
            else:
                SQL = """ update content_dialogue set updated_at=%s where content_id=%s"""
                self.cursor.execute(SQL, (updatedAt, contentID))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    # ----------------------------------------------------- best similar movies --------------------------------------
    def getMovieContentID(self, movieName, movieYear, movieType=None):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from contents where original_title = %s and release_year = %s """
            if movieType:
                SQL += """  and content_type ='movie'  """
            self.cursor.execute(SQL, (movieName, movieYear))
            data = self.cursor.fetchone()
            if data:
                contentID = data[0]
                self.conn.close()
                return contentID
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkIfMovieExistInBestSimilar(self, movieUrl):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_best_similar_list_page where url = %s """
            self.cursor.execute(SQL, (movieUrl,))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return True
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertMovieDataIntoBestSimilar(self, movieName, movieUrl, contentID, updateAt):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ insert into crawling_best_similar_list_page(movieName, url, content_id, updated_at) 
                      values (%s,%s,%s,%s)    
                  """
            self.cursor.execute(SQL, (movieName, movieUrl, contentID, updateAt))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def getBestSimilarMoviesUrl(self):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """  select id,content_id,url from crawling_best_similar_list_page """
            self.cursor.execute(SQL)
            urls = self.cursor.fetchall()
            return urls if urls else False
        except Exception as error:
            print(error)

    def checkMoviesStoryAndStyle(self, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_best_similar_content where content_id=%s """
            self.cursor.execute(SQL, (contentID,))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertMoviesStoryAndStyle(self, contentID, story, style, updatedAt):
        try:
            if not self.checkMoviesStoryAndStyle(contentID):
                self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                # SQL
                SQL = """ insert into crawling_best_similar_content(content_id, story, style, updated_at) 
                          values(%s,%s,%s,%s) 
                      """
                self.cursor.execute(SQL, (contentID, story, style, updatedAt))
                self.conn.commit()
                self.conn.close()
        except Exception as error:
            print(error)

    def checkIfMovieIDAndContentIDExist(self, movieID, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_best_similar_movies where content_id=%s 
                      and content_id_of_similar_movies = %s
                  """
            self.cursor.execute(SQL, (movieID, contentID))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertMostSimilarMovies(self, contentID, movieSimilarContentID, updatedAt, similarity, count):
        try:
            if not self.checkIfMovieIDAndContentIDExist(contentID, movieSimilarContentID):
                self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                # SQL
                SQL = """ insert into crawling_best_similar_movies(content_id, content_id_of_similar_movies, 
                           updated_at, similarity) values (%s,%s,%s,%s)
                  """
                self.cursor.execute(SQL,
                                    (contentID, movieSimilarContentID, updatedAt, similarity[count].replace('%', '')))
                self.conn.commit()
                self.conn.close()
        except Exception as error:
            print(error)

    # ----------------------------------------------------digitBinge--------------------------------------

    def checkIfDigitBingeMovieAlreadyExistORNot(self, movieName, Year):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_digit_binge_movies where movie_name=%s and release_year=%s"""
            self.cursor.execute(SQL, (movieName, Year))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertDigitBingeMovies(self, movieName, movieType, releaseYear, whereToWatch, genreType):
        try:
            existID = self.checkIfDigitBingeMovieAlreadyExistORNot(movieName, releaseYear)
            updated_at = datetime.now()
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existID:
                # SQL
                SQL = """insert into crawling_digit_binge_movies(movie_name, release_year, movie_type, where_to_watch, 
                            updated_at, genreType)values(%s,%s,%s,%s,%s,%s)"""
                self.cursor.execute(SQL, (movieName, releaseYear, movieType, whereToWatch, updated_at, genreType))
                self.conn.commit()
            else:
                SQL = """ update crawling_digit_binge_movies set updated_at=%s where id=%s """
                self.cursor.execute(SQL, (updated_at, existID))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    # -------------------------------------- amazon prime --------------------------------------------

    def checkIfMovieExistForOTT(self, contentID, seasonNumber, providerName):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from ott_upcoming_latest_shows_movies where content_id=%s and provider_name =%s"""
            if seasonNumber:
                SQL += """ and seasonNumber=""" + str(seasonNumber)
            self.cursor.execute(SQL, (contentID, providerName))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertOttMoviesAndShows(self, movieName, primeUrl, releaseYear, imdbRating,
                                contentType, contentID, source, seasonNumber, ottReleaseDate, sourceProviderUrl,
                                providerName):
        try:
            updatedAt = datetime.now()
            existMovieID = self.checkIfMovieExistForOTT(contentID, seasonNumber, providerName)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existMovieID:
                # SQL
                SQL = """ insert into ott_upcoming_latest_shows_movies(movie_name, source, url, released_year, imdb_rating, 
                                    content_type, content_id, seasonNumber, ott_release_date, providerUrl, provider_name)
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                     """
                self.cursor.execute(SQL, (movieName, source, primeUrl, releaseYear, imdbRating, contentType, contentID,
                                          seasonNumber, ottReleaseDate, sourceProviderUrl, providerName))
                self.conn.commit()
            else:
                SQL = """ update ott_upcoming_latest_shows_movies set updated_at=%s where id=%s"""
                self.cursor.execute(SQL, (updatedAt, existMovieID))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def checkIfMovieOrShowExistOrNotForOTTJustWatch(self, contentID, contentType,
                                                    movieOrShowName, justWatchUrl,
                                                    releaseYear, seasonNumber):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_just_watch_upcoming_shows_movies where movie_name=%s 
                                                                                     and just_watch_url=%s 
                                                                                     and released_year=%s 
                                                                                     and content_type=%s
                                                                                     and content_id=%s  
                                                                                     
                """
            if contentType == 'web':
                contentType = 'show'
                SQL += """ seasonNumber=""" + str(seasonNumber)
            self.cursor.execute(SQL, (movieOrShowName, justWatchUrl, releaseYear, contentType, contentID))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertIntoJustWatchComingTodayForOTT(self, movieOrShowName, justWatchID, justWatchUrl,
                                             providerUrl, imdbScore, seasonNumber, releaseYear,
                                             contentType, source, contentID, tmdbRefID, dateRange):
        try:
            updatedAt = datetime.now()
            existID = self.checkIfMovieOrShowExistOrNotForOTTJustWatch(contentID, contentType,
                                                                       movieOrShowName, justWatchUrl,
                                                                       releaseYear, seasonNumber
                                                                       )
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existID:
                # SQL
                SQL = """ insert into crawling_just_watch_upcoming_shows_movies(movie_name, source, just_watch_url, 
                                provider_url, released_year, imdb_rating, content_type, content_id, just_watch_id, 
                                seasonNumber, tmdb_ref_id, ott_release_date)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """
                self.cursor.execute(SQL, (movieOrShowName, source, justWatchUrl, providerUrl, releaseYear, imdbScore,
                                          contentType, contentID, justWatchID, seasonNumber, tmdbRefID, dateRange))
                self.conn.commit()
            else:
                # SQL
                SQL = """ update crawling_just_watch_upcoming_shows_movies set source=%s,provider_url=%s,imdb_rating=%s
                            , updated_at=%s where id=%s
                      """
                self.cursor.execute(SQL, (source, providerUrl, imdbScore, updatedAt, existID))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def getContentOTTMoviesOrShowOfNewReleaseFromJustWatch(self):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """ select content_type, content_id, just_watch_id, seasonNumber,tmdb_ref_id from 
                                        crawling_just_watch_upcoming_shows_movies 
                  """
            self.cursor.execute(SQL)
            data = self.cursor.fetchall()
            if data:
                self.conn.close()
                return data
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getTmdbRefIDByContentID(self, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select tmdb_ref_id from contents where id =%s """
            self.cursor.execute(SQL, (contentID,))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getTmdbRefIDByContentTypeAndTitle(self, contentType, title, year):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select tmdb_ref_id,id from contents where content_type=%s and title=%s and release_year=%s"""
            self.cursor.execute(SQL, (contentType, title, year))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0], data[1]
            self.conn.close()
            return False, False
        except Exception as error:
            print(error)

    def verifyOTTReleaseDate(self, ottReleaseDate, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select ott_release_date from contents where  id=%s"""
            self.cursor.execute(SQL, (contentID,))
            data = self.cursor.fetchone()
            self.conn.close()
            print(data[0])
            if str(data[0]) == str(ottReleaseDate):
                return data[0], True
            return data[0], False
        except Exception as error:
            print(error)

    def updateOTTReleaseDateByContentID(self, ottReleaseDate, contentID):
        try:
            existOTTReleaseDate, dateFlag = self.verifyOTTReleaseDate(ottReleaseDate, contentID)
            if not dateFlag:
                self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                # SQL
                SQL = """ update contents set ott_release_date=%s where id=%s"""
                self.cursor.execute(SQL, (ottReleaseDate, contentID))
                self.conn.commit()
                self.conn.close()
                return [str(ottReleaseDate), str(existOTTReleaseDate)]
        except Exception as error:
            print(error)

    def getStreamProviderName(self, websiteUrl):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select provider from streaming_provider  """
            SQL += """ where official_website like '%""" + str(websiteUrl) + """%' """
            self.cursor.execute(SQL)
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkIfImdbAwardsIdExistOrNot(self, year, imdbID, awardType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from crawling_imdb_awards_year_wise where  imdb_id=%s and year=%s  and award_type=%s"""
            self.cursor.execute(SQL, (imdbID, year, awardType))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertImdbAwardsIDByYear(self, imdbID, year, awardType):
        try:
            imdbIDExist = self.checkIfImdbAwardsIdExistOrNot(year, imdbID, awardType)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            if not imdbIDExist:
                SQL = """ insert into crawling_imdb_awards_year_wise(imdb_id, year, award_type)values(%s,%s,%s)"""
                self.cursor.execute(SQL, (imdbID, year, awardType))
                self.conn.commit()
            else:
                SQL = """ update crawling_imdb_awards_year_wise set updated_at=%s where id=%s"""
                self.cursor.execute(SQL, (datetime.now(), imdbIDExist))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def getRecentlyReleaseMoviesImdbID(self, offset):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select imdb_ref_id, id
                            from contents
                            where release_date <= cast(now() as date)
                              and release_date >= DATE_SUB(cast(now() as date), INTERVAL 3 DAY) limit 100 offset %s
                  """
            self.cursor.execute(SQL, (offset,))
            data = self.cursor.fetchall()
            if data:
                self.conn.close()
                return data
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getJustWatchPopularIMDBID(self):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ 
                        select imdb_id, movieType
                            from just_watch_crawlers
                            where  imdb_id is not null and latest_crawl=1
                            order by just_watch_popular_id limit 501;

                  """
            self.cursor.execute(SQL)
            data = self.cursor.fetchall()
            if data:
                self.conn.close()
                return data
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getContentIDByImdbID(self, imdbRefID, contentType=None):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from contents """
            SQL += """ where   imdb_ref_id='""" + str(imdbRefID) + """'"""
            self.cursor.execute(SQL)
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkIfAlreadyExistSimilarContent(self, contentID, relatedContentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from content_similar_to where content_id=%s and related_content_id=%s """
            self.cursor.execute(SQL, (contentID, relatedContentID))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertSimilarMoviesShowsToContentSimilar(self, contentId, relatedContentIDs, source):
        try:
            for relatedContentID in relatedContentIDs:
                try:
                    similarID = self.checkIfAlreadyExistSimilarContent(contentId, relatedContentID[0])
                    self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
                    # SQL
                    if not similarID:
                        SQL = """ 
                        insert into content_similar_to(content_id, related_content_id, source ,updated_at, priority)
                                    values(%s,%s,%s,%s,%s)
                              """
                        self.cursor.execute(SQL, (contentId, relatedContentID[0], source, datetime.now(),
                                                  relatedContentID[1]))
                        self.conn.commit()
                    else:
                        SQL = """ update content_similar_to set updated_at=%s, source=%s, priority=%s where id =%s"""
                        self.cursor.execute(SQL, (datetime.now(), source, relatedContentID[1], similarID))
                        self.conn.commit()
                    self.conn.close()
                except Exception as error:
                    print(error)
        except Exception as error:
            print(error)

    def checkIfContentQueueIDExistOrNot(self, contentID, contentType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select id from content_queue_temp where content_id=%s """
            if contentType:
                SQL += """ and content_type='""" + str(contentType) + """'"""
            self.cursor.execute(SQL, (contentID,))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def insertContentIDIntoQueue(self, contentID, contentType=None):
        try:
            print(contentID)
            existID = self.checkIfContentQueueIDExistOrNot(contentID, contentType)
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            if not existID:
                # SQL
                SQL = """ insert into content_queue_temp(content_id, is_updated, updated_at,content_type) 
                                    values(%s,%s,%s,%s)
                      """
                self.cursor.execute(SQL, (contentID, 0, datetime.now(), contentType))
                self.conn.commit()
            else:
                # SQL
                SQL = """ update content_queue_temp set is_updated=%s,updated_at=%s where id=%s"""
                print(existID)
                self.cursor.execute(SQL, (0, datetime.now(), existID))
                self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)

    def getContentTypeAndTmdbID(self, contentID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select content_type, tmdb_ref_id  from contents where id=%s"""
            self.cursor.execute(SQL, (contentID,))
            data = self.cursor.fetchone()
            if data[0]:
                self.conn.close()
                return data[1], data[0]
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkIfOffersUlrExistOrNot(self, tmdbID, contentType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            # SQL
            SQL = """ select just_watch_movie_id, tmdb_ref_id,content_type,just_watch_url  
                        from crawling_just_watch_list_page where tmdb_ref_id=%s and content_type=%s
                  """
            self.cursor.execute(SQL, (tmdbID, contentType))
            data = self.cursor.fetchall()
            if data:
                self.conn.close()
                return data
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def getTmdbID(self, contentType):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """ select id, tmdb_ref_id from contents where tmdb_ref_id is not null and content_type=%s"""
            self.cursor.execute(SQL, (contentType,))
            data = self.cursor.fetchall()
            self.conn.close()
            return data
        except Exception as error:
            print(error)

    def checkIfAlreadyPeopleExistInPeople(self, cur, peopleTmdb):
        try:
            SQL = """ select id from people where tmdb_reference_id=%s"""
            cur.execute(SQL, (peopleTmdb,))
            data = cur.fetchone()
            if data:
                return data['id']
            return False
        except Exception as error:
            print(error)

    def insertCastAndCrew(self, castAndCrewDetails):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            peopleExistOrNot = self.checkIfAlreadyPeopleExistInPeople(self.cursor, castAndCrewDetails['peopleTmdbID'])
            if not peopleExistOrNot:
                SQL = """ insert into people(first_name,
                               last_name,
                               real_name,
                               status,
                               tmdb_reference_id,
                               imdb_ref_id,
                               popularity,
                               imdb_rating,
                               profile_path,updated_at) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            
                      """
                self.cursor.execute(SQL, (castAndCrewDetails['firstName'], castAndCrewDetails['lastName'],
                                          castAndCrewDetails['realName'], 'publish', castAndCrewDetails['peopleTmdbID'],
                                          None, castAndCrewDetails['popularity'], None,
                                          castAndCrewDetails['profile_path'],
                                          datetime.now()
                                          ))
                self.conn.commit()
                self.conn.close()
                return self.cursor.lastrowid
            SQL = """ update people set updated_at=now() where id=%s"""
            self.cursor.execute(SQL, (peopleExistOrNot,))
            self.conn.commit()
            self.conn.close()
            return peopleExistOrNot
        except Exception as error:
            print(error)

    def checkIfPeopleRoleExistOrNot(self, cur, contentID, sessionID, personID, character):
        try:
            SQL = """ select id from content_role_people where content_id=%s and 
                                                               person_id=%s and 
                                                               character_name=%s
                  """
            if character is None:
                character = ''
            if sessionID:
                SQL += """ and season_id=%s""" + str(sessionID)
            cur.execute(SQL, (contentID, personID, str(character).strip()))
            data = cur.fetchone()
            if data:
                return data['id']
            return False
        except Exception as error:
            print(error)

    def insertIntoContentRole(self, contentID, sessionID, personID, roleID,
                              createdAt, updatedAt, order, character,
                              status, peopleImdbID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            peopleID = self.checkIfPeopleRoleExistOrNot(self.cursor, contentID, sessionID, personID, character)
            if not peopleID:
                SQL = """ 
                        insert into content_role_people(content_id, season_id, person_id, role_id, created_at, updated_at, people_order,
                                    character_name, status, people_tmdb_ref_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    
                      """
                self.cursor.execute(SQL,
                                    (contentID, sessionID, personID, roleID, createdAt, updatedAt, order, character,
                                     status, peopleImdbID))
                self.conn.commit()
                return self.cursor.lastrowid
            SQL = """ update content_role_people set updated_at=now() where id=%s"""
            self.cursor.execute(SQL, (peopleID,))
            self.conn.commit()
            self.conn.close()
            return peopleID
        except Exception as error:
            print(error)

    def getPeopleRole(self, roleName):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """ select id from roles where name =%s"""
            self.cursor.execute(SQL, (roleName,))
            roleID = self.cursor.fetchone()
            if roleID:
                self.conn.close()
                return roleID['id']
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def checkIfContentImagesAlreadyExist(self, contentID, sessionID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """ select id from content_images_mapping  where content_id=%s"""
            if sessionID:
                SQL += """ and season_id= """+str(sessionID)
            self.cursor.execute(SQL, (contentID,))
            data = self.cursor.fetchone()
            if data:
                self.conn.close()
                return True
            self.conn.close()
            return False
        except Exception as error:
            print(error)

    def updateImagesOfContents(self, imagePath, backdropsID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """ insert into images(image_type, image_url, updated_at, status) values(%s,%s,%s,%s)"""
            self.cursor.execute(SQL, (backdropsID, imagePath, datetime.now(), 'publish'))
            self.conn.commit()
            imageID = self.cursor.lastrowid
            self.conn.close()
            return imageID
        except Exception as error:
            print(error)

    def updateImagesIntoContentImageMapping(self, imageID, contentID, sessionID):
        try:
            self.conn, self.cursor = self.dbConnectionObj.dBConnectionForStreamA2Z()
            SQL = """ insert into content_images_mapping(image_id, content_id, season_id,
                                 updated_at, status) values(%s,%s,%s,%s,%s)
                  """
            self.cursor.execute(SQL, (imageID, contentID, sessionID, datetime.now(), 'publish'))
            self.conn.commit()
            self.conn.close()
        except Exception as error:
            print(error)
