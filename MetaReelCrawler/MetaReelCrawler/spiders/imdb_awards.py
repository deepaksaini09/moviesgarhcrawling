import random

from scrapy.selector import Selector
import scrapy
import sys
from datetime import datetime
from scrapy.http import Request
from common_services import commonServices
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from db_services import dBServices
import time

dBServiceObj = dBServices()


class QuotesSpider(scrapy.Spider):
    name = "imdb_awards_test"

    def __init__(self, offset, allDataOrLatestYear, **kwargs):
        super().__init__(**kwargs)
        self.commonService = commonServices()
        self.moviesUrl = [
            # "https://www.imdb.com/title/tt7405458/awards/",
            # "https://www.imdb.com/title/tt7405458/awards/",
            # "https://www.imdb.com/title/tt0041380/awards/",
            # "https://www.imdb.com/title/tt0059512/awards/",
            # "https://www.imdb.com/title/tt0060479/awards/",
            # "https://www.imdb.com/title/tt0083144/awards/",
        ]
        self.offset = offset
        self.latestYearOrNot = allDataOrLatestYear

    def start_requests(self):
        while True:
            user_agents = random.choice(self.commonService.userAgentsList())
            headers = {'USER_AGENT': user_agents}

            username = "lum-customer-hl_171c206b-zone-capchacrawling"
            password = "2950px18mfqt"
            port = 22225

            session_id = random.random()
            super_proxy_url = ('http://%s-session-%s:%s@zproxy.luminati.io:%d' %
                               (username, session_id, password, port))

            if self.latestYearOrNot:
                imdbMovieID = dBServiceObj.getIMDBMovieIDOfLatestYear(self.offset)
            else:
                imdbMovieID = dBServiceObj.getIMDBMovieID(self.offset)
            self.offset += 1000
            for imdbID in imdbMovieID:
                request = Request('https://www.imdb.com/title/{id}/awards/'.format(id=imdbID[0]), callback=self.parse,
                                  headers=headers, dont_filter=True)
                request.meta['imdbId'] = imdbID[0]
                request.meta['proxy'] = "{super_proxy_url}".format(super_proxy_url=super_proxy_url)
                yield request
            if not imdbMovieID:
                break

    def parse(self, response, **kwargs):
        # print('\n\n\n\n\n _____ \n ')
        # print(response)
        hxs = response
        imdb_ref_id = response.meta['imdbId']

        awards_path = hxs.xpath('//section[contains(@class,"ipc-page-section ipc-page-section--base")]')
        # print(awards_path)
        total_awards = len(awards_path) - 2
        # print(total_awards)
        for index in range(total_awards):
            # h3 = awards_path[index].xpath('.//h3/span/text()')
            try:
                awardName = commonServices.checkIfItemIsListType(awards_path[index].xpath('.//span/text()').extract(),
                                                                 0)
                if awardName == "Contribute to this page":
                    break
                # print(awardName)

                awards_list = awards_path[index].xpath('.//ul[contains(@class,"meta-data-award-list" )]/li')
                # print(awards_list)
                for award_section in awards_list:
                    try:
                        year_awardType = commonServices.checkIfItemIsListType(
                            award_section.xpath('.//a/text()').extract(),
                            0)
                        # print(year_awardType)
                        year, awardType = year_awardType.split(' ')

                        award_text = commonServices.checkIfItemIsListType(
                            award_section.xpath('.//a/span/text()').extract(),
                            0)
                        performance_award = commonServices.checkIfItemIsListType(
                            award_section.xpath('.//li[contains(@role,"presentation" )]//span/text()').extract(),
                            0)  # list of awards
                        peoples = award_section.xpath(
                            './/li[contains(@role,"presentation" )]//a/text()').extract()  # list of people

                        people_imdb_link = award_section.xpath(
                            './/li[contains(@role,"presentation" )]//a/@href').extract()  # list of people

                        people_imdb_id_mapping = {}
                        people_imdb_id = None
                        if people_imdb_link:
                            for people, person_href in zip(peoples, people_imdb_link):
                                people_imdb_id = person_href.split('/')[2]
                                people_imdb_id_mapping[people] = people_imdb_id

                        if people_imdb_id_mapping:
                            for people in people_imdb_id_mapping:
                                dBServiceObj.insertDataIntoImdbRew(awardName, year, imdb_ref_id, people,
                                                                   people_imdb_id_mapping[people], performance_award,
                                                                   awardType, award_text)
                                # print(awardName, year, imdb_ref_id, people, people_imdb_id_mapping[people],
                                #       performance_award, awardType, award_text, )
                        else:
                            dBServiceObj.insertDataIntoImdbRew(awardName, year, imdb_ref_id, None, '',
                                                               performance_award,
                                                               awardType, award_text, )
                            # print(awardName, year, imdb_ref_id, None, '', performance_award, awardType, award_text, )
                    except Exception as error:
                        print(error)
            except Exception as error:
                print(error)
                return None


if __name__ == "__main__":
    print('startTime: ', datetime.now())
    args = sys.argv
    allDataOrLatestYear = None
    if len(args) >= 2:
        # for latest Year
        allDataOrLatestYear = True
    offset = 0
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl('imdb_awards_test', offset, allDataOrLatestYear)
    print('star process--')
    process.start(stop_after_crawl=True)
    print('endTime: ', datetime.now(), allDataOrLatestYear)
