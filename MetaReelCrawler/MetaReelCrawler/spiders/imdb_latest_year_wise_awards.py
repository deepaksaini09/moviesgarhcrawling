# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from scrapy.selector import Selector
# from db_services import dBServices
#
# dBServiceObj = dBServices()
#
#
# class imdbAwardsYearWise:
#     def __init__(self):
#         pass
#
#     def getAllAwardsImdbID(self):
#         chrome_options = Options()
#         chrome_options.add_argument('--headless')
#         chrome_options.add_argument('--no-sandbox')
#         chrome_options.add_argument('--disable-dev-shm-usage')
#         chrome_options.add_argument("--window-size=1920,1080")
#         chrome_options.add_argument("enable-features=NetworkServiceInProcess")
#         driver = None
#         print(driver)
#         try:
#             chrome_path = '/Users/deepak/chromedriver/chromedriver'
#             driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chrome_path)
#             year = 2023
#             driver.get('https://www.imdb.com/event/ev0000003/{awardYear}/1/?ref_=acd_eh'.format(awardYear=year))
#             awardType = 'oscar'
#             htmlData = driver.page_source
#             resData = Selector(text=htmlData)
#             data = resData.xpath('//a[contains(@href,"title")]/@href').extract()
#             listData = [i.replace('/?ref_=ev_nom', ' ').replace('/title/', '') for i in data]
#             listData = list(set(listData))
#             for imdbID in listData:
#                 if imdbID.count('tt') and not imdbID.count('http'):
#                     dBServiceObj.insertImdbAwardsIDByYear(imdbID.strip(), year, awardType)
#
#         except Exception as error:
#             print(error)
#
#
# if __name__ == '__main__':
#     obj = imdbAwardsYearWise()
#     obj.getAllAwardsImdbID()
