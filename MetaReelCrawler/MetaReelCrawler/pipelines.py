# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter

from MetaReelCrawler.MetaReelCrawler.items import MetareelcrawlerItem,PaginationUrlForSong
from db_services import dBServices


class MetareelcrawlerPipeline:
    def __init__(self):
        self.dBServiceObj = dBServices()

    def process_item(self, item, spider):
        if isinstance(item, MetareelcrawlerItem):
            self.dBServiceObj.insertGadget360SongData(item)
        elif isinstance(item, PaginationUrlForSong):
            self.dBServiceObj.insertPaginationUrl(item)
        return item


