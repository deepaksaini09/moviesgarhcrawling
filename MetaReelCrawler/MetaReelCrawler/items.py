# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MetareelcrawlerItem(scrapy.Item):
    just_watch_genre_popular_id = scrapy.Field()
    just_watch_url = scrapy.Field()
    tmdb_url = scrapy.Field()
    tmdb_id = scrapy.Field()


class PaginationUrlForSong(scrapy.Item):
    url_type = scrapy.Field()
    url = scrapy.Field()
    released_type = scrapy.Field()
