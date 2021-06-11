import scrapy


class JobparserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
from urllib.parse import urljoin
from scrapy import Selector
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose, Join


def employer_url(employer_id):
    return urljoin("https://hh.ru/", employer_id)

def clean_output(item):
    return item.replace("\xa0", " ")

class VacancyLoader(ItemLoader):
    default_item_class = dict
    item_type_out = TakeFirst()
    url_out = TakeFirst()
    title_out = TakeFirst()
    salary_in = MapCompose(clean_output)
    salary_out = Join('')
    description_in = MapCompose(clean_output)
    description_out = Join(' ')
    skills_in = MapCompose(clean_output)
    employer_url_in = MapCompose(employer_url)
    employer_url_out = TakeFirst()

    #Добавили item_type, чтобы через pipeline сохранять вакансии и работодателей в разные коллекции
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.context.get("response"):
            self.add_value("url", self.context["response"].url)
        self.add_value("item_type", "vacancy")

class EmployerLoader(ItemLoader):
    default_item_class = dict
    item_type_out = TakeFirst()
    name_in = MapCompose(clean_output)
    site_out = TakeFirst()
    business_fields_out = MapCompose(lambda fields: [field.capitalize() for field in fields.split(', ')])
    description_in = MapCompose(clean_output)
    description_out = Join('')
    url_out = TakeFirst()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.context.get("response"):
            self.add_value("url", self.context["response"].url)
        self.add_value("item_type", "company")
 11  jobparser/main.py
@@ -0,0 +1,11 @@
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from jobparser.spiders.hhru import HhruSpider


if __name__ == '__main__':
    crawler_settings = Settings()
    crawler_settings.setmodule('jobparser.settings')
    crawler_process = CrawlerProcess(settings=crawler_settings)
    crawler_process.crawl(HhruSpider)
    crawler_process.start()
# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class JobparserSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class JobparserDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from .settings import BOT_NAME
from pymongo import MongoClient


class JobparserPipeline:
    def process_item(self, item, spider):
        return item

class SaveToMongoPipeline:
    def __init__(self):
        client = MongoClient()
        self.db = client[BOT_NAME]

    def process_item(self, item, spider):
        collection_name = f"{spider.name}_{item.get('item_type', '')}"
        self.db[collection_name].insert_one(item)
        return item
BOT_NAME = 'jobparser'

SPIDER_MODULES = ['jobparser.spiders']
NEWSPIDER_MODULE = 'jobparser.spiders'

LOG_ENABLE = True
LOG_LEVEL = 'DEBUG'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; rv:78.0) Gecko/20100101 Firefox/78.0'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.5
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 32
CONCURRENT_REQUESTS_PER_IP = 32

# Disable cookies (enabled by default)
COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'jobparser.middlewares.HhRuSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'jobparser.middlewares.HhRuDownloaderMiddleware': 543,
# }

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'jobparser.pipelines.JobparserPipeline': 100,
    'jobparser.pipelines.SaveToMongoPipeline': 300,
}
import scrapy
from jobparser.loaders import VacancyLoader, EmployerLoader
from jobparser.spiders.xpath_selectors import PAGE_XPATH, VACANCY_DATA_XPATH, EMPLOYER_DATA_XPATH


from copy import copy
from urllib.parse import urlencode

class HhruSpider(scrapy.Spider):
    name = 'hhru'
    allowed_domains = ['hh.ru']
    start_urls = ['https://hh.ru/search/vacancy?schedule=remote&L_profession_id=0&area=113']
    api_vacancy_list_path = "/shards/employerview/vacancies"
    api_vacancy_list_params = {
        "page": 0,
        "currentEmployerId": None,
        "json": True,
        "regionType": "OTHER",
        "disableBrowserCache": True,
    }

    def _get_follow(self, response, xpath, callback):
        for a_link in response.xpath(xpath):
            yield response.follow(a_link, callback=callback)

    def parse(self, response):
        yield from self._get_follow(response, PAGE_XPATH['pagination_button'], self.parse)
        yield from self._get_follow(response, PAGE_XPATH['vacancy_page'], self.vacancy_parse)

    def vacancy_parse(self, response):
        loader = VacancyLoader(response=response)
        for key, xpath in VACANCY_DATA_XPATH.items():
            loader.add_xpath(key, xpath)
        data = loader.load_item()
        yield response.follow(data['employer_url'], callback=self.employer_parse)
        yield data

    def employer_parse(self, response):
        loader = EmployerLoader(response=response)
        for key, xpath in EMPLOYER_DATA_XPATH.items():
            loader.add_xpath(key, xpath)
        data = loader.load_item()

        #соберем список вакансий работодателя по api
        employer_id = response.url.split("/")[-1]
        params = copy(self.api_vacancy_list_params)
        params["currentEmployerId"] = employer_id
        yield response.follow(
            self.api_vacancy_list_path + "?" + urlencode(params),
            callback=self.api_vacancy_list_parse,
            cb_kwargs=params
        )
        yield data

    def api_vacancy_list_parse(self, response, **params):
        data = response.json()
        if data['@hasNextPage']:
            params["page"] += 1
            yield response.follow(
                self.api_vacancy_list_path + "?" + urlencode(params),
                callback=self.api_vacancy_list_parse,
                cb_kwargs=params
            )
        for vacancy in data['vacancies']:
            yield response.follow(
                vacancy["links"]["desktop"],
                callback=self.vacancy_parse
            )
