import typing
import time
import requests
from urllib.parse import urljoin
import bs4
import pymongo
import datetime as dt


class GbBlogParse:
    headers = {
        "User-Agent": "BeregnyakDS"
    }
    __parse_time = 0

    def __init__(self, start_url, db, delay=1.0):
        self.start_url = start_url
        self.db = db
        self.delay = delay
        self.done_urls = set()
        self.tasks = []
        self.tasks_creator({self.start_url, }, self.parse_feed)

    def _get_response(self, url):
        while True:
            next_time = self.__parse_time + self.delay
            if next_time > time.time():
                time.sleep(next_time - time.time())
            response = requests.get(url, headers=self.headers)
            print(f"RESPONSE: {response.url}")
            self.__parse_time = time.time()
            if response.status_code == 200:
                return response

    #  метод, достающий response и складыающий его в callback
    def get_task(self, url: str, callback: typing.Callable) -> typing.Callable:
        def task():
            response = self._get_response(url)
            return callback(response)

        return task


    def tasks_creator(self, urls: set, callback: typing.Callable):
        urls_set = urls - self.done_urls
        for url in urls_set:
            self.tasks.append(
                self.get_task(url, callback)
            )
            self.done_urls.add(url)

    def run(self):
        while True:
            try:
                task = self.tasks.pop(0)
                task()
            except IndexError:
                break


    # метод , собирающий страницы с постами (пагинации)
    def parse_feed(self, response: requests.Response):
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        ul_pagination = soup.find('ul', attrs={"class": "gb__pagination"})
        pagination_links = set(
            urljoin(response.url, itm.attrs.get('href'))
            for itm in ul_pagination.find_all('a') if
            itm.attrs.get("href")
        )
        self.tasks_creator(pagination_links, self.parse_feed)
        post_wrapper = soup.find("div", attrs={"class": "post-items-wrapper"})
        self.tasks_creator(
            set(
                urljoin(response.url, itm.attrs.get('href'))
                for itm in post_wrapper.find_all("a", attrs={"class": "post-item__title"}) if
                itm.attrs.get("href")
            ),
            self.parse_post
        )

    '''Домашнее задание.  Собрать поля:
    url страницы материала +
    Заголовок материала +
    Первое изображение материала (Ссылка)+
    Дата публикации (в формате datetime) +
    имя автора материала  +
    ссылка на страницу автора материала +
    комментарии в виде (автор комментария и текст комментария)'''

    def get_comments(self, commentable_id):
        comments = []
        url_comments = f'https://gb.ru/api/v2/comments?commentable_id={commentable_id}&commentable_type=Post'
        for el in self._get_response(url_comments).json():
            comments_data={
                'author': el['user']['full_name'],
                'comment': el['comment']['body'],
            }
            comments.append(comments_data)

            return comments


    def parse_post(self, response: requests.Response):
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        author_name_tag = soup.find('div', attrs={"itemprop": "author"})
        created_date= soup.find('time', attrs = {'class': 'text-md'}).attrs['datetime']
        commentable_id = soup.find("comments").attrs.get("commentable-id")
        data = {
            'url': response.url,
            "title": soup.find('h1', attrs={'class': "blogpost-title"}).text,
            "author": {
                'url': urljoin(response.url, author_name_tag.parent.attrs["href"]),
                'name': author_name_tag.text,
            },
            'image': soup.find('img').attrs['src'],
            'created_at': dt.datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%S%z"),
            'comments': self.get_comments(commentable_id)
        }
        self._save(data)



    def _save(self, data: dict):
        collection = self.db["gb_blog_parse"]
        collection.insert_one(data)


if __name__ == '__main__':
    client_db = pymongo.MongoClient("mongodb://localhost:27017")
    db = client_db["gb_parse_18_05"]
    parser = GbBlogParse("https://gb.ru/posts", db)
    parser.run()