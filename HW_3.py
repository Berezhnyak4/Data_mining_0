from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from . import models

class Database:
    def __init__(self, db_url):
        engine = create_engine(db_url)
        models.Base.metadata.create_all(bind=engine)
        self.maker = sessionmaker(bind=engine)

    def get_or_create(self, session, model, **data):
        # if type(model) == models.Comment:
        #     print("model here")
        #     return model(**data)
        db_data = session.query(model).filter(model.url == data["url"]).first()
        if not db_data:
            db_data = model(**data)
        return db_data

    def create_post(self, data):
        session = self.maker()
        author = self.get_or_create(session, models.Author, **data["author"])
        post = self.get_or_create(session, models.Post, **data["post_data"], author=author)
        tags = map(
            lambda tag_data: self.get_or_create(session, models.Tag, **tag_data), data["tags"]
        )
        comments = map(
            lambda comments_data: self.get_or_create(session, models.Comment, **comments_data), data["comments"]
        )

        post.tags.extend(tags)
        post.comments.extend(comments)
        session.add(post)
        try:
            session.commit()
            print("commited")  # deb
        except Exception:
            session.rollback()
        finally:
            session.close()


from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import relationship

from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    ForeignKey,
    DateTime
)

# связующий класс Base. От него наследуются все классы (таблицы), относящиеся к этой базе данных!
Base = declarative_base()

#Создадим классы Миксины - Id, Url, Name

class IdMixin():
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

class UrlMixin():
    url = Column(String(2048), nullable = False, unique = True)

class NameMixin():
    name = Column(String(200), nullable = False, unique = False)

#Классы (таблицы), относящиеся к базе данных Base (наследуются от него)

#Вспомогательный класс для реализации связи many to many между постами и тегами
#т.к. одни и те же теги могут быть в разных постах

posts_tags = Table(
    'posts_tags',
    Base.metadata,
    Column('post_id', Integer, ForeignKey('post.id')),
    Column('tag_id', Integer, ForeignKey('tag.id')))

class Post(Base, IdMixin, UrlMixin):
    __tablename__ = 'post'
    title = Column(String, nullable=False, unique=False)
    author_id = Column(Integer, ForeignKey("author.id"), nullable=False)
    image = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, nullable=False)
    author = relationship("Author", backref="posts")
    tags = relationship("Tag", secondary=posts_tags)


class Author(Base, IdMixin, UrlMixin, NameMixin):
    __tablename__ = 'author'

class Comments(Base, IdMixin, UrlMixin):
    __tablename__ = 'comments'
    text = Column(String(2048))
    created_at = Column(DateTime, )
    likes_count = Column(Integer)
    Column(DateTime, nullable=False)
    # ранее выводили отдельно имя и фамилию автора, сейчас соединяем с автором внешним ключом:
    author_id = Column(Integer, ForeignKey("author.id"), nullable = False)
    author = relationship("Author", backref="comments")
    post_id = Column(Integer, ForeignKey("post.id"), nullable = False)
    post = relationship("Post", backref = "comments")
    parent_comment_id = Column(Integer, ForeignKey("comments.id"), nullable = True)

class Tag(Base, IdMixin, UrlMixin, NameMixin):
    __tablename__ = 'tag'
    posts = relationship("Post", secondary = posts_tags)


import typing
import time
import requests
from urllib.parse import urljoin
import bs4
import pymongo
import datetime as dt
from database.database import Database


class GbBlogParse:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:88.0) "
                      "Gecko/20100101 Firefox/88.0"
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

    def get_comments(self, commentable_id):
        comments = []
        url_comment = f'https://gb.ru/api/v2/comments?commentable_id={commentable_id}&commentable_type=Post'
        response = self._get_response(url_comment)
        data = response.json()
        for comment in data:
            info = self._get_details(comment["comment"])
            comments += info
        return comments

    def _get_details(self, comment):
        comment_id = comment["id"]
        text = comment["body"]
        user = comment['user']['full_name']
        created_at = comment["created_at"]
        likes_count = comment["likes_count"]
        comment_details = [{
            "comment_id": comment_id,
            "athors_full_name": user,
            "text": text,
            "time_created": created_at,
            "likes_count": likes_count
        }]
        # Если есть ответы к комментарию
        if comment["children"]:
            for child_comment in comment["children"]:
                comment_details += self._get_details(child_comment["comment"])
        return comment_details


    def parse_post(self, response: requests.Response):
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        author_name_tag = soup.find('div', attrs={"itemprop": "author"})
        created_date = soup.find('time', attrs={'class': 'text-md'}).attrs['datetime']
        commentable_id = soup.find("comments").attrs.get("commentable-id")
        data = {
            "post_data": {
                "url": response.url,
                "title": soup.find('h1', attrs={'class': "blogpost-title"}).text,
                "image": soup.find('img').attrs['src'],
                "created_at": dt.datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%S%z")
            },
            "comments_data": self.get_comments(commentable_id),
            "author": {
                "url": urljoin(response.url, author_name_tag.parent.attrs["href"]),
                "name": author_name_tag.text
            },
            "tags_data": [
                {"name": tag.text,
                 "url": urljoin(response.url, tag.attrs.get("href"))}
                for tag in soup.find_all("a", attrs={"class": "small"})
            ]
        }
        self._save(data)
    #print(1)



    def _save(self, data: dict):
        self.db.add_post(data)
        #print(1)

if __name__ == '__main__':
    #для MongoDB
    #client_db = pymongo.MongoClient("mongodb://localhost:27017")
    #db = client_db["gb_parse_18_05"]

    #для SQLite
    orm_database = Database("sqlite:///gb_blog_parse.db")
    parser = GbBlogParse("https://gb.ru/posts", orm_database)
    parser.run()