from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
import pandas as pd
import numpy as np
import datetime
import time

import logging
from sqlalchemy import create_engine
from sqlalchemy.types import TEXT

import re
from imdb import IMDb, IMDbError
import random

# - 本项目作用是爬取 https://250.took.nl 页面IMDb 历史页面数据，并从imdb python 库中获取影片基本信息
# - 存入MySQL数据库和excel表中，数据库中包含2个表，imdb_history, movie_basic_info
# - 现已保存1996.04-2019.09年的数据

# The historical information of IMDb 250 comes from the website https://250.took.nl

url = 'https://250.took.nl/compare/full'

# The begin and end date
begin = datetime.date(1996,4,25)
end = datetime.date(2019,9,15)

# Database mysql
engine = create_engine('mysql+pymysql://root:123456@localhost:3306/mydb') # 数据库设置

# Log
logger = logging.getLogger('imdb_history')
logger.setLevel(logging.DEBUG)

# To log file
fh = logging.FileHandler('imdb_history.log')
fh.setLevel(logging.DEBUG)

# To console
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Format of the log
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

# 保存新电影的基本信息，从Python库 imdb中获取
def save_fresh_movies(movie_IDs_dic):
    fresh_IMDb_IDS = []
    fresh_Titles = []
    countries = []
    languages = []
    directors = []
    genres = []
    kinds = []
    movie_years = []

    for movie_ID in movie_IDs_dic.keys():
        if movie_IDs_dic.get(movie_ID) == 0:
            # get country, languages, directors, genres
            # create an instance of the IMDb class
            ia = IMDb()

            try:
                movie = ia.get_movie(movie_ID[2:]) # delete tt

                fresh_title = movie.get('title', 'None exist')

                if fresh_title != 'None exist':
                    fresh_Titles.append(fresh_title)
                    fresh_IMDb_IDS.append(movie_ID)

                    logger.info('Get new movie: ' + '(' + movie_ID + ') ' + fresh_title)

                    country = ','.join(movie.get('countries', ['None exist']))
                    countries.append(country)

                    language = ','.join(movie.get('languages', ['None exist']))
                    languages.append(language)

                    if 'directors' in movie.keys():
                        director = []
                        for person in movie.get('directors'):
                            director.append(person.get('name', ['None exist']))
                        directors.append(','.join(director))
                    else:
                        directors.append('None exist')

                    genre = ','.join(movie.get('genres', ['None exist']))
                    genres.append(genre)

                    kind = movie.get('kind', 'None exist')
                    kinds.append(kind)

                    movie_year = movie.get('year', 'None exist')
                    movie_years.append(movie_year)

            except IMDbError as e:
                logger.error(str(e))

    if len(fresh_IMDb_IDS)>0:
        df2 = pd.DataFrame(fresh_IMDb_IDS)
        df2['Title'] = np.array(fresh_Titles)
        df2['Country'] = np.array(countries)
        df2['Language'] = np.array(languages)
        df2['Director'] = np.array(directors)
        df2['Genre'] = np.array(genres)
        df2['kind'] = np.array(kinds)
        df2['Year'] = np.array(movie_years)

        try:
            df2.to_csv('movie_basic_info.csv', mode='a', header=0, index=0)
            df2.to_sql('movie_basic_info', con=engine, if_exists='append', index=True) # 存入数据库表 movie_basic_info 

            for fresh_IMDb_id in fresh_IMDb_IDS:
                movie_IDs_dic[fresh_IMDb_id] = 1
        except Exception as e:
            logger.error(str(e))

# 检查电影id列表中是否包含未存储基本信息的电影
def is_have_fresh_movie(movie_IDs_dic):
    for movie_ID in movie_IDs_dic.keys():
        if movie_IDs_dic.get(movie_ID) == 0:
            return True
    return False


# Get already stored data from database
# 从数据中获取已经存储的电影列表，以做断点继续爬取数据，而不必重新爬取，只需要修改起始日期即可
movie_IDs_dic={}
stored_movie_basic_info_df = pd.read_sql_table("movie_basic_info", engine, chunksize=50)
for chunk in stored_movie_basic_info_df:
    for movie_id in chunk['0']:
        movie_IDs_dic[movie_id]=1

i=0 # 日期计数
while i <=(end - begin).days+1:

    day = begin + datetime.timedelta(days=i)

    tables=[]
    try:

        # 爬取页面
        url = 'https://250.took.nl/history/' + str(day.year) + '/' + str(day.month) + '/' + str(day.day)+'/'+'full'

        # http_proxies=['http://167.99.114.215:8080',
        #               'http://167.71.242.25:80',
        #               'http://206.72.203.42:80',
        #               'http://144.217.85.164:80',
        #               'http://35.230.185.216:80',
        #               'http://68.188.59.198:80',
        #               'http://64.251.21.59:80',
        #               'http://18.218.5.0:80',
        #               'http://178.18.62.195:80']
        # 代理池，以防被网站拦截爬取信息
        https_proxies=['https://51.79.102.222:8080',
                       'https://144.217.74.219:3128',
                       'https://51.79.24.128:8080',
                       'https://51.79.25.162:8080',
                       'https://167.114.197.123:3128',
                       'https://54.39.53.104:3128',
                       'https://178.128.225.180:3128',
                       'https://167.71.97.196:3128'
                      ]

        # http_proxie = random.sample(http_proxies, 1)[0]
        https_proxie= random.sample(https_proxies, 1)[0]

        # logger.info('http_proxie:'+http_proxie+' '+'https_proxie:'+https_proxie)

        logger.info('https_proxie:' + https_proxie)
        proxies = {
            'https': https_proxie
        }

        with requests.Session() as s:
            s.mount('http://', HTTPAdapter(max_retries=10))
            s.mount('https://', HTTPAdapter(max_retries=10))
            s.keep_alive = False
            res = s.get(url,stream=False,timeout=10,proxies=proxies)

        soup = BeautifulSoup(res.text, 'lxml')
        tables = soup.select('table')

        logger.info(url)
    except Exception as e:
        logger.error(str(e))
        continue

    df1=pd.DataFrame()
    for table in tables:

        content=pd.concat(pd.read_html(table.prettify()))

        df_temp=pd.DataFrame(content)

        if df_temp.shape[0]>=250: # If the row number  >=250 , then it's the table of IMDb 250
            df1=df_temp
            logger.info("Get movie information "+str(day))

            links = table.findAll('a')
            movie_link_ids=[]
            imdb_pages=[]


            for l in links:
                if re.match('<a href="https://www.imdb.com/title/tt(.*?)', str(l)):
                    movie_ID = re.match('<a href="https://www.imdb.com/title/(.*?)"><img alt="IMDb"(.*?)', str(l)).group(1)

                    movie_link_ids.append(movie_ID)
                    imdb_pages.append('https://www.imdb.com/title/' + movie_ID)

                    if movie_IDs_dic.get(movie_ID) is None:
                        movie_IDs_dic[movie_ID]=0

            df1['Day'] = str(day)
            df1['MovieID']=np.array(movie_link_ids)
            df1['IMDbPage']=np.array(imdb_pages)
            df1=df1.drop(['Unnamed: 1','Unnamed: 5'],axis=1) # Delete useless column
            break

    # Save data to excel and Save data into database
    # 保存电影历史信息到excel表和存入数据库
    if len(df1)>0:
        try:
            df1.to_excel('excel/' + str(day) + '-imdb.xlsx', index=False, header=True)
            df1.to_sql('imdb_history', con=engine, if_exists='append', index=False,dtype={col_name: TEXT for col_name in df1})# To force columns type is Text 存入数据库表 imdb_history
            i+=1
        except Exception as e:
            logger.error(str(e))
    else:
        i+=1

    # save fresh movie
    # 保存电影基本信息
    save_fresh_movies(movie_IDs_dic)

# 如果电影id列表中还包含未存储过基本信息的电影就再查询存储一遍，以防有缺漏
while is_have_fresh_movie(movie_IDs_dic):
    save_fresh_movies(movie_IDs_dic)