
## 爬取网页数据
- https://zhuanlan.zhihu.com/p/33986020

```python
from bs4 import BeautifulSoup
import requests
import pandas as pd
url = 'https://segmentfault.com/a/1190000007688656'
res = requests.get(url)
soup = BeautifulSoup(res.text, 'lxml')
tables = soup.select('table')
df_list = []
for table in tables:
    df_list.append(pd.concat(pd.read_html(table.prettify())))
df = pd.concat(df_list)
df.to_excel('vscode快捷键大全.xlsx')
```
## 数据库读写
- https://segmentfault.com/a/1190000014210743
```python
# -*- coding: utf-8 -*-

# 导入必要模块
import pandas as pd
from sqlalchemy import create_engine

# 初始化数据库连接，使用pymysql模块
# MySQL的用户：root, 密码:147369, 端口：3306,数据库：mydb
engine = create_engine('mysql+pymysql://root:147369@localhost:3306/mydb')

# 查询语句，选出employee表中的所有数据
sql = '''
      select * from employee;
      '''

# read_sql_query的两个参数: sql语句， 数据库连接
df = pd.read_sql_query(sql, engine)

# 输出employee表的查询结果
print(df)

# 新建pandas中的DataFrame, 只有id,num两列
df = pd.DataFrame({'id':[1,2,3,4],'num':[12,34,56,89]})

# 将新建的DataFrame储存为MySQL中的数据表，不储存index列
df.to_sql('mydf', engine, index= False)

print('Read from and write to Mysql table successfully!')
```

```python
# -*- coding: utf-8 -*-

# 导入必要模块
import pandas as pd
from sqlalchemy import create_engine

# 初始化数据库连接，使用pymysql模块
engine = create_engine('mysql+pymysql://root:147369@localhost:3306/mydb')

# 读取本地CSV文件
df = pd.read_csv("E://mpg.csv", sep=',')

# 将新建的DataFrame储存为MySQL中的数据表，不储存index列
df.to_sql('mpg', engine, index= False)

print("Write to MySQL successfully!")
```
## 强制数据库数据类型

### SQLAlchemy Column type
- https://stackoverflow.com/questions/30137806/where-can-i-find-a-list-of-the-flask-sqlalchemy-column-types-and-options
### pandas to_sql all columns as nvarchar
- https://stackoverflow.com/questions/34383000/pandas-to-sql-all-columns-as-nvarchar
- You can create this dict dynamically if you do not know the column names in advance:
```
from sqlalchemy.types import NVARCHAR
df.to_sql(...., dtype={col_name: NVARCHAR for col_name in df})
```
- Note that you have to pass the sqlalchemy type object itself (or an instance to specify parameters like NVARCHAR(length=10)) and not a string as in your example.