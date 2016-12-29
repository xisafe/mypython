# encoding: utf-8
from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as s
from sqlalchemy.orm import sessionmaker
BaseModel = declarative_base()
engine =s.create_engine("mssql+pymssql://sa:!@#qweasdZXC@192.168.0.98\\SQLSERVERDB/cv_dw",deprecate_large_types=True)
Session = sessionmaker(bind=engine)
session=Session()
def init_db():
    BaseModel.metadata.create_all(engine)
def drop_db():
    BaseModel.metadata.drop_all(engine)
class User(BaseModel):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    name = Column(CHAR(30)) # or Column(String(30))
    #username = db.Column(db.String(64), unique=True, index=True)
    #role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))

init_db()
#session=Session()
#user = User(name='a')
#session.add(user)
#user = User(name='b')
#session.add(user)
#user = User(name='a')
#session.add(user)
#user = User()
#session.add(user)
#session.commit()
#declarative_base() 创建了一个 BaseModel 类，这个类的子类可以自动与一个表关联。
#以 User 类为例，它的 __tablename__ 属性就是数据库中该表的名称，它有 id 和 name 这两个字段，分别为整型和 30 个定长字符。Column 还有一些其他的参数，我就不解释了。
#最后，BaseModel.metadata.create_all(engine) 会找到 BaseModel 的所有子类，并在数据库中建立这些表；drop_all() 则是删除这些表。

