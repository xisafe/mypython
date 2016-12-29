# encoding: utf-8
from sqlalchemy import func, or_, not_

from test import SqlAlchemy_Calss as t

session=t.session
User=t.User
user =t. User(name='a')
t.session.add(user) # user = t.User(name='b')t.session.add(user)
user = t.User(name='华哥')
t.session.add(user)  # user = t.User() t.session.add(user)
#t.session.commit()
query = t.session.query(t.User)
#print query # 显示SQL 语句
# print query.statement # 同上
for user in query: # 遍历时查询
    print user.name
print query.all() # 返回的是一个类似列表的对象
#print query.first().name # 记录不存在时，first() 会返回 None #print query.one().name # 不存在，或有多行记录时会抛出异常
query2 = session.query(t.User.name)
print query2.all() # 每行是个元组
print query2.limit(1).all() # 最多返回 1 条记录 print query2.offset(1).all() # 从第 2 条记录开始返回

print query2.order_by(t.User.name).all()
print query2.order_by('name').all()
print query2.order_by(t.User.name.desc()).all()
print query2.order_by('name desc').all()
print session.query(t.User.id).order_by(t.User.name.desc(), t.User.id).all()

print query2.filter(t.User.id == 1).scalar() # 如果有记录，返回第一条记录的第一个元素
print session.query('id').select_from(t.User).filter('id = 1').scalar()
# print query2.filter(t.User.id > 1, t.User.name != 'a').scalar() # and
query3 = query2.filter(t.User.id > 1) # 多次拼接的 filter 也是 and
query3 = query3.filter(t.User.name != 'a')
# print query3.scalar()
print query2.filter(or_(t.User.id == 1, t.User.id == 2)).all() # or
print query2.filter(t.User.id.in_((1, 2))).all() # in

query4 = session.query(User.id)
print query4.filter(User.name == None).scalar()
print query4.filter('name is null').scalar()
print query4.filter(not_(User.name == None)).all() # not
print query4.filter(User.name != None).all()

print query4.count()
print session.query(func.count('*')).select_from(User).scalar()
print session.query(func.count('1')).select_from(User).scalar()
print session.query(func.count(User.id)).scalar()
print session.query(func.count('*')).filter(User.id > 0).scalar() # filter() 中包含 User，因此不需要指定表
print session.query(func.count('*')).filter(User.name == 'a').limit(1).scalar() == 1 # 可以用 limit() 限制 count() 的返回数
print session.query(func.sum(User.id)).scalar()
print session.query(func.now()).scalar() # func 后可以跟任意函数名，只要该数据库支持
print session.query(func.current_timestamp()).scalar()
# print session.query(func.md5(User.name)).filter(User.id == 1).scalar()

query.filter(User.id == 1).update({User.name: 'c'})
user = query.get(1)
print user.name

user.name = 'd'
session.flush() # 写数据库，但并不提交
print query.get(1).name

session.delete(user)
session.flush()
print query.get(1)

session.rollback()
print query.get(1).name
query.filter(t.User.id == 1).delete()
session.commit()
print query.get(1)

