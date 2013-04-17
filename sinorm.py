'''
Created on 2013-4-15

@author: RobinTang

This is Object Relation Mapping(ORM) Library

v1.1 2013-04-17

v1.0 2013-04-15
'''

import types

db = None   # database
cur = None  # cursor
mode_debug = False  # debug switch, every SQL will display if the value is True, otherwise nothing to display 
autocommit = True   # commit switch, if the value is True, the database will be commit after every modify operation 

__TYPE_SQLITE__ = 0
__TYPE_MYSQL__ = 1
__dbtype__ = 0

class Error(Exception):
    '''ORM Base Error'''
    def __init__(self, e):
        Exception.__init__(self, e)

def __ifornot__(e, t, f):
    '''A expression like C languege e?:t:f'''
    if e:
        return t
    else:
        return f

def __typemap__(v):
    '''Mapping Python type with Database type'''
    if type(v) is types.StringType:
        if len(v):
            return v
        else:
            return 'text'
    elif type(v) is types.FloatType:
        return __ifornot__(v, 'float not null', 'float')
    elif type(v) is types.IntType:
        return __ifornot__(v, 'int not null', 'int')
    elif type(v) is types.LongType:
        return __ifornot__(v, 'bigint not null', 'bigint')

def __literal__(v):
    '''Database literal'''
    if __dbtype__ == __TYPE_SQLITE__:
        return "'%s'"%v
    elif __dbtype__ == __TYPE_MYSQL__:
        return db.literal(v)

def __createconditions__(conditions, condtype):
    '''Create conditions by conditions and condition-type'''
    if conditions and type(conditions) is types.DictType:
        condtype = ' %s ' % condtype
        conditions = condtype.join(['`%s`=%s' % (k, __literal__(v)) for (k, v) in conditions.items()])
    if conditions:
        conditions = ' where %s' % conditions
    return conditions

def __checkdb__():
    '''This method use to check the db variable is valid'''
    global db, cur
    if not db:
        raise Error('The database connection is None. Please open a connection from SinORM before you user the modle')
    if not cur:
        cur = db.cursor()

def set_db(sdb):
    '''Set the database'''
    global db, cur, __dbtype__
    dbtype = str(type(sdb)).lower()
    if dbtype.find('sqlite')>=0:
        # sqlite
        if mode_debug:
            print 'sqlite'
        __dbtype__ = __TYPE_SQLITE__
    elif dbtype.find('mysql')>=0:
        # MySQL
        if mode_debug:
            print 'MySQL'
        __dbtype__ = __TYPE_MYSQL__
    else:
        raise Error('Unknown database type:%s'%dbtype)
    db = sdb
    cur = db.cursor()

def db_commit():
    '''Commit the database'''
    db.commit()

def exe_sql(sql, commit=False):
    '''Execute a SQL'''
    __checkdb__()
    if mode_debug:
        print sql
    res = cur.execute(sql)
    if commit:
        db_commit()
    return res

def get_objects_by_sql(sql):
    '''Get objects from table'''
    count = exe_sql(sql)
    if count:
        names = [x[0] for x in cur.description]
        res = []
        allrow = cur.fetchall()
        for row in allrow:
            obj = dict(zip(names, row))
            res.append(obj)
    else:
        res = []
    return res

def get_objects(table, columns='*', conditions='', condtype='and', limit='', order='', offset=''):
    '''Get objects from table'''
    if columns and type(columns) is types.ListType:
        columns = ','.join(['`' + v + '`' for v in columns])
    conditions = __createconditions__(conditions, condtype)
    if limit:
        limit = ' limit %s' % limit
    if order:
        order = ' order by %s' % order
    if offset:
        offset = ' offset %s' % offset
    sql = 'select %s from `%s` %s%s%s%s' % (columns, table, conditions, order, limit, offset)
    return get_objects_by_sql(sql)

def get_object(table, keyid, keyidname='id'):
    '''Get one object from table by keyid, keyid is primary-key value, keyidname is primary-key name, default it is "id"'''
    sql = 'select * from `%s` where %s=%s' % (table, keyidname, __literal__(keyid))
    objs = get_objects_by_sql(sql)
    if objs:
        return objs[0]
    else:
        return None

def set_objects(table, obj, conditions='', condtype='and'):
    '''Update objects to database by conditions'''
    conditions = __createconditions__(conditions, condtype)
    setsql = ','.join(['`%s`=%s' % (k, __literal__(v)) for (k, v) in obj.items()])
    sql = 'update `%s` set %s%s' % (table, setsql, conditions)
    return exe_sql(sql, autocommit)

def set_object(table, obj, keyid=0, keyidname='id'):
    '''Update a object to database'''
    objkeyid = None
    if obj.has_key(keyidname):
        objkeyid = obj[keyidname]
        if not keyid:
            keyid = obj[keyidname]
        del obj[keyidname]    
    conditions = '`%s`=%s' % (keyidname, __literal__(keyid))
    res = set_objects(table, obj, conditions)
    if objkeyid:
        obj[keyidname] = objkeyid
    return res

def del_objects(table, conditions='', condtype='and'):
    '''Delete objects to database by conditions'''
    conditions = __createconditions__(conditions, condtype)
    sql = 'delete from `%s`%s' % (table, conditions)
    return exe_sql(sql, autocommit)

def del_object(table, obj, keyid=0, keyidname='id'):
    '''Delete a object to database by keyid'''
    objkeyid = None
    if obj.has_key(keyidname):
        objkeyid = obj[keyidname]
        if not keyid:
            keyid = obj[keyidname]
        del obj[keyidname]    
    conditions = '`%s`=%s' % (keyidname, __literal__(keyid))
    res = del_objects(table, conditions)
    if objkeyid:
        obj[keyidname] = objkeyid
    return res

def add_object(table, obj):
    '''Add a object to table'''
    keys = ','.join(['`%s`' % k for k in obj.keys()])
    vals = ','.join([__literal__(v) for v in obj.values()])
    sql = 'insert into `%s`(%s) values(%s)' % (table, keys, vals)
    return exe_sql(sql, autocommit)    

def create_table(table, tplobj, keyidname='id', new=False):
    '''Create a table by a template object's property'''
    if new:
        # drop table before start to create
        sql = 'drop table if exists `%s`' % table
        exe_sql(sql)
    if not tplobj.has_key(keyidname):
        if __dbtype__ == __TYPE_SQLITE__:
            tplobj[keyidname] = 'integer not null'  # SQLite
        elif __dbtype__ == __TYPE_MYSQL__:
            tplobj[keyidname] = 'int not null auto_increment' # MySQL
    struct = ','.join(['`%s` %s' % (k, __typemap__(v)) for (k, v) in tplobj.items()])
    struct = '%s,%s' % (struct, 'primary key (`%s`)' % keyidname)
    sql = 'create table if not exists `%s`(%s)' % (table, struct)
    return exe_sql(sql, autocommit)

def reset_table(table):
    '''Clear all record by table name'''
    sql = 'truncate table `%s`' % table
    return exe_sql(sql, autocommit)

def drop_table(table):
    '''Drop the data and table'''
    sql = 'drop table `%s`' % table
    return exe_sql(sql, autocommit)



def __test__():
    # table name
    table = 't_students'
    
    # create the table
    create_table(table,
               {
                'name':'char(128) not null', # the type of field 'name' is char(128) not null
                'age':1, # the type of field 'age' is integer, and can not be null, because the value is not 0 
                'height':0.0, # the type of field 'height' is float, and can be null, because the value is 0
                'info':''   # the type of field 'info' is text, and can be null, because the value is a empty string
    }, new = True)
    
    # add three object to the table
    add_object(table, {'name':'Tom', 'age':22})
    add_object(table, {'name':'Jack', 'age':21, 'height':171.2})
    add_object(table, {'name':'Robin', 'age':23, 'info': 'A Python Programmer'})
    
    # commit database
    db_commit()
    
    # get all objects from database
    students = get_objects(table)
    print students
    
    # get object by condition, where name='Robin'. Of course, you can use such as: conditions="name='Robin'"
    robin = get_objects(table, conditions={'name':'Robin'}, limit=1)[0]
    print robin
    
    # change the object's attribute value
    robin['name'] = 'RobinTang' # set new name for robin
    set_object(table, robin) # save to database
    
    # commit database
    db_commit()
    
    # get robin from database
    robin2 = get_object(table, robin['id'])
    print robin2
    
    # delete robin
    del_object(table, robin2)
    students = get_objects(table)
    print students
        
    # commit database
    db_commit()
    
    
    # get the count
    print get_objects(table, columns='count(*) as count')[0]['count']

if __name__ == '__main__':
    mode_debug = True
    autocommit = False
    
    # Test MySQL
    print '---Start Test MySQL---'
    try:
        import MySQLdb
        db = MySQLdb.connect(host='127.0.0.1', user='trb', passwd='123', db='dbp', port=3306)
        set_db(db)
        __test__()
    except:
        print '****Test MySQL Fail****'
    print '---End Test MySQL---'
    
    
    # Test sqlite
    print '---Start Test sqlite---'
    try:
        import sqlite3
        db = sqlite3.connect('sqlite.db')
        set_db(db)
        __test__()
    except:
        print '****Test sqlite Fail****'
    print '---End Test sqlite---'
    
    

    
    
    
