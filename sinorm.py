'''
Created on 2013-4-15

@author: RobinTang

This is Object Relation Mapping(ORM) Library

v1.2
Add Postgre Database supporting, base on pgdb.

v1.1
Add sqlite supporting.

v 1.0
Only implemented some basic method to access database,
and no relationship be supported in this version.
Only tested on MySQL.
'''

import types

db = None   # database
cur = None  # cursor
mode_debug = False  # debug switch, every SQL will display if the value is True, otherwise nothing to display 
autocommit = True   # commit switch, if the value is True, the database will be commit after every modify operation 

__TYPE_SQLITE__ = 0
__TYPE_MYSQL__ = 1
__TYPE_PGDB__ = 2

__dbnames__ = ('sqlite', 'mysql', 'postgre')

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
    global __dbtype__
    isNone = type(v) is types.NoneType
    if __dbtype__ == __TYPE_SQLITE__:
        return "'%s'"%v
    elif __dbtype__ == __TYPE_MYSQL__:
        return db.literal(v)
    elif __dbtype__ == __TYPE_PGDB__:
        return __ifornot__(isNone, 'Null', "'%s'"%v)


def __nameliteral__(k):
    '''Database name literal'''
    global __dbtype__
    if __dbtype__ == __TYPE_SQLITE__:
        return '`%s`'%k
    elif __dbtype__ == __TYPE_MYSQL__:
        return '`%s`'%k
    elif __dbtype__ == __TYPE_PGDB__:
        return k

def __createconditions__(conditions, condtype):
    '''Create conditions by conditions and condition-type'''
    if conditions and type(conditions) is types.DictType:
        condtype = ' %s ' % condtype
        conditions = ' where %s'%condtype.join(['%s=%s' % (__nameliteral__(k), __literal__(v)) for (k, v) in conditions.items()])
    elif conditions:
        conditions = ' where %s' % conditions
    else:
        conditions = ''
    return conditions

def __checkdb__(newcur=False):
    '''This method use to check the db variable is valid'''
    global db, cur
    if not db:
        raise Error('The database connection is None. Please open a connection from SinORM before you user the modle')
    if not cur or newcur:
        try:
            db.ping(True)
        except:
            # pgdb will raise error
            pass
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
    elif dbtype.find('pgdb')>=0:
        # PostgreSQL
        if mode_debug:
            print 'PostgreSQL'
        __dbtype__ = __TYPE_PGDB__
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
        print '%s sql: %s'%(__dbnames__[__dbtype__], sql)
    try:
        res = cur.execute(sql)
    except:
        __checkdb__(newcur=True)
        res = cur.execute(sql)
    if commit:
        db_commit()
    return res

def get_objects_by_sql(sql):
    '''Get objects from table'''
    count = True
    exe_sql(sql)
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

def get_objects(table, columns='*', conditions='', condtype='and', limit='', order='', offset='', group=''):
    '''Get objects from table'''
    if columns and type(columns) is types.ListType:
        columns = ','.join([__nameliteral__(v) for v in columns])
    conditions = __createconditions__(conditions, condtype)
    if limit:
        limit = ' limit %s' % limit
    if order:
        order = ' order by %s' % order
    if offset:
        offset = ' offset %s' % offset
    if group:
        group = ' group by %s' % group
    sql = 'select %s from %s%s%s%s%s%s' % (columns, __nameliteral__(table), conditions, group, order, limit, offset)
    return get_objects_by_sql(sql)

def get_object(table, keyid, keyidname='id'):
    '''Get one object from table by keyid, keyid is primary-key value, keyidname is primary-key name, default it is "id"'''
    sql = 'select * from %s where %s=%s' % (__nameliteral__(table), keyidname, __literal__(keyid))
    objs = get_objects_by_sql(sql)
    if objs:
        return objs[0]
    else:
        return None

def set_objects(table, obj, conditions='', condtype='and'):
    '''Update objects to database by conditions'''
    conditions = __createconditions__(conditions, condtype)
    setsql = ','.join(['%s=%s' % (__nameliteral__(k), __literal__(v)) for (k, v) in obj.items()])
    sql = 'update %s set %s%s' % (__nameliteral__(table), setsql, conditions)
    return exe_sql(sql, autocommit)

def set_object(table, obj, keyid=0, keyidname='id'):
    '''Update a object to database'''
    objkeyid = None
    if obj.has_key(keyidname):
        objkeyid = obj[keyidname]
        if not keyid:
            keyid = obj[keyidname]
        del obj[keyidname]    
    conditions = '%s=%s' % (__nameliteral__(keyidname), __literal__(keyid))
    res = set_objects(table, obj, conditions)
    if objkeyid:
        obj[keyidname] = objkeyid
    return res

def del_objects(table, conditions='', condtype='and'):
    '''Delete objects to database by conditions'''
    conditions = __createconditions__(conditions, condtype)
    sql = 'delete from %s%s' % (__nameliteral__(table), conditions)
    return exe_sql(sql, autocommit)

def del_object(table, obj, keyid=0, keyidname='id'):
    '''Delete a object to database by keyid'''
    objkeyid = None
    if obj.has_key(keyidname):
        objkeyid = obj[keyidname]
        if not keyid:
            keyid = obj[keyidname]
        del obj[keyidname]    
    conditions = '%s=%s' % (__nameliteral__(keyidname), __literal__(keyid))
    res = del_objects(table, conditions)
    if objkeyid:
        obj[keyidname] = objkeyid
    return res

def add_object(table, obj):
    '''Add a object to table'''
    keys = ','.join(['%s' % __nameliteral__(k) for k in obj.keys()])
    vals = ','.join([__literal__(v) for v in obj.values()])
    sql = 'insert into %s(%s) values(%s)' % (__nameliteral__(table), keys, vals)
    return exe_sql(sql, autocommit)    

def create_table(table, tplobj, keyidname='id', new=False):
    '''Create a table by a template object's property'''
    global __dbtype__
    if new:
        # drop table before start to create
        sql = 'drop table if exists %s' % __nameliteral__(table)
        exe_sql(sql)
    if not tplobj.has_key(keyidname):
        if __dbtype__ == __TYPE_SQLITE__:
            tplobj[keyidname] = 'integer not null'  # SQLite
        elif __dbtype__ == __TYPE_MYSQL__:
            tplobj[keyidname] = 'int not null auto_increment' # MySQL
        elif __dbtype__== __TYPE_PGDB__:
            tplobj[keyidname] = 'bigserial not null' # PostgreSQL
    struct = ','.join(['%s %s' % (__nameliteral__(k), __typemap__(v)) for (k, v) in tplobj.items()])
    struct = '%s,%s' % (struct, 'primary key (%s)' % __nameliteral__(keyidname))
    sql = 'create table %s(%s)' % (__nameliteral__(table), struct)
    return exe_sql(sql, autocommit)

def reset_table(table):
    '''Clear all record by table name'''
    sql = 'truncate table %s' % __nameliteral__(table)
    return exe_sql(sql, autocommit)

def drop_table(table):
    '''Drop the data and table'''
    sql = 'drop table %s' % __nameliteral__(table)
    return exe_sql(sql, autocommit)



def __test__():
    # table name
    table = 't_students'
    
    # create the table
    create_table(table,
               {
                'name':'varchar(128) not null', # the type of field 'name' is char(128) not null
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
    print 'count=%d'%get_objects(table, columns='count(*) as count')[0]['count']



def __testall__():
    global mode_debug, autocommit
    mode_debug = True
    autocommit = False
    # Test MySQL
    print '\n---Start Test MySQL---'
    try:
        import MySQLdb
        try:
            db = MySQLdb.connect(host='127.0.0.1', user='trb', passwd='123', db='dbp', port=3306)
        except:
            db = MySQLdb.connect(host='127.0.0.1', user='root', passwd='123456', db='dbp', port=3306)
        set_db(db)
        __test__()
    except Error, e:
        raise e
        print '****Test MySQL Fail****'
    print '---End Test MySQL---'
    
    # Test sqlite
    print '\n---Start Test sqlite---'
    try:
        import sqlite3
        db = sqlite3.connect('sqlite.db')
        set_db(db)
        __test__()
    except Error, e:
        raise e
        print '****Test sqlite Fail****'
    print '---End Test sqlite---'

    # Test postgresql
    print '\n---Start Test PostgreSQL---'
    try:
        import pgdb
        db = pgdb.connect(user='robin', password='trb', database='dbp')
        set_db(db)
        __test__()
    except Error, e:
        raise e
        print '****Test PostgreSQL Fail****'
    print '---End Test PostgreSQL---'

    
if __name__ == '__main__':
    __testall__()
    

    
    
    
