#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import glob
import os,sys,errno
from pprint import pprint
import MySQLdb
import time
import ConfigParser

def config_map(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                print ("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def disk_usage(path):
    st = os.statvfs(path)
    total = (st.f_blocks * st.f_frsize)
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    try:
        percent = (float(used) / total) * 100
    except ZeroDivisionError:
        percent = 0
    return round(percent, 1)


if __name__ == '__main__':
  #logger
  logger = logging.getLogger('vs-cleaner')
  hdlr = logging.FileHandler('/var/log/nginx/prerecord.log')
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
  hdlr.setFormatter(formatter)
  logger.addHandler(hdlr)
  logger.setLevel(logging.WARNING)

  #config
  Config = ConfigParser.ConfigParser()
  Config.read("/usr/local/sbin/vs/vs.cfg")
  del_limit = config_map("vs")['del_limit']
  stor = config_map("vs")['stor']
  numserv = config_map("vs")['numserv']

  db_host = config_map("mysql")['db_host']
  db_user = config_map("mysql")['db_user']
  db_pass = config_map("mysql")['db_pass']
  db_name = config_map("mysql")['db_name']

  size = disk_usage("/var/spool/" + stor)
  print "Usage => " + str(size)

  db = MySQLdb.connect(host= db_host, user= db_user, passwd= db_pass, db= db_name, charset='utf8')
  cursor = db.cursor()
  sql = 'SELECT stream, reserv FROM cams WHERE reserv>0'
  cursor.execute(sql)
  data =  cursor.fetchall()

  done = 0
  pr_cam = []
  for stream,reserv in data:
    pr_cam.append( str(stream) )
    sql_cam = 'SELECT id,path FROM records WHERE start_time < NOW() - INTERVAL '+str(reserv)+' DAY AND s_name="'+str(stream)+'" AND server ="'+str(numserv)+'"'
    cursor.execute(sql_cam)
    data_cam =  cursor.fetchall()
    for id,path in data_cam:
        print 'Delete file ' + path

        try:
          if os.path.isfile(path):
            os.unlink( path )
          del_sql = 'DELETE FROM records WHERE id = ' + str(id)
          cursor.execute(del_sql)
          db.commit()
          logger.warning("File: %s - deleted" % str(path) )
          done = done + 1
        except Exception, e:
          logger.error("Error %s while deleting file: %s" % (str(e), path) )


  del_dict = {}

  sql = 'SELECT id,s_name,path FROM records WHERE decoded=1 AND server ="' +str(numserv)+ '" ORDER BY start_time LIMIT ' + str(del_limit)
  cursor.execute(sql)
  data =  cursor.fetchall()
  for  id,s_name,path in data:
    if str(s_name) in pr_cam:
      continue
    else:
      del_dict[str(id)] = str(path)

  for k,v in del_dict.items():
    if size > 98:
      print 'Delete file ' + k + ' => ' + v
      print "size al" + str(size)
      try:
        if os.path.isfile(v):
          os.unlink( v )
        del_sql = 'DELETE FROM records WHERE id = ' + str(k)
        cursor.execute(del_sql)
        db.commit()
        size = disk_usage("/var/spool/" + stor)
        logger.warning("File: %s - deleted" % str(path) )
        done = done + 1
      except Exception, e:
        logger.error("Error %s while deleting file: %s" % ( str(e), path )  )

  
  if done > 0:
      logger.warning("---------------------------------Deleted %s files-----------------------------------" % done )


  cursor.close()
  db.close()
