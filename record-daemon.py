#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import time
from pprint import pprint
from vs_inspect import VideoInspector
import re
import glob
import os,sys,errno
import logging
#for jpg
import urllib2
import ConfigParser
#http://www.tummy.com/software/python-memcached/
import memcache

from Queue import Queue
from threading import Thread, Lock
from time import gmtime, strftime
from copy import deepcopy


class Worker(Thread):
    #global logger

    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
              func(*args, **kargs)
            except Exception, e:
              logger.error("%s" % str(e) )

            self.tasks.task_done()

class ThreadPool:
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads): Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        self.tasks.join()

def Supervisor(thr_list):
    #global logger
    thr = []
    thread_sw = None
    for thread_name in thr_list:
        thr.append(None)

    while True:
         i = 0
         for thread_name in thr_list:
            if not thr[i] or not thr[i].is_alive():
                thr[i] = Thread(target = thread_name )
                thr[i].daemon = True
                thr[i].start()
                logger.error("Starting thread for: %s" % str(thread_name) )
            thr[i].join(1)
            i = i + 1

         time.sleep(10)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def config_map(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            #if dict1[option] == -1:
            #    DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

def get_jpg(cam):
    #global logger
    global urls

    if urls[ str(cam['path']) ]['auth'] == '0':
      try:
        ts = time.time()
        file = "/var/jpg/" + cam['name'] + ".jpg"
        file_ts = file + str(ts)
        if cam['login'] != "None" and cam['pass'] != "None" and cam['ip'] != "":
          url = "http://" + cam['ip'] + urls[ str(cam['path']) ]['path']
          #print "FILe " + file + " FILETS " + file_ts + " URL " + url
          passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
          passman.add_password(None, url, cam['login'], cam['pass'])
          auth_handler = urllib2.HTTPBasicAuthHandler(passman)
          opener = urllib2.build_opener(auth_handler)
          urllib2.install_opener(opener)
          req = urllib2.urlopen(url, timeout=5)
          CHUNK = 16 * 1024
          with open(file_ts, 'wb') as fp:
            while True:
              chunk = req.read(CHUNK)
              if not chunk: break
              fp.write(chunk)
          fp.close()
          os.rename(file_ts, file)
          logger.warning("Get jpg: login->%s pass->%s ip->%s path->%s name->%s " % ( str(cam['login']), str(cam['pass']), str(cam['ip']), str(cam['path']), str(cam['name']) ) )
      except Exception,e:
          raise Exception("Error: %s while downloading jpeg from %s" % ( str(e), cam['name'] ) )
    #else:
        #options for basic_auth
        #opts['args'] = " --http-user=" + str(cam['login']) + " --http-password=" + str(cam['pass']) + " "


def convert_video(flv_file, cname, ts1, opts, storage):
  from vs_encoder import VideoEncoder

  year = time.strftime("%Y", time.localtime(int(ts1)))
  month = time.strftime("%m", time.localtime(int(ts1)))
  day = time.strftime("%d", time.localtime(int(ts1)))
  h_m_s = time.strftime("%H_%M_%S", time.localtime(int(ts1)))

  dirname = "/var/spool/" + storage + "/" + cname + "/" + year + "/" + month + "/" + day
  if not os.path.exists( dirname ):
    mkdir_p( dirname )

  opts_v = opts['video'] + " " + opts['fps'] + " " + opts['sync']
  logger.warning("Converting file %s with video options -> %s audio -> %s" % ( str(flv_file), str(opts_v), str(opts['audio']) ) )
  video = VideoEncoder( flv_file )
  output_file = dirname + "/" + cname + "-" + h_m_s + ".mp4"

  video.execute(
        "%(ffmpeg_bin)s " + opts['fps'] + " " + opts['sync'] + " -i %(input_file)s " + opts['video'] + " " + opts['audio'] + " -f mp4 -y %(output_file)s",
        output_file
  )

  return output_file

def mysql_insert():
    global db
    global memc
    global rows
    global convert_opts
    global urls
    global cams

    backup_file = "/usr/local/sbin/vs/mysql_back.txt"

    memc = memcache.Client(['127.0.0.1:11211'], debug=0)
    memc.set("alive", "true")
    try:
        db = MySQLdb.connect(host= db_host, user= db_user, passwd= db_pass, db= db_name, charset='utf8')
    except MySQLdb.Error, e:
        logger.error("%d: %s" % (e.args[0], e.args[1]) )
        if len(rows) > 0:
          f = open(backup_file,"a")
          for row in rows:
            for item in row:
                f.write(item + "=")
            f.write("\n")
          rows = []
          f.close()
          sys.exit()

    from_b = 0
    if os.path.isfile(backup_file):
	  f = open(backup_file, "r")
	  rows = []
	  while True:
	    line = f.readline()
	    line = line.strip()
            #del last =
            line = line[:-1]
	    if not line:
	       break
	    row = re.split('=', line)
	    rows.append( tuple(row) )

          #pprint(rows)
          from_b = 1
          f.close()
          os.unlink( backup_file )

    while True:
      try:
        mem_opts = memc.get('opts')
        convert_opts = { };
        if not mem_opts:
          cursor = db.cursor()
          sql = 'SELECT id,fps,video,audio,sync FROM profiles LIMIT 300'
          cursor.execute(sql)
          data =  cursor.fetchall()
          memc.set('opts',data,1800)
          cursor.close()
          for rec in data:
              id,fps,video,audio,sync = rec
              convert_opts[ str(id) ] = { "fps" : str(fps), "video": str(video), "audio": str(audio), "sync": str(sync) }
        else:
          for rec in mem_opts:
              id,fps,video,audio,sync = rec
              convert_opts[ str(id) ] =  { "fps" : str(fps), "video": str(video), "audio": str(audio), "sync": str(sync) }

        mem_urls = memc.get('urls')
        urls = { };
        mem_cams = memc.get('cams')
        cams = { };
        if not mem_urls:
          cursor = db.cursor()
          sql = 'SELECT id,auth,path FROM cams_url LIMIT 300'
          cursor.execute(sql)
          data =  cursor.fetchall()
          memc.set('urls',data,1800)
          cursor.close()
          for rec in data:
            id,auth,path = rec
            urls[ str(id) ] = { "auth": str(auth), "path": str(path) }
        else:
          for rec in mem_urls:
            id,auth,path = rec
            urls[ str(id) ] = { "auth": str(auth), "path": str(path) }

        if not mem_cams:
          cursor = db.cursor()
          sql = 'SELECT id,ip,l,p,path,stream FROM cams WHERE server=%s LIMIT 300' % numserv
          cursor.execute(sql)
          data =  cursor.fetchall()
          memc.set('cams',data,1800)
          cursor.close()
          for rec in data:
            id,ip,l,p,path,stream = rec
            cams[ str(id) ] = { "ip": str(ip), "login": str(l), "pass": str(p), "path": str(path), "name": str(stream) }
        else:
          for rec in mem_cams:
            id,ip,l,p,path,stream = rec
            cams[ str(id) ] = { "ip": str(ip), "login": str(l), "pass": str(p), "path": str(path), "name": str(stream) }
      except Exception, e:
            logger.error("Error while get converting opts from mysql/memcache: %s" % str(e) )

      if len(rows) > 0:
        cursor = db.cursor()
    	rows_c = deepcopy(rows)
	rows = []
        stmt = "INSERT INTO records (path, start_time, s_name, frames, fps, video, audio, stop_time, size, decoded, server) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            cursor.executemany(stmt, rows_c  )
            db.commit()
            logger.warning("Insert data to mysql: %s" % str(len(rows_c)) )
            if from_b > 0:
	      if os.path.isfile(backup_file):
                os.unlink( backup_file )

        except Exception, e:
            logger.error("Error while inserting to mysql: %s" % str(e) )
            f = open(backup_file,"a")
            for row in rows_c:
              for item in row:
                  f.write(item + "=")
              f.write("\n")
            rows_c = []
            f.close()
            sys.exit()

        cursor.close()

      if memc.get("alive") == None:
          memc = memcache.Client(['127.0.0.1:11211'], debug=0);
          memc.set("alive", "true")

      time.sleep(10)

    return 0

#htym
def start_vs_pool():
    #global db
    #global memc
    #global convert_opts
    pool = ThreadPool(poolnum)
    while True:
      filelist = glob.glob("/var/spool/video/temp/*.flv")
      for ffile in  filelist:
         pool.add_task(copy_file, ffile)
      pool.wait_completion()
      time.sleep(10)


def start_jpg_pool():
    #global db
    #global memc
    #global cams
    #global urls
    pool = ThreadPool(poolnum_jpg)
    while True:
      for id in cams:
        pool.add_task(get_jpg, cams[id])

      pool.wait_completion()
      time.sleep(10)


def copy_file(flv_file):
    #global logger
    #global numserv
    #global mutex
    #global storage
    #global rows
    #global convert_opts

    statinfo = os.stat(flv_file)

    fs = statinfo.st_size
    if fs > 1000:
      try:
         video = VideoInspector( flv_file )
      except Exception, e:
        os.unlink(flv_file)
        logger.error("Error -> %s for file-> %s - removed" % (str(e), str(flv_file)) )

      frame = video.frame()

      svideo = video.video_codec()
      try:
        ts1 = re.search("([0-9]+)%([0-9]+)", flv_file).group(1)
        ts2 = re.search("([0-9]+)%([0-9]+)", flv_file).group(2)
      except Exception, e:
        os.unlink(flv_file)
        raise Exception("Invalid filename -> %s - removed" % flv_file )

      duration = int(ts2)-int(ts1)
      if duration == 0: 
        duration = 1
      fps = round( float(frame) / duration )

      if int(fps) == 0:
        os.unlink(flv_file)
        raise Exception("Invalid fps -> %s for file -> %s - removed" % (str(fps), str(flv_file) ) )

      #options for converting
      encode = None
      try:
        encode = video.encode()
        logger.warning("Found metadata: File-> %s, Encode-> %s" % ( str(flv_file), str(encode) ))
      except Exception, e:
        encode = None

      opts = {'video':"", 'audio':"", 'fps':"", 'sync':"" }
      saudio = ""
      audio_str = video.audio_stream()
      if audio_str != None:
            match = re.search('none', audio_str, re.IGNORECASE)
            if match == None:
              saudio = video.audio_codec()

      if encode is not None:
          opts_c = deepcopy(convert_opts)
          opts = opts_c[ encode ]
          if opts['fps'] == '1':
              opts['fps'] = " -r " + str(int(fps)) + " "
          else:
              opts['fps'] = ""
      else:
          opts['video'] = "-c copy"

      cname = re.search("([a-zA-Z-0-9_]+)-([0-9]+)%([0-9]+)", flv_file).group(1)
      date_start = time.strftime("%Y-%m-%d %T", time.localtime(int(ts1)))
      date_stop = time.strftime("%Y-%m-%d %T", time.localtime(int(ts2)))

      converted_file = ""
      try:
        converted_file = convert_video(flv_file, cname, ts1, opts, storage)
      except Exception, e:
        raise Exception("Error while converting file %s" % flv_file)

      fileinfo = os.stat(converted_file)
      result_size = fileinfo.st_size

      row = (str(converted_file), str(date_start), str(cname), str(frame), str(int(fps)), str(svideo), str(saudio), str(date_stop), str(result_size), str("1"), str(numserv) )
      rows.append(row)
      os.unlink(flv_file)

      time.sleep(1)

      logger.warning("Converted file-> %s, Frames-> %s, FPS-> %s, Vcodec -> %s, Acodec -> %s, Time -> %s, Size -> %s, Duration -> %s" % ( str(converted_file), str(frame), str(fps), str(svideo), str(saudio), str(duration), str(result_size), str(duration) ) )
      del video
      del frame
      del svideo
      del saudio
      del ts1
      del ts2
      del fps
      del cname
      del date_start
      del date_stop 

    else:
      logger.error("File-> %s - Bad fragment -> %s - removed" % ( str(flv_file), str(fs) ) )
      os.unlink(flv_file)

if __name__ == '__main__':
    Config = ConfigParser.ConfigParser()
    Config.read("/usr/local/sbin/vs/vs.cfg")

    #logger
    logger = logging.getLogger('record')
    hdlr = logging.FileHandler('/var/log/nginx/prerecord.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.ERROR)

    #global variables
    numserv = config_map("vs")['numserv']
    mutex = Lock()
    poolnum = int(config_map("vs")['threads_vs'])
    poolnum_jpg = int(config_map("vs")['threads_jpg'])
    storage = config_map("vs")['stor']
    db_host = config_map("mysql")['db_host']
    db_user = config_map("mysql")['db_user']
    db_pass = config_map("mysql")['db_pass']
    db_name = config_map("mysql")['db_name']

    rows = []
    memc = None
    db = None
    convert_opts = {}
    urls = {}
    thr_list = [ mysql_insert, start_vs_pool, start_jpg_pool  ]
    Supervisor( thr_list )

    sys.exit()

