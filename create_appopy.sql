DROP DATABASE IF EXISTS httplog;
DROP ROLE IF EXISTS appopy;
CREATE ROLE appopy WITH LOGIN PASSWORD 'ua3Aepha';
CREATE DATABASE httplog OWNER appopy;
REVOKE ALL PRIVILEGES ON DATABASE httplog FROM PUBLIC;
GRANT ALL PRIVILEGES ON DATABASE httplog TO appopy; 
\c httplog;
SET ROLE appopy;

CREATE TABLE logdata (
    pk_data_id      BIGSERIAL PRIMARY KEY,
    domain          TEXT ,
    remip           cidr,
    remtime         TIMESTAMP,
    request         TEXT,
    status          SMALLINT,
    size            INT,
    referer         TEXT,
    agent           TEXT 
    );

CREATE TABLE stat_daily (
    daily_data      JSONB
    );

CREATE TABLE config (
    configname     TEXT,
    configvalue    TEXT
    );

INSERT INTO config (configname,configvalue) VALUES ('importpath','/var/lib/pgsql/9.5/data/appopy_import');
INSERT INTO config (configname,configvalue) VALUES ('geoippath','/usr/share/GeoIP/GeoIP.dat');


-- We need to reset role to postgres,to create plpythonu-function
RESET ROLE;

-- Function for fileimport
CREATE FUNCTION appopy_import ()
    RETURNS TEXT
AS $$
    from re import compile as rcompile
    from os import listdir, path, remove
    from gzip import open as zopen
    from zipfile import is_zipfile
    from json import dumps
    from pygeoip import GeoIP, GeoIPError, MEMORY_CACHE
    from datetime import datetime

    geopath_q=plpy.execute("SELECT configvalue FROM config WHERE configname = 'geoippath';")
    geopath=geopath_q[0]['configvalue']
    gi = GeoIP(geopath, MEMORY_CACHE)
    daily_list = []
    tablelist= []

    # class for daily domainstatistic
    class daily_domain:
        def __init__(self):
            self.hostname = ""
            self.date_time = ""
            self.count = 0
            self.traffic = 0
            self.land = {} 
        def get_json(self):
            dd_dict={}
            dd_dict['host'] = self.hostname
            dd_dict['date'] = self.date_time
            dd_dict['count'] = self.count
            dd_dict['traffic'] = self.traffic
            for ll in self.land.keys():
                dd_dict[ll] = self.land[ll]
            return dumps(dd_dict)

    impath_q=plpy.execute("SELECT configvalue FROM config WHERE configname = 'importpath';")
    impath=impath_q[0]['configvalue']

    parts = [r'(?P<host>\S+)',r'(?P<rip>\S+)',r'\S+',r'(?P<user>.+)',
             r'\[(?P<time>.+)\]',r'"(?P<request>.+)"',r'(?P<status>([0-9]+|-))',
             r'(?P<size>\S+)',r'"(?P<referer>.*)"',r'"(?P<agent>.*)"',r'".*"']
    pattern = rcompile(r'\s+'.join(parts) + r'\s*\Z')

    importfiles=listdir(impath)
    if len(importfiles) == 0:
        plpy.notice('appopy: no files to import')
    for logfile in importfiles:
        logfile=path.join(impath, logfile)
        if is_zipfile(logfile):
            filelog = zopen(logfile)
        else:
            filelog = open(logfile)
        for line in filelog:
            m = pattern.match(line)
            if not m:
                plpy.warning("appopy: logfileentry does not match regex-pattern and could not be imported:\n   "+str(line))
                continue                
            domain_ok=False
            res = m.groupdict()
            if res['size'] == "-":
                res['size'] = 0
            else:
                res['size'] = int(res['size'])
            if res['status'] == '-':
                res['status'] = 0
            else:
                res['status'] = int(res['status'])
            tablename="d_"+res['host'][:30].replace('.','_').replace('-','_')
 
            if tablename not in tablelist:
                table_exists=plpy.execute("SELECT relname FROM pg_class where relname=%s;" % 
                    ( plpy.quote_nullable(tablename),))
                if not table_exists:
                    plpy.execute("CREATE TABLE IF NOT EXISTS %s (check(domain = %s)) inherits(logdata);" % 
                        (tablename,plpy.quote_nullable(res['host'])))
                tablelist.append(tablename)
            
            plpy.execute("""INSERT INTO %s (domain, remip, remtime, request, status, size, referer, agent) 
                                VALUES (%s,%s,%s,%s,%d,%d,%s,%s);""" % (
                tablename,
                plpy.quote_nullable(res['host']),
                plpy.quote_nullable(res['rip']),
                plpy.quote_nullable(res['time']),
                plpy.quote_nullable(res['request']),
                res['status'],
                res['size'],
                plpy.quote_nullable(res['referer']),
                plpy.quote_nullable(res['agent'])))
 
            ptime, pzone = res['time'].split()
            ptime = str(datetime.strptime(ptime, "%d/%b/%Y:%H:%M:%S").date())

            for i,cdomain in enumerate(daily_list):
                if cdomain.hostname == res['host'] and cdomain.date_time == ptime:
                    domain_ok=True
                    domain_list_index=i
            if domain_ok:
                ddomain=daily_list[domain_list_index]
            else:
                ddomain=daily_domain()
                ddomain.hostname=res['host']
                ddomain.date_time=ptime
                daily_list.append(ddomain)
            ddomain.count += 1
            if res['rip'] == '127.0.0.1':
                local_land='localhost'
            else:
                try:
                    local_land=gi.country_code_by_addr(res['rip'])
                    if not local_land:
                        local_land = 'notfound'
                except GeoIPError:
                    local_land='noipv4'
            if local_land not in ddomain.land.keys():
                ddomain.land[local_land]=1
            else:
                ddomain.land[local_land]+= 1
            ddomain.traffic += res['size']
 
        for daidom in daily_list:
            plpy.execute("INSERT INTO stat_daily (daily_data) VALUES (%s);" % 
                (plpy.quote_nullable(daidom.get_json()),))        

        if filelog:
            filelog.close()
        #remove(logfile)
                
$$ LANGUAGE plpythonu;

