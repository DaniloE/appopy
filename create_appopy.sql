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
    method          TEXT,
    request         TEXT,
    status          SMALLINT,
    size            BIGINT,
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

INSERT INTO config (configname,configvalue) VALUES ('importpath','/var/lib/pgsql/9.4/data/appopy_import');
INSERT INTO config (configname,configvalue) VALUES ('geoippath','/usr/share/GeoIP/GeoIP.dat');


-- We need to reset role to postgres,to create plpythonu-function
RESET ROLE;

-- Function for fileimport
CREATE FUNCTION appopy_import ()
    RETURNS TEXT
AS $$
    import re
    import os
    import gzip
    import zipfile
    import json
    import pygeoip
    from datetime import datetime

    geopath_q=plpy.execute("SELECT configvalue FROM config WHERE configname = 'geoippath';")
    geopath=geopath_q[0]['configvalue']
    gi = pygeoip.GeoIP(geopath, pygeoip.MEMORY_CACHE)
    daily_list = []

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
            return json.dumps(dd_dict)

    impath_q=plpy.execute("SELECT configvalue FROM config WHERE configname = 'importpath';")
    impath=impath_q[0]['configvalue']

    parts = [r'(?P<host>\S+)',r'(?P<rip>\S+)',r'\S+',r'(?P<user>.+)',
             r'\[(?P<time>.+)\]',r'"(?P<request>.+)"',r'(?P<status>([0-9]+|-))',
             r'(?P<size>\S+)',r'"(?P<referer>.*)"',r'"(?P<agent>.*)"',r'".*"']
    pattern = re.compile(r'\s+'.join(parts) + r'\s*\Z')

    importfiles=os.listdir(impath)
    if len(importfiles) == 0:
        plypa.notice('appopy: Keine Dateien zum Import im Verzeichnis')
    for logfile in importfiles:
        logfile=os.path.join(impath, logfile)
        if zipfile.is_zipfile(logfile):
            filelog = gzip.open(logfile)
        else:
            filelog = open(logfile)
        for line in filelog:
            m = pattern.match(line)
            if m is None:
                plpy.warning("appopy: logfileentry does not match regex-pattern and could not be imported:\n   "+str(line))
                continue                
            else:
                domain_ok=False
                res = m.groupdict()
                if res['size'] == "-":
                    res['size'] = 0
                else:
                    res['size'] = int(res['size'])
                try:
                    request_splitted=res['request'].split()
                    res['method'] = request_splitted[0]
                    res['request'] = request_splitted[1]
                except IndexError:
                    #plpy.warning("appopy: no method in request, saved as method UNDEF:\n "+str(line))
                    res['method'] = 'UNDEF'
                if res['status'] == '-':
                    res['status'] = '0'
                tablename="date_"+res['host'].replace('.','_').replace('-','_')

                table_exists=plpy.execute("SELECT relname FROM pg_class where relname=%s;" % 
                    ( plpy.quote_nullable(tablename),))
                if not table_exists:
                    plpy.execute("CREATE TABLE IF NOT EXISTS %s (check(domain = %s)) inherits(logdata);" % 
                        (tablename,plpy.quote_nullable(res['host'])))

                plpy.execute("""INSERT INTO %s (domain, remip, remtime, method, request, status, size, referer, agent) 
                                VALUES (%s,%s,%s,%s,%s,%s,%d,%s,%s);""" % (
                    tablename,
                    plpy.quote_nullable(res['host']),
                    plpy.quote_nullable(res['rip']),
                    plpy.quote_nullable(res['time']),
                    plpy.quote_nullable(res['method']),
                    plpy.quote_nullable(res['request']),
                    plpy.quote_nullable(res['status']),
                    res['size'],
                    plpy.quote_nullable(res['referer']),
                    plpy.quote_nullable(res['agent'])))

                ptime, pzone = res['time'].split()
                ptime = str(datetime.strptime(ptime, "%d/%b/%Y:%H:%M:%S").date())

                for ddomain in daily_list:
                    if ddomain.hostname == res['host'] and ddomain.date_time == ptime:
                        ddomain.count += 1
                        if res['rip'] == '127.0.0.1' or res['rip'] == '::1':
                            local_land='localhost'
                        else:
                            try:
                                local_land=gi.country_code_by_addr(res['rip'])
                                if local_land == '':
                                    local_land = 'notfound'
                            except pygeoip.GeoIPError:
                                local_land='noipv4'
                        if local_land not in ddomain.land.keys():
                            ddomain.land[local_land]=1
                        else:
                            ddomain.land[local_land]+= 1
                        ddomain.traffic += res['size']
                        domain_ok=True
    
                if not domain_ok:
                    dado=daily_domain()
                    dado.hostname=res['host']
                    dado.date_time=ptime
                    if res['rip'] == '127.0.0.1' or res['rip'] == '::1':
                        local_land='localhost'
                    else:
                        try:
                            local_land=gi.country_code_by_addr(res['rip'])
                            if local_land == '':
                                local_land = 'notfound'
                        except pygeoip.GeoIPError:
                            local_land='noipv4'
                    dado.land[local_land]=1
                    dado.count += 1
                    dado.traffic += res['size']
                    daily_list.append(dado)
        for daidom in daily_list:
            plpy.execute("INSERT INTO stat_daily (daily_data) VALUES (%s);" % 
                (plpy.quote_nullable(daidom.get_json()),))        

        if filelog:
            filelog.close()
                
$$ LANGUAGE plpythonu;

