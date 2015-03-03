# appopy
PL/Python (Postgres) Import for Http-Logs

Requirements

* Python 2.x
* PyGeoIP
* Postgres >= 9.3

Information

* Run SQL-Script with user postgres, to create database and user for the import
-> as root user do something like "su - postgres -c "psql < /root/create_appopy_nojson.sql""
* Default database user is "appopy", database name is "httplog"
* Default import dir for logfiles is /var/lib/pgsql/9.4/data/appopy_import
* Default logformat in function is LogFormat "%V %a %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\" \"%{cookie}i\""

After all its just a little example how it can be done.
