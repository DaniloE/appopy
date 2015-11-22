# appopy
PL/Python (Postgres) Import for Http-Logs

Requirements

* Python 2.x
* PyGeoIP
* Postgres >= 9.3

Information

appopy imports http-logfiles and stores the entries in seperate tables for each domain.
Additional there is a table called "daily_stats" where a daily values are kept in jsonb-format.
The function will also make a lookup in the GeoIP-data for every remote IP and stores the request-count per country in the daily stats.
When the importis finished, the logfile will be deleted.
The import-dir and the path to the GeoIP-database are stored in an additional config table.

Usage

* Run SQL-Script with user postgres, to create database and user for the import
-> as root user do something like "su - postgres -c "psql < /root/create_appopy.sql""
* Connect to database and use function appopy_import to import the logfiles in the import dir
* default values are listed below

Default values

* database user is "appopy", database name is "httplog"
* import dir for logfiles is /var/lib/pgsql/9.5/data/appopy_import
* logformat in function is LogFormat "%V %a %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\" \"%{cookie}i\""



After all its just a little example how it can be done.
