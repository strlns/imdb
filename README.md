# imdb

IMDB toy project

PHP script to download IMDB datasets provided by IMDB, connect to a MySQL-DB on a remote host and then write the
data into the MySQL database.

See
https://www.imdb.com/interfaces/

This only practical use of this is when:

- You want/have to use a database on some shared hosting provider
- The hosting provider has disabled access to features of the DBMS to import the data directly
  (MySQL normally can process TSV and CSV files into inserts natively).

This script can take long to run, it is just a gist in repository form.

The script `import-to-netcup.php` accepts the URL pointing to the current `.tsv.gz` file as its argument.
It then treats the file basename as table name and imports the data into the corresponding table.

For convenience, the script also replaces dots in the file basename with underscores.
