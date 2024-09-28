# Commands for manipulating database
### Remove the tables in the database
I you want to remove the tables in the database then you first need to remove the foregin key reference between the tables. This can be achieved by using the following command:
```
 SET FOREIGN_KEY_CHECKS = 0;
```
- 0: remove the foregin key references
- 1: include the foreign key references

The command for dropping the tables: 
```
DROP TABLE db1.TrackPoint;
```

If you only want to remove the values which in the table: 
```
 DELETE FROM <database_name>.<table_name>;
```
The command will delete the values, but will NOT reset the identity columns (the indexing of the id will not be reset to 0). To be able to also reset the identity columns we can use TRUNCATE: 
```
TRUNCATE  TABLE <database_name>.<table_name>
```

If you want to see the tables: 
```
SHOW TABLES FROM <table_name>
```
