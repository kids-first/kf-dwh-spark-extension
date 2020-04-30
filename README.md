Kids First DWH extension
=========

This project contains some extensions for variant clusters. 
It provide a session extension with an implementation of [ParserInterface](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-ParserInterface.html).

When activated this extension will :
- Create a view for occurences, based on acls parameter `kf.dwh.acls` passed in spark config
- Use `variant_live` as default database
- In all queries, replace `saved_sets` by `json.${config_saved_sets}` where `config_saved_sets` correspond to the parameter `kf.dwh.saved_sets`


Example of usage :

```
val readSession = getSparkSessionBuilder()
    .config("spark.sql.extensions", "org.kidsfirstdrc.dwh.spark.extension.DWHSparkSessionExtension")
    .config("kf.dwh.saved_sets", "s3a://path/to/my_saved_sets")
    .config("kf.dwh.acls", """{"SD_1":["acl1", "acl2"], "SD_2":["acl1", "acl3"]}""")
    .getOrCreate()

readSession.table("occurences") //return the occurences based on acls
readSession.table("saved_sets") //return the saved sets of the user
    
``` 

More information on spark session : https://databricks.com/session/how-to-extend-apache-spark-with-customized-optimizations





