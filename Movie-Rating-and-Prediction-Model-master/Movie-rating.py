
import os
from pyspark.sql.functions import explode,col,desc,asc, split


basic = spark.read.csv("/shared/users/avi/desktop/test/title.basics.tsv",sep="\t")
rating = spark.read.csv("/shared/users/avi/desktop/test/title.ratings.tsv",sep="\t")


filterColumn = basic.select('_c0', '_c1', '_c2', '_c5','_c7','_c8').withColumnRenamed('_c0','tconst').withColumnRenamed('_c1', 'titletype').withColumnRenamed('_c2','title').withColumnRenamed('_c5','year').withColumnRenamed('_c7','runtime').withColumnRenamed('_c8','genre')

filterMovie = filterColumn.filter("titletype = 'movie'").filter("year > '1999'").filter("year < '2018'").select('tconst','title','year','runtime','genre').orderBy(desc('year'))

filterRating = rating.select('_c0', '_c1', '_c2').withColumnRenamed('_c0','tconst').withColumnRenamed('_c1', 'avgRating').withColumnRenamed('_c2','votes')

output = filterMovie.join(filterRating, on="tconst").filter("votes > 10000").orderBy(desc('year'),desc('avgRating'))


nameBasic = spark.read.csv("/shared/users/avi/desktop/test/name.basics.tsv",sep="\t")
crew = spark.read.csv("/shared/users/avi/desktop/test/title.crew.tsv",sep="\t")

nameBasicColumnRename = nameBasic.select('_c0', '_c1').withColumnRenamed('_c0','writer').withColumnRenamed('_c1', 'writeName')
nameColumnRename = crew.select('_c0', '_c2').withColumnRenamed('_c0','tconst').withColumnRenamed('_c2', 'writer')


writerJoin = nameColumnRename.join(output, on='tconst')
writerExplode = writerJoin.withColumn("writer", explode(split(col("writer"), ","))).select("tconst","writer","title","year","avgRating","votes",'runtime','genre')

table1 = writerExplode.toDF("tconst","writer","title","year","avgRating","votes",'runtime','genre')
z.show(table1)


writerJoining = writerExplode.join(nameBasicColumnRename, on='writer')
writerOutput = writerJoining.select('tconst','title','writeName').withColumnRenamed('title','title1')

table2 = writerOutput.toDF('tconst','title1','writeName')
z.show(table2)


nameBasicColumnRename1 = nameBasic.select('_c0', '_c1').withColumnRenamed('_c0','director').withColumnRenamed('_c1', 'directorName')
nameColumnRename1 = crew.select('_c0', '_c1').withColumnRenamed('_c0','tconst').withColumnRenamed('_c1', 'director')

table3 = nameColumnRename1.toDF('tconst','director')
z.show(table3)


directorJoin = nameColumnRename1.join(output, on='tconst')
directorExplode = directorJoin.withColumn("director", explode(split(col("director"), ","))).select("tconst","director","title","year","avgRating","votes",'runtime','genre')
directorJoining = directorExplode.join(nameBasicColumnRename1, on='director')

table4 = directorJoining.toDF("tconst","director","directorName","title","year","avgRating","votes",'runtime','genre')
z.show(table4)


final = writerOutput.join(directorJoining, on='tconst')

table5 = final.toDF('tconst','title1','writeName','director','title','year','avgRating','votes','runtime','genre','directorName')
z.show(table5)


finalOutput = final.select('title', 'year','avgRating','votes','directorName','writeName',"runtime","genre").filter('votes > 10000').orderBy(desc('year'),desc('avgRating'))
x = finalOutput.toDF('title', 'year','avgRating','votes','directorName','writeName',"runtime","genre")
z.show(x)


finalOutput.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/shared/users/avi/desktop/test/2000-18")