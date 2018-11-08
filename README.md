# add more cases match from <Spark-JDBC-limit>
   use while(prev.child !=null)  to search for JDBCRelation
   
   and use reflection to replace the node other than re-create the whole tree!
   
   
# spark-jdbc-limit   
#https://raw.githubusercontent.com/lightcopy/spark-jdbc-limit/
Spark JDBC optimization rule to propagate limit to database

Repository provides rule to push down `LocalLimit` to JDBC relation and add it to the executed
query. You could potentially use `fetchsize` option and/or filters, but it is much nicer when
optimizer does this job for you:).

This repository also contains fix for global commit/rollback for Postgres; it will roll back all
commits made within task set when error occurs in any tasks, including the very last one.

## Usage
Build jar using `sbt package` and add it to Spark jars classpath. In the code add rule to the
Spark session extra optimizations, like this:
```scala
spark.experimental.extraOptimizations ++= Seq(org.apache.spark.sql.PropagateJDBCLimit)
```

I tested with **postgres** and **mysql** with following `spark-shell` on large table, but example
below is done for simple range DataFrame.

```shell
spark-shell \
  --jars target/scala-2.11/spark-jdbc-limit_2.11-0.1.0-SNAPSHOT.jar \
  --packages mysql:mysql-connector-java:5.1.38
```

Following code is used to test reads with limit:
```scala
val props = new java.util.Properties()
props.put("driver", "com.mysql.jdbc.Driver")
val url = "jdbc:mysql://localhost:3306/db?user=user&password=password&useSSL=false"

spark.range(1000).write.jdbc(url, "test", props)

// Add rule to extra optimizations, this will trigger usage of relation with limit
spark.experimental.extraOptimizations = Seq(org.apache.spark.sql.PropagateJDBCLimit)

val df = spark.read.jdbc(url, "test", props)
df.show()
df.limit(10).groupBy("id").count().collect
```

This is how query plan looks for `df.show()`:
```
== Parsed Logical Plan ==
GlobalLimit 21
+- LocalLimit 21
   +- Relation[id#23L] JDBCRelation(test) [numPartitions=1]

== Analyzed Logical Plan ==
id: bigint
GlobalLimit 21
+- LocalLimit 21
   +- Relation[id#23L] JDBCRelation(test) [numPartitions=1]

== Optimized Logical Plan ==
GlobalLimit 21
+- LocalLimit 21
   +- Relation[id#23L] JDBCRelationWithLimit(test) [numPartitions=1] [limit=21]

== Physical Plan ==
CollectLimit 21
+- *Scan JDBCRelationWithLimit(test) [numPartitions=1] [limit=21] [id#23L]
      ReadSchema: struct<id:bigint>
```

Here is query plan for `df.limit(10).groupBy("id").count().collect`:
```
== Parsed Logical Plan ==
Aggregate [id#23L], [id#23L, count(1) AS count#49L]
+- GlobalLimit 10
   +- LocalLimit 10
      +- Relation[id#23L] JDBCRelation(test) [numPartitions=1]

== Analyzed Logical Plan ==
id: bigint, count: bigint
Aggregate [id#23L], [id#23L, count(1) AS count#49L]
+- GlobalLimit 10
   +- LocalLimit 10
      +- Relation[id#23L] JDBCRelation(test) [numPartitions=1]

== Optimized Logical Plan ==
Aggregate [id#23L], [id#23L, count(1) AS count#49L]
+- GlobalLimit 10
   +- LocalLimit 10
      +- Relation[id#23L] JDBCRelationWithLimit(test) [numPartitions=1] [limit=10]

== Physical Plan ==
*HashAggregate(keys=[id#23L], functions=[count(1)], output=[id#23L, count#49L])
+- *HashAggregate(keys=[id#23L], functions=[partial_count(1)], output=[id#23L, count#58L])
   +- *GlobalLimit 10
      +- Exchange SinglePartition
         +- *LocalLimit 10
            +- *Scan JDBCRelationWithLimit(test) [numPartitions=1] [limit=10] [id#23L]
                  ReadSchema: struct<id:bigint>
```

## Build
Simply run `sbt package` to build jar, or `sbt compile` to compile code.
