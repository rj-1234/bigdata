package com.ericsson.gaia.spark.tests.datamanipulation

object SparkTestApplication {

    private final val DATA_DIRECTORY: String = "/tmp/gaia-spark-test"
    private final val OUTPUT_DIRECTORY: String = DATA_DIRECTORY + "/output"

    /**
      * Spark data manipulation exercise:
      *
      * Input description: the input files for the exercise are 9 files located in DATA_DIRECTORY:
      * /tmp/gaia-spark-test/
      * ├── file1.txt
      * ├── file2.txt
      * ├── file3.txt
      * ├── file4.txt
      * ├── file5.txt
      * ├── file6.txt
      * ├── file7.txt
      * ├── file8.txt
      * └── ip-lookups.txt
      *
      * All of these files can be read on all the Spark executors and the Spark driver
      * file1.txt to file8.txt contain the raw data and ip-lookups.txt contains the matching hostnames to the IP addresses used in the raw data.
      *
      * Valid records in filex.txt are strings made of at least two comma separated entries. The second of these fields is an IP address.
      *
      * Valid records in ip-lookups.txt are strings made of at least two comma separated entries, the first one is the IP address, the second one is the
      * hostname
      *
      * Your work is to write a Spark application that reads all the data in file1.txt to file8.txt and then for each record (line) in each of the files,
      * replace the second field (an IP address) by its corresponding hostname as provided in ip-lookups.txt. Finally, write the resulting data back to
      * /tmp/gaia-spark-test/output using rdd.saveAsTextFile(OUTPUT_DIRECTORY)
      *
      * Note that:
      *  - not all records in the input are valid and you must only write back the valid records
      *  - not all IPs in the valid records can be matched to a hostname and you must only write back the records for which the IP could be found in
      *    ip-lookups.txt
      *  - the application should work with any amount of input records in file*.txt (possibly terabytes of data)
      *  - the amount of data in ip-lookups.txt is lower and will never exceed 200MiB
      *
      * The application should return a tuple of 3 Longs (see @return)).
      *
      * @return a tuple of 3 Long respectively returning:
      *          - the total number of input records in file*.txt
      *          - the number of invalid record (that could not be parsed)
      *          - the number of failed lookups (number of records for which an hostname was not found in ip-lookups.txt)
      */
    private def test(): (Long, Long, Long) = {
        (0l, 0l, 0l)
    }

    def main(args: Array[String]): Unit = {
        val (total, invalid, notFound): (Long, Long, Long) = test()
        System.out.println("  > total records: " + total)
        System.out.println("  > failed parsing: " + invalid)
        System.out.println("  > failed lookups: " + notFound)
    }

}
