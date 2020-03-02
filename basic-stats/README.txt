Scripts are written to run the basic, domain, distribution, and
entity stats. The output of the scripts can be found at 
test02.sindice.net:/user/sindice/analytics/basic-stats/${stat-name}/${sindice-dump=timestamp}/

for example you can find basic-class-stats from the 2011-07-28 dump at
/user/sindice/analytics/basic-stats/basic-class-stats/2011-07-28

You can also find the scripts at:
test02.sindice.net:/home/thoper/hadoop-production/stats-scripts

or get them from svn at sindice/core/analytics/src/main/stat-scripts

by default the scripts get the most recent export from
test02.sindice.net:/user/sindice/export/

and output the sequence files to
test02.sindice.net:/user/sindice/analytics/basic-stats/${stat-name}/${sindice-dump=timestamp}/MAP/

and a text file with the top 100,000 records to
/user/sindice/analytics/basic-stats/${stat-name}/${sindice-dump=timestamp}/TEXT/

If you want to run a job, you can simply run the script of the
corresponding job. If you prefer to manually launch the stats job, you
can use the following command with the appropriate values
$HADOOP_BIN jar $CLASS_PATH $CLASS --input $IMPORT_DIR --output $OUTPUT_DIR --n-reduce 10

Here's an example

~/hadoop-production/bin/hadoop jar
~/hadoop-production/stats-scripts/analytics-0.0.2-SNAPSHOT-jar-with-dependencies.jar org.sindice.core.analytics.stats.basic.cli.BasicClassStatsCLI 
--input /user/sindice/export/2011-07-28 --output /user/sindice/analytics/basic-stats/basic-class-stats/2011-07-28/MAP --n-reduce 10


EXAMPLE SCRIPT: Here is the script to run basic class stats,
basic-class-stats.sh
-----------------------------------------------------------------------
#!/bin/sh
. ./get-export-dir.sh

#this function gets the most recent date of the dumps in
test02.sindice.net:/user/sindice/export
DATE=$(getExportDir)

. ./run-stats-job.sh
#Call the function to run the job and create the text file, Below is the
signature to call the function.
#function, stat-name, export-date, class, number of fields, fields to group by(0 for none),
#field to sort by, field to filter on, threshold to filter records (less than), top N 
#(keep only top n)(0 to not filter), the type of the sort field (int or long), number of 
#reducers, other parameters
runStatsJob basic-class-stats $DATE
org.sindice.core.analytics.stats.basic.cli.BasicClassStatsCLI 5 0 2 2 5 100000 int 20

-----------------------------------------------------------------------

The function runsStatsJob performs two tasks.  First it runs that stats jobs with
the output as a sequence file, this contains the full results and is stored in the
MAP directory.  The second task is if filters, groups, sorts, and takes the top n 
records, the output of this is a text file and is stored in the TEXT directory.
runStatsJobs takes several parameter. You can change the filter field and threshold,
grouping fields, sort fields, and number of output records These are the only values
that should be modified in the scripts. If you want to change the output directory 
or the input directory, you change this in run-stats-job.sh. 

Note that you can group by more than one field, for this input you can include
multiple values that are separated by commas with no spaces (Example: 1,2).
If the groupby field is 0, the output will be sorted by the sort field.  If the 
groupby fields are not zero than the output records will first be
grouped by the groupby fields and then sorted by the sort field.