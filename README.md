SummarizeTezLogsForHive
=======================

*   Tool used to summarize Tez log files for Hive queries, the output format provides drill down ability all the way down to the task level.

Usage: summarizeLogs.jar
*	-inputfile <arg>      input log file to parse.
*	-inputfolder <arg>    input folder to parse contained files.
*	-outputPath <arg>     out folder for summary file.
*		-outputprefix <arg>   prefix for output file name.
*		-writetoconsole       flag to print to console.
		

Example:
 
  java -jar parsetezlogs.jar -inputfolder /Users/mmokhtar/tez-history/branch-0.14/hive-run -outputPath /Users/mmokhtar/tez-history/branch-0.14/hive-run
