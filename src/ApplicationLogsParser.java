import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ApplicationLogsParser {

	private static final String  entity = "entity";

	private static final String entitytype = "entitytype";
	private static final String events = "events";
	private static final String otherinfo = "otherinfo";
	private static final String relatedEntities = "relatedEntities";
	private static final String TEZ_DAG_ID = "TEZ_DAG_ID";						// Done without record counts
	private static final String TEZ_VERTEX_ID = "TEZ_VERTEX_ID";				// Done without record counts
	private static final String TEZ_TASK_ID = "TEZ_TASK_ID";					// Done without record counts
	private static final String TEZ_TASK_ATTEMPT_ID = "TEZ_TASK_ATTEMPT_ID";
	
	// Command line options
	private static final String INPUT_FILE = "inputfile"; 
	private static final String INPUT_FOLDER = "inputfolder";
	private static final String OUTPUT_PATH = "outputPath";
	private static final String OUTPUT_PREFIX = "outputprefix";
	private static final String WRITE_TO_CONSOLE = "writetoconsole";
	
	private String inputFile;
	private String inputFolder;
	private String outPutFolder;
	private String outPutPrefix;
	
	private boolean writeToConsole;
	private Dag currentDag = null;
	private List<File> inputFileList;
	private List<Dag> dagList;
	private Options cmdLineOptions;
	private CommandLineParser cmdLineParser;
	
	public static void main(String[] args) throws JSONException, Exception {

		String applogFilePath = "//Users//mmokhtar//Downloads//schedulingissues//history.txt.appattempt_1406587903854_0285_000002";
		//applogFilePath = "//Users//mmokhtar//Downloads//schedulingissues//q17200g";
		//applogFilePath = "//Users//mmokhtar//Downloads//schedulingissues//loadorc";
		applogFilePath = "//Users//mmokhtar//Downloads//schedulingissues//loadtext";
		
		
		File applogFile = new File(applogFilePath);
		
		ApplicationLogsParser logParser = new ApplicationLogsParser();
	
		logParser.ParseCommandLineOptions(args);
		
		logParser.parseLogFiles();
		
		//logParser.readApplicationLogFile(applogFile);
			
		logParser.PrintSummary();
	}
	
	public ApplicationLogsParser() {
		super();
		this.dagList = new ArrayList<Dag>();
		
		this.inputFileList = new ArrayList<File>();
		
		// Construct the command line parsers
		cmdLineOptions = new Options();
		cmdLineParser = new GnuParser();

		// Set the command line options	
		cmdLineOptions.addOption(INPUT_FILE, true, "input log file to parse");
		cmdLineOptions.addOption(INPUT_FOLDER, true, "input folder to parse contained files");
		cmdLineOptions.addOption(OUTPUT_PATH, true, "out folder for summary file");
		cmdLineOptions.addOption(OUTPUT_PREFIX, true, "prefix for output file name");
		cmdLineOptions.addOption(WRITE_TO_CONSOLE,false , "flag to print to console");
		
	}
	
	// Parse the command line options
	public void ParseCommandLineOptions(String[] args)
	{
		try {
			CommandLine cmd = cmdLineParser.parse( cmdLineOptions, args);
			
			
			if ((cmd.hasOption(INPUT_FILE) && cmd.hasOption(INPUT_FOLDER)) || (!cmd.hasOption(INPUT_FILE) && !cmd.hasOption(INPUT_FOLDER)))
			{
				String error = "Parsing failed.  Reason: " + " Please use either " + INPUT_FILE + " or " + INPUT_FOLDER;
				throw new ParseException(error);
			}
			
			if(cmd.hasOption(INPUT_FILE))
			{
				inputFile = cmd.getOptionValue(INPUT_FILE);
				 File fileToParse = new File(inputFile);
				 if (fileToParse.isFile())
				 {
					 inputFileList.add(fileToParse);
				 }
				 else
				 {
					 throw new ParseException("Command line option " + INPUT_FILE + " excpects a file");
				 }
			}
			
			if(cmd.hasOption(INPUT_FOLDER))
			{
				inputFolder = cmd.getOptionValue(INPUT_FOLDER);
				 File filesToParse = new File(inputFolder);
				 FilenameFilter fileFilter = new FilenameFilter() {
					@Override
					public boolean accept(File dir, String name) {
						if(name.contains(".DS_Store"))
						{
							return false;
						}
						
						if(name.endsWith(".csv"))
						{
							return false;
						}
						return true;
					}
				};
						 
						 
				 if (filesToParse.isDirectory())
				 {
					 inputFileList = Arrays.asList(filesToParse.listFiles(fileFilter));	 
				 }
				 else
				 {
					 throw new ParseException("Command line option " + INPUT_FOLDER + " excpects a folder");
				 }
			}

			if(cmd.hasOption(OUTPUT_PATH))
			{
				outPutFolder = cmd.getOptionValue(OUTPUT_PATH);
			}
			
			if(cmd.hasOption(OUTPUT_PREFIX))
			{
				outPutPrefix = cmd.getOptionValue(OUTPUT_PREFIX);
			}
			
			if(cmd.hasOption(WRITE_TO_CONSOLE))
			{
				writeToConsole = true;
			}
			else
			{
				writeToConsole = false;
			}
			
		} catch( ParseException exp ) {
	        // oops, something went wrong
	        System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "summarizeLogs.jar", cmdLineOptions );

	    }
	}
	
	public void HandleDag (JSONObject dagJson) throws JSONException, Exception
	{
		if (currentDag== null)
		{
			currentDag = new Dag();
		}
			
		if (!dagJson.getString(entitytype).equals(TEZ_DAG_ID))
		{
			 throw new Exception("Check the dag provided to  HandleDag : " + dagJson.getString(entitytype) + " "+ dagJson.toString());
		}
		
		for (String key : dagJson.keySet()) {
			
			 switch (key) {
			 
			 // Parse events such as DAG_INITIALIZED, DAG_STARTED , DAG_SUBMITTED and DAG_FINISHED
			 case events:  
				 {
					// String eventType = dagJson.getString(events);
					 JSONArray ja =  dagJson.getJSONArray(events);
					 currentDag.HandleDagEvents(ja);
				 };
	             break;
			 
				 case entity:  
				 {
					 currentDag.setEntity((String) dagJson.get(entity));
				 };
	             break;
	             
				 case otherinfo:
				 {
					 currentDag.setOtherInfo(dagJson.getJSONObject(otherinfo));
				 }
				 
				 case relatedEntities:
				 {
					 currentDag.handleRelatedEntities(dagJson);
				 }
			 }
		}
		
		// If we are done with this dag save it off to the list and start a new one.
		// as there can be multiple dags per file
		if (currentDag.isDagParsingComplete())
		{
			dagList.add(currentDag);
			currentDag = new Dag();
		}
	}

	public void HandleVertex (JSONObject dagJson) throws JSONException, Exception
	{
		if (currentDag== null)
		{
			throw new Exception("We are trying to parse a Vertex without a dag" + dagJson.toString());	
		}
		
		if (!dagJson.getString(entitytype).equals(TEZ_VERTEX_ID))
		{
			 throw new Exception("Check the dag provided to  HandleVertex : " + dagJson.getString(entitytype) + " "+ dagJson.toString());
		}
		
		// The JSON has a flat structured, so this work is needed :( 
		currentDag.HandleVertexEvents(dagJson);
	}
	
	public void HandleTask (JSONObject dagJson) throws JSONException, Exception
	{
		if (currentDag== null)
		{
			throw new Exception("We are trying to parse a Vertex without a dag" + dagJson.toString());	
		}
		
		if (!((dagJson.getString(entitytype).equals(TEZ_TASK_ID)) || (dagJson.getString(entitytype).equals(TEZ_TASK_ATTEMPT_ID))))
		{
			 throw new Exception("Check the dag provided to  HandleTask : " + dagJson.getString(entitytype) + " "+ dagJson.toString());
		}
		
		// The JSON has a flat structured, so this work is needed :( 
		currentDag.HandleTaskEvents(dagJson);
	}
	
	public void PrintSummary() throws IOException
	{
		
		
		for (Dag td : dagList)
		{
			List<String> miscCountersHeader = td.GetaggregatedInfoKeys();

			// DAG
			
			if (outPutFolder.isEmpty() || writeToConsole)
			{
				// Print the DAG header 
				System.out.println(dagList.get(0).getDagSummaryHeader());
	
				// Print the DAG values
				System.out.println(td.getDagSummaryValues());	
				
				// Vertex
				td.PrintVertexSummary(miscCountersHeader);
				System.out.println("\n" + Task.getTaskSummaryHeader(miscCountersHeader));
	
				// Task 
				td.PrintTaskSummary(miscCountersHeader);
				System.out.println("\n");
			}
			
			if (outPutFolder.length() > 0)
			{
				File outFolder = new File(outPutFolder);
				
				// Create the folder if it doesn't exist
				if (!outFolder.exists())
				{
					outFolder.mkdir();
				}
				
				String summaryFileName = outPutFolder + "//"+outPutPrefix + "-" + td.getDagApplicationId()+"-"+td.getEntity()+".csv";
				
				FileWriter fileWriter = new FileWriter(summaryFileName);
	
				fileWriter.write(dagList.get(0).getDagSummaryHeader()+"\n");
				fileWriter.write(td.getDagSummaryValues()+"\n");
				fileWriter.write("\n");
				
				for (String vertexSummaryLine : td.getVertexSummary(miscCountersHeader))
				{
					fileWriter.write(vertexSummaryLine+"\n");
				}
				
				
				for (String taskSummaryLine : td.getTaskSummary(miscCountersHeader))
				{
					fileWriter.write(taskSummaryLine+"\n");
				}
				
				fileWriter.flush();
				fileWriter.close();
			}
		}		
	}
	
	@SuppressWarnings("finally")
	public void parseLogFiles() throws JSONException, Exception
	{
		for (File logFile : inputFileList)
		{
			try
			{
				readApplicationLogFile(logFile);
			}
			catch (Exception e)
			{
				System.err.print("Error parsing" + logFile.getAbsolutePath() + "\n" + e.getClass() + "\n" + e.getMessage()   + "\n"  );
			}
			finally
			{
				continue;
			}
		}
	}
	
	private void readApplicationLogFile(File appLogFile)
			throws JSONException, Exception {
		try {

			BufferedReader br = new BufferedReader(new FileReader(
					appLogFile.getPath()));
			String jsonLogLine = null;

			while ((jsonLogLine = br.readLine()) != null) {
				jsonLogLine = jsonLogLine.replaceAll("\\p{Cc}", "").replaceAll("[\u0000-\u001f]","");
				JSONObject obj = new JSONObject(jsonLogLine);

				if (obj.has(entitytype)) {

					String entityName = obj.getString(entitytype);
					
					// Parse the DAG info
					if (entityName.equals(TEZ_DAG_ID)) {
						
						HandleDag(obj);
					}
					
					// Parse the DAG info
					if (entityName.equals(TEZ_VERTEX_ID)) {
						
						HandleVertex(obj);
					}
					
					if (entityName.equals(TEZ_TASK_ID) || entityName.equals(TEZ_TASK_ATTEMPT_ID) ) {
						HandleTask(obj);
					}
				}
			}
			
			br.close();
		} catch (FileNotFoundException ex) {
			System.out.println(ex.getMessage());	
		}
	}

}
