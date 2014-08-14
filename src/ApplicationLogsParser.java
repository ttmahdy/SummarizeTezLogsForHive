import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

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
	public static void main(String[] args) throws JSONException, Exception {
		// TODO Auto-generated method stub

		String applogFilePath = "//Users//mmokhtar//Downloads//schedulingissues//history.txt.appattempt_1406587903854_0285_000002";
		//applogFilePath = "//Users//mmokhtar//Downloads//schedulingissues//q17200g";
		File applogFile = new File(applogFilePath);
		ApplicationLogsParser logParser = new ApplicationLogsParser();

		logParser.readApplicationLogFile(applogFile);
		
		logParser.PrintSummary();

	}
	private Dag currentDag = null;
	private List<Dag> dagList;
	
	public ApplicationLogsParser() {
		super();
		this.dagList = new ArrayList<Dag>();
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
	
	public void PrintSummary()
	{

		
		for (Dag td : dagList)
		{
			List<String> miscCountersHeader = td.GetaggregatedInfoKeys();
			
			System.out.println(dagList.get(0).getDagSummaryHeader());
			System.out.println(td.getDagSummaryValues());	
			
			td.PrintVertexSummary(miscCountersHeader);
			System.out.println("\n" + Task.getTaskSummaryHeader(miscCountersHeader));

			td.PrintTaskSummary(miscCountersHeader);
			System.out.println("\n");
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

		}
	}

}
