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

	public ApplicationLogsParser() {
		super();
		this.dagList = new ArrayList<tezdag>();
	}

	private static final String TEZ_DAG_ID = "TEZ_DAG_ID";						// Done
	private static final String TEZ_VERTEX_ID = "TEZ_VERTEX_ID";
	private static final String entitytype = "entitytype";
	private static final String events = "events";
	private static final String otherinfo = "otherinfo";
	private static final String  entity = "entity";
	private List<tezdag> dagList;
	private tezdag currentDag = null;
	
	public void handleTezVertex (JSONObject dagJson) throws JSONException, Exception
	{
		if (currentDag== null)
		{
			throw new Exception("We are trying to parse a Vertex without a dag" + dagJson.toString());	
		}
		
		if (!dagJson.getString(entitytype).equals(TEZ_VERTEX_ID))
		{
			 throw new Exception("Check the dag provided to  handleTezVertex : " + dagJson.getString(entitytype) + " "+ dagJson.toString());
		}
		
		// The JSON has a flat structured, so this work is needed :( 
		currentDag.HandleVertexEvents(dagJson);
	}
	
	public void handleTezDag (JSONObject dagJson) throws JSONException, Exception
	{
		if (currentDag== null)
		{
			currentDag = new tezdag();
		}
			
		if (!dagJson.getString(entitytype).equals(TEZ_DAG_ID))
		{
			 throw new Exception("Check the dag provided to  handleTezDag : " + dagJson.getString(entitytype) + " "+ dagJson.toString());
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
			 }
			
		}
		
		// If we are done with this dag save it off to the list and start a new one.
		// as there can be multiple dags per file
		if (currentDag.isDagParsingComplete())
		{
			dagList.add(currentDag);
			currentDag = new tezdag();
		}
	}

	private long readApplicationLogFile(File appLogFile)
			throws JSONException, Exception {
		long timeStamp = 0;
		try {

			BufferedReader br = new BufferedReader(new FileReader(
					appLogFile.getPath()));
			String jsonLogLine = null;

			int rowNum = 0;

			while ((jsonLogLine = br.readLine()) != null) {
				jsonLogLine = jsonLogLine.replaceAll("\\p{Cc}", "").replaceAll("[\u0000-\u001f]","");
				JSONObject obj = new JSONObject(jsonLogLine);

				if (obj.has(entitytype)) {

					String entityName = obj.getString(entitytype);
					
					// Parse the DAG info
					if (entityName.equals(TEZ_DAG_ID)) {
						
						handleTezDag(obj);
					}
					
					// Parse the DAG info
					if (entityName.equals(TEZ_VERTEX_ID)) {
						
						handleTezVertex(obj);
					}
				}

				rowNum++;
			}
			
			System.out.println(dagList.get(0).getDagSummaryHeader());
			
			for (tezdag td : dagList)
			{
				System.out.println(td.getDagSummaryValues());	
			}
			
			System.out.println(Vertex.getVertexSummaryHeader());
			
			for (tezdag td : dagList)
			{
				td.PrintVertexSummary();
			}

			
			
			br.close();
		} catch (FileNotFoundException ex) {

		}

		return timeStamp;
	}

	public static void main(String[] args) throws JSONException, Exception {
		// TODO Auto-generated method stub

		String applogFilePath = "//Users//mmokhtar//Downloads//schedulingissues//history.txt.appattempt_1406587903854_0285_000002";
		File applogFile = new File(applogFilePath);
		ApplicationLogsParser logParser = new ApplicationLogsParser();
		logParser.readApplicationLogFile(applogFile);

	}

}
