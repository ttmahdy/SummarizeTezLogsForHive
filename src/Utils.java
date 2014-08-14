import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;


public class Utils {

	static final String OUTPUT_STRING = "OUTPUT";
	static final String INPUT_STRING = "INPUT";
	static final String MISC_STRING = "MISC";
	private static final String counterName = "counterName";
	private static final String counterValue = "counterValue";
	
	public static void parseKeyValuePairs(JSONArray ja, JSONObject jo , HashMap<String, String> hashmap )
	{
		if (ja != null)
		{
			parseKeyValuePairs(ja,hashmap);
			return;
		}
		
		hashmap.put(jo.getString(counterName), 
					 jo.get(counterValue).toString());
	}
	
	public static void parseKeyValuePairs(JSONArray ja, HashMap<String, String> hashmap )
	{
		 for (int j = 0 ; j < ja.length(); j++)
		 {
			 hashmap.put(ja.getJSONObject(j).getString(counterName), 
					 ja.getJSONObject(j).get(counterValue).toString());
		 }
	}

	
	private static void AggregateHelper(String key, Long value, HashMap<String,Long> destinationHashMap)
	{
		
		if ( destinationHashMap.containsKey(key))
		{
			Long oldValue = (destinationHashMap.get(key));
			destinationHashMap.put(key, oldValue + value);
		}
		else
		{
			destinationHashMap.put(key, value);	
		}
	}
	
	public static HashMap <String, Long> AggregateTaskCounters(HashMap<String, HashMap<String, String>> taskCountersHashMap)
	{
		HashMap<String, Long> aggregatedTaskCounters = new HashMap<>();
		
		for (String key : taskCountersHashMap.keySet())
		{
			if (key.contains(OUTPUT_STRING))
			{
				HashMap<String, String> currentOutputTask = taskCountersHashMap.get(key);
				
				for (String currentKey : currentOutputTask.keySet())
				{
					String modifiedKey = OUTPUT_STRING + "_" + currentKey;
					Long valueToAdd = Long.parseLong(currentOutputTask.get(currentKey));
					AggregateHelper(modifiedKey, valueToAdd, aggregatedTaskCounters );
				}
			}
			else if(key.contains(INPUT_STRING))
			{
				HashMap<String, String> currentInputTask = taskCountersHashMap.get(key);
				for (String currentKey : currentInputTask.keySet())
				{
					String modifiedKey = INPUT_STRING + "_" + currentKey;
					Long valueToAdd = Long.parseLong(currentInputTask.get(currentKey));
					AggregateHelper(modifiedKey, valueToAdd, aggregatedTaskCounters);
				}
			}
			else
			{
				HashMap<String, String> currentInputTask = taskCountersHashMap.get(key);
				for (String currentKey : currentInputTask.keySet())
				{
					String modifiedKey = MISC_STRING + "_" + currentKey;
					Long valueToAdd = Long.parseLong(currentInputTask.get(currentKey));
					AggregateHelper(modifiedKey, valueToAdd, aggregatedTaskCounters);
				}
			}
		}
		return aggregatedTaskCounters;
	}
	
}
