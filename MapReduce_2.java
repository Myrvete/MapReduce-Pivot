import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import javax.naming.Context;

import org.w3c.dom.Text;

public class MapReduce {

public static class PivotMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
	private LongWritable outputKey;
	private Text outputValue = new Text();
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//recuperation de la ligne
		String line = value.toString();
		
		//separation des mots
		String[] splitLine = line.split(";");
		
		int i=1;
		for(String word: splitLine ){
			
			outputKey = new LongWritable(i);
			outputValue.set(word.trim());
			
			//List(key2, value2)
			context.write(outputKey, outputValue);
			i++;
		}
	}
}	
	
public static class PivotReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
		
	private LongWritable outputKey;	
    private Text newLine = new Text();
        
	public void reduce(LongWritable key, ArrayList<Text> values, Context context) throws IOException, InterruptedException {
	
	    Iterator<Text> it = values.iterator();

	    String s = "";
	    int i = 1;
	    //reconstruction des lignes
	    while (it.hasNext()) {
	    	outputKey = new LongWritable(i);
		    s += it.next() + ";";
		}
	    newLine.set(s);

		//List(key3, value3)
	    context.write(outputKey, newLine);
	    i++;
	}
}
}



