import org.rocksdb.RocksDB;
import org.rocksdb.Options;  
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksDBException;

public class db_util{
	public static int countOccurences(String str, String word) { 
			String str_array[] = str.split(word); 	  
			return str_array.length; 	
		} 
		
	public static void util_wordlist(RocksDB word_db)throws RocksDBException{
			int word_count = 0;
			
			RocksIterator iter = word_db.newIterator();
			for(iter.seekToFirst(); iter.isValid(); iter.next()){
				// Get raw data array
				String raw = new String(iter.value());
				// Array for storing the temp result
				String [] temp_list = new String[countOccurences(raw,";")+1];
				// delimiter ;
				// Get rid of those invisible bastard
				if (raw.indexOf(";")<0)continue;
				String [] segment  = raw.split(";");	
				// make an assumption there is always at least 1 record exist for a entry
				int list_count =1;  
				
				String [] fblock = segment[0].split(":");
				temp_list[0] = new String(fblock[0] + ":" + fblock[1]);
		
				for(int i = 1; i< segment.length ;i++){
				// delimiter : ,Before : only have pageID and count, after is pure pos  
				String [] block = segment[i].split(":");

				int j;
				boolean set =false;
				// check if anything in the temp list have the same doc_ID
					for (j = 0; j< list_count;j++){
						if(block[0].equals(temp_list[j].substring(0,block[0].length()))){
						// if same ID exist perviously, append it with "," and the new value
						temp_list[j] = new String(temp_list[j]+","+block[1]);
						set =true;
						break;
						}
						
					}
					// if it cannot be found and new instance to the temp_list
					// with the value of block[0] + ":" + block[1]
					// then increment count by 1
					if (!set){
					temp_list[list_count] = new String(block[0]+":" + block[1]);
					list_count++;
					}
				}
				// After sorting all segemnt in the temp_list, join all segment into a single string
				// for each end of pageID record append ";" at the end
				String result = "";
				for (int i =0; i<list_count ; i++)
					result = new String(result+temp_list[i]+";");
				// put bcak the new value
				word_db.put(iter.key(),result.getBytes());
				// count the no.of word 
				word_count++;
				
				//Debug message
				System.out.println(new String(iter.key())+"\t"+result);	
			}
				System.out.println("This database consists of : "+word_count+" ID");		
		}
	
		public static void main(String[] args)throws Exception {
			
			RocksDB.loadLibrary();
			Options options = new Options();
			options.setCreateIfMissing(true);
			RocksDB word_db = RocksDB.open(options,"../Phase1/db/word");

			util_wordlist(word_db); // debug test
		}
}
	
	