import org.htmlparser.beans.LinkBean;
import org.htmlparser.beans.StringBean;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.AndFilter;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;  
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksDBException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Document;

import java.net.URL;
import java.net.URLConnection;
import java.net.MalformedURLException;

import java.util.Date;
import java.util.Vector;
import java.util.StringTokenizer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

import IRUtilities.*;
import java.io.*;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


class StopStem
{
  // StopStem Variables
	private Porter porter;
	private java.util.HashSet stopWords;
	
	// Constructor
	public StopStem(String str) throws Exception {
		super();
		porter = new Porter();
		stopWords = new java.util.HashSet();
    
		String line;
		BufferedReader reader = new BufferedReader(new FileReader(str));
		while ((line = reader.readLine()) != null) { stopWords.add(line);}
		reader.close();
	} 
	// Stemming
	public String stem(String str){ return porter.stripAffixes(str);}
	
	// Stopword
	public boolean isStopWord(String str){ return stopWords.contains(str);}
	
}

public class Phase1{

	// Phase1 Variables
	private RocksDB db;
	private static Options options= new Options();
	private static String url;
	
	// Constructors
	Phase1(String _url, String db_path) throws Exception {
		this.url = _url;
		this.options.setCreateIfMissing(true);
		this.db = RocksDB.open(options, db_path);
	}
	
	Phase1(String _url){ this.url = _url;} // overload constructor  
	
	// Extract Words from URL, Returns String Vector Array of Words
	public Vector<String> extractWords() throws ParserException {
		// use StringTokenizer to tokenize the result from StringBean
		Vector<String> result = new Vector<String>();
		StringBean bean = new StringBean();
		bean.setURL(url);
		bean.setLinks(false);
		String contents = bean.getStrings();
		StringTokenizer st = new StringTokenizer(contents);
		while (st.hasMoreTokens()) {result.add(st.nextToken());}
		
		return result;
	}
	
	// Extract Links from URL, Returns String Vector Array of Links
	public Vector<String> extractLinks() throws ParserException {
		Vector<String> result = new Vector<String>();
		LinkBean bean = new LinkBean();
		bean.setURL(url);
		URL[] urls = bean.getLinks();
		for (URL s : urls) { result.add(s.toString());}
		
		return result;            
	}
	
	// Add Word Entry to DB (DocX + Y)
	public void addEntry(String word, int x, int y) throws RocksDBException {
		if (word.length() > 1){
			byte[] content = db.get(word.getBytes());
		  
			// changed from doc to D to save space and use: as the delimiter for doc id, use; as the delimiter of pos
			// Create new key for new word
			if (content == null) {  content = ("D" + x + ":" + y +";").getBytes();}
		  
			// Append existing key for word
			else { content = (new String(content) + "D" + x + ":" + y +";").getBytes();}
			db.put(word.getBytes(), content);
		}
    }
	
		// Start to regret mate, this overload function is to punish you not think through !
	public static void addEntry(String word, int x, int y ,RocksDB dbx) throws RocksDBException {
		if (word.length() > 1){
			byte[] content = dbx.get(word.getBytes());
		  
			// changed from doc to D to save space and use: as the delimiter for doc id, use; as the delimiter of pos
			// Create new key for new word
			if (content == null) {  content = ("D" + x + ":" + y +";").getBytes();}
		  
			// Append existing key for word
			else { content = (new String(content) + "D" + x + ":" + y +";").getBytes();}
			dbx.put(word.getBytes(), content);
		}
    }
	
	
	// rearrage the format of word_db
	// during insertion it will be in a from of "Dx:y;" where x is the pageID ,y is the pos
	// this function will group all the same pageID record in a form of "Dx:y,z;" which y,z is the postion(s);
	// Never run this during searching! only call when a crawler is completed 	
	
	public static void util_wordlist(RocksDB word_db)throws RocksDBException{
		
		
		RocksIterator iter = word_db.newIterator();
		for(iter.seekToFirst(); iter.isValid(); iter.next()){
			// Get raw data array
			String raw = new String(iter.value());
			System.out.println(iter.key()+"   "+raw);
			// delimiter ;
			String [] segment  = raw.split(";");
			// Array for storing the temp result
			String [] temp_list = null;
			int list_count =0;
			
			// Do it for all the 
			for(int i = 0; i< segment.length ;i++){
			// delimiter : ,Before : only have pageID and count, after is pure pos  
			String [] block = segment[i].split(":");
			int j;
			// check if anything in the temp list have the same doc_ID
				for (j = 0; j< list_count;j++){
					if(block[0].equals(temp_list[j].substring(0,block[0].length()))){
					// if same ID exist perviously, append it with "," and the new value
					temp_list[j] = new String(temp_list[j]+","+block[1]);
					break;
					}
					// if it cannot be found and new instance to the temp_list
					// with the value of block[0] + ":" + block[1]
					// then increment count by 1
					temp_list[list_count] = new String(block[0]+":" + block[1]);
					
					list_count++;
				}
			}
			// After sorting all segemnt in the temp_list, join all segment into a single string
			// for each end of pageID record append ";" at the end
			String result = "";
			for (int i =0; i<list_count ; i++)
				result = new String(temp_list[i]+";");
			// put bcak the new value
			//word_db.put(iter.key(),result.getBytes());
			
			//Debug message
			//System.out.println(new String(iter.key())+result);
		}
	}
	
	// Add URL Entry to DB
	public int addUrl(String url , RocksDB url_db) throws RocksDBException
	{
		byte[] content = url_db.get(url.getBytes());
		int count;
		//System.out.println(url + "\t" + url.getBytes() + "\t" + content);
		
		// If the URL doesn't exist, Returns iterated count/ length (after clean the db folder)
		if (content == null) {
			count = 0;
			RocksIterator iter = url_db.newIterator();
			for(iter.seekToFirst(); iter.isValid(); iter.next()){count++;}
			content = (String.valueOf(count)).getBytes();
			url_db.put(url.getBytes(), content);
		}
		
		// Do nothing as the URL exists in DB, Returns original count
		else { 
			count = Integer.parseInt(new String(content));
		}
		return count;
	}
	
	public static String removeFront(String text){
		text = text.replace("https://", "");
		text = text.replace("http://", "");
		text = text.replace("www.", "");
		return text;
	}
	
	//add URL Relation to DB, forward is considered only
	public static void addRelation(String url, Vector<String> links, RocksDB relation_db, StopStem domain_id) throws RocksDBException{
		String status;
		url = removeFront(url);
		String[] target = url.split("/");
		for(int i = 0; i < links.size(); i++){
			boolean domain = false, self = false, extra = false, sub = false;
			if(relation_db.get(links.get(i).getBytes()) == null){
				if(removeFront(links.get(i)).equals(url)){
					self = true;
				}else{
					String[] compare = removeFront(links.get(i)).split("/");
					System.out.print(removeFront(links.get(i)));
					
					int length, other;
					if(target.length >= compare.length){
						length = compare.length;
						other = target.length;
					}else{
						length = target.length;
						other = compare.length;
					}
					for(int j = 0; j < length; j++){
						if(target[j].equals(compare[j])){
							//check with domain_Identifier
							if(!domain_id.isStopWord(target[j])){
								domain = true;
								System.out.print("\t (testing)");
							}
						}else if(domain == false){
							extra = true;
							break;
						}
					}
					if(domain == true && length != compare.length)
						sub = true;
				}
				if(self == true)
					status = "self";
				else if(extra == true)
					status = "external";
				else if(sub == true)
					status = "forward";
				else
					status = "backward";
				//System.out.println(" : status " + status + "\n---------------------------");
				relation_db.put(links.get(i).getBytes(), status.getBytes());
			}else{
				System.out.println(links.get(i) + " has the relation as \n" + new String(relation_db.get(links.get(i).getBytes())));
				//System.out.println("---------------------------");
			}
		}
	}
	
	public static String getRelation(String link, RocksDB relation_db) throws RocksDBException{
		if(relation_db.get(link.getBytes()) != null){
			return new String(relation_db.get(link.getBytes()));
		}else
			return "Not registered";
	}
	
	//Count Occurences of word in String, Returns count
	public static int countOccurences(String str, String word) { 
		// Split the String by spaces in str_array 
		String str_array[] = str.split(" "); 
	   
		int count = 0; 

		// Increment count for every match
		for (String match: str_array){
			if (word.equals(match)) 
				count++; 
		} 
	  
		return count; 	
	} 
	
	// Prints out Word Entries and Occurences to Terminal and FileWrite
	public void printAll(PrintWriter printWriter,int i) throws Exception{  // Overload funtion for writer
		RocksIterator iter = db.newIterator();
		for(iter.seekToFirst(); iter.isValid(); iter.next()) {
			String temp = new String(iter.value());
			String delimiter = new String("D"+ i);
			
			if (countOccurences(temp,delimiter)>0){
				printWriter.print(new String(iter.key()) + " " + countOccurences(temp,delimiter) + ";");
				//System.out.print(new String(iter.key()) + " " + countOccurences(temp,delimiter) + ";");
			}
		}
		printWriter.println();
		//System.out.println();
	} 
	
	public static void writing (PrintWriter printWriter,Phase1 master,Phase1 child,int doc_id,int size,int size_format, RocksDB relation_db) throws Exception
	{
		//Prints Title to Terminal and FileWrite
		String title = Jsoup.connect(child.url).get().title();
		printWriter.println(title);			
		System.out.println(title);
		System.out.println(master.url);
		
		
		//Prints URL to Terminal and FileWrite
		printWriter.println(child.url);
		//System.out.println(child.url);
		
		//Prints Date to Terminal and FileWrite
		Date mod;
		URL u = new URL(child.url);		
		long date = u.openConnection().getLastModified();
		
		// Last Modified Date not mentionned in URL Header
		if (date == 0) { mod = new Date();} 
		
		// Last Modified Date mentionned in URL Header
		else {
			  System.out.print("Last Modification on:");
			  printWriter.print("Last Update on:");
			  mod = new Date(date);
		}
		  System.out.print(mod +"\t");
		  printWriter.print(mod +"\t");

		//Prints Size to Terminal and FileWrite
    
		// Size mentionned in URL Header
		if (size_format == 0){
			  System.out.println("SIZE: " + size + "B");
			  printWriter.println("SIZE: " + size + "B");
		}
		
		// Size not mentionned in URL Header, Count
		else if (size_format == 1){
			  System.out.println("SIZE = " + size + " Characters");
			  printWriter.println("SIZE = " + size + " Characters");
		}

		// Prints out Word Entries and Occurences to Terminal and FileWrite 
		master.printAll(printWriter,doc_id);
	
		//Child links
		Vector<String> clinks = child.extractLinks();
		for(int i = 0; i < clinks.size(); i++){
			  printWriter.println(clinks.get(i) + "\n" + master.getRelation(clinks.get(i), relation_db));
			  System.out.println(clinks.get(i) + "\n" + master.getRelation(clinks.get(i), relation_db));
		}
		printWriter.println("-------------------------------------------------------------------------------------------");
		System.out.println("-------------------------------------------------------------------------------------------");
	}
	
	public static void main (String[] args) throws Exception
	{
		StopStem stopStem = new StopStem("stopwords.txt");
		StopStem domain_iden = new StopStem("domain_Identifier.txt");
		FileWriter fileWriter = new FileWriter("../Phase1/spider_result.txt");
		PrintWriter printWriter = new PrintWriter(fileWriter);	
		RocksDB.loadLibrary();

		Options options = new Options();
		options.setCreateIfMissing(true);
		RocksDB url_db = RocksDB.open(options,"../Phase1/db/URL");
		RocksDB relation_db = RocksDB.open(options, "../Phase1/db/Relation");
		RocksDB title_db = RocksDB.open(options, "../Phase1/db/Title");

		Phase1 crawler = new Phase1("http://www.cse.ust.hk/","../Phase1/db/word");
		Vector<String> plinks = crawler.extractLinks();
		
		
		// Parent Link: One Off
		int doc_id = crawler.addUrl(crawler.url,url_db);
		Vector<String> words = crawler.extractWords();
		for(int i = 0; i < words.size(); i++){   //insert word into crawler
			if (stopStem.isStopWord(words.get(i)) == false){
				String wd = stopStem.stem(words.get(i));
				if (stopStem.isStopWord(wd) == false){
					crawler.addEntry(wd,doc_id ,i);
				}
			}
		}
		// Get the title
		String title = Jsoup.connect(crawler.url).get().title();
		String [] t_words= title.split(" ");
		for(int i = 0; i < t_words.length; i++){   //insert word into title_db
			if (stopStem.isStopWord(t_words[i]) == false){
				String wd = stopStem.stem(t_words[i]);
				if (stopStem.isStopWord(wd) == false){
					addEntry(wd,doc_id,i,title_db);
				}
			}
		}
		
		crawler.addRelation(crawler.url, plinks, relation_db,domain_iden);
		
		//get the size and write to the file
		URL u = new URL(crawler.url);							
		URLConnection uc = u.openConnection();
		int psize = uc.getContentLength();
		System.out.println("1st link");
		if (psize == -1){
			int totalchar = 0; 
			for (String count: words){ totalchar += count.length();};
			psize = totalchar;
			writing (printWriter,crawler,crawler,doc_id,psize,1, relation_db);
		}else{
			writing (printWriter,crawler,crawler,doc_id,psize,0, relation_db);
		}
		
						
		//2. recursive part
		for (int j = 1; j < 31;j++){
			Phase1 child = new Phase1(plinks.get(j)); //plink is a string vector from line 250 (just after main)
			Vector<String> cwords = child.extractWords();
			doc_id = child.addUrl(child.url,url_db);
			
			for(int i = 0; i < cwords.size(); i++){  // insert all word in a page 
				if (stopStem.isStopWord(cwords.get(i)) == false){
					String wd = stopStem.stem(cwords.get(i));
					if (stopStem.isStopWord(wd) == false){
						crawler.addEntry(wd,doc_id ,i);
					}
				}
			}
			
			// Get the title
			title = Jsoup.connect(child.url).get().title();
			t_words= title.split(" ");
			for(int i = 0; i < t_words.length; i++){   //insert word into title_db
				if (stopStem.isStopWord(t_words[i]) == false){
					String wd = stopStem.stem(t_words[i]);
					if (stopStem.isStopWord(wd) == false){
						addEntry(wd,doc_id,i,title_db);
					}
				}
			}
		
			child.addRelation(child.url, child.extractLinks(), relation_db,domain_iden);
			
			//get the size and write to the file
			u = new URL(child.url);							
			uc = u.openConnection();
			int size = uc.getContentLength();
			if (size == -1){
				int totalchar = 0; for (String count: cwords){ totalchar += count.length();};
				size = totalchar;
				writing (printWriter,crawler,child,doc_id,size,1, relation_db);
			}else{
				writing (printWriter,crawler,child,doc_id,size,0, relation_db);
			}
		}
		
		printWriter.close();
	}	
}
	