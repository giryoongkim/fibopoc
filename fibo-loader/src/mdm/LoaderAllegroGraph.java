/******************************************************************************
Author: 	Gi Ryoong Kim
Date: 		June-17-2016
******************************************************************************/

package mdm;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.sql.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.relique.jdbc.csv.CsvDriver;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.ntriples.NTriplesWriter;
import org.openrdf.rio.rdfxml.RDFXMLWriter;

import com.franz.agraph.pool.AGConnPool;
import com.franz.agraph.pool.AGConnProp;
import com.franz.agraph.pool.AGPoolProp;
import com.franz.agraph.repository.AGAbstractRepository;
import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGFreetextIndexConfig;
import com.franz.agraph.repository.AGFreetextQuery;
import com.franz.agraph.repository.AGGraphQuery;
import com.franz.agraph.repository.AGQueryLanguage;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;
import com.franz.agraph.repository.AGTupleQuery;
import com.franz.agraph.repository.AGValueFactory;

public class LoaderAllegroGraph {

	// Static variables
    public static String SERVER_URL;
    public static String CATALOG_ID;
    public static String REPOSITORY_ID;
    public static String USERNAME;
    public static String PASSWORD;
    public static String JDBC_DRIVER;
    public static String JDBC_URL;
    public static String BASE_URI;
    public static String CONTEXT;
    public static String DATA_TABLE;
    public static String MAP_TABLE;

    
    

    /**
     * Creating a Repository Connection
     */
    public static AGRepositoryConnection connectAg(boolean close) throws Exception {
        // Tests getting the repository up. 
        println("\nStarting connection.....");
        AGServer server = new AGServer(SERVER_URL, USERNAME, PASSWORD);
        println("Server version: " + server.getVersion());
        println("Server build date: " + server.getBuildDate());
        println("Server revision: " + server.getRevision());
        println("Available catalogs: " + server.listCatalogs());
        AGCatalog catalog = server.getCatalog(CATALOG_ID);          // open catalog
        println("Available repositories in catalog " + 
                (catalog.getCatalogName()) + ": " + 
                catalog.listRepositories());
        closeAll();
        
        AGRepository myRepository = catalog.openRepository(REPOSITORY_ID);
        println("Got a repository.");
        myRepository.initialize();
        println("Initialized repository.");
        
        println("Repository is writable? " + myRepository.isWritable());
        
        AGRepositoryConnection conn = myRepository.getConnection();
        closeBeforeExit(conn);
        println("Got a connection.");
        println("Repository " + (myRepository.getRepositoryID()) +
                " is up! It contains " + (conn.size()) +
                " statements."              
                );
        List<String> indices = conn.listValidIndices();
        println("All valid triple indices: " + indices);
        indices = conn.listIndices();
        println("Current triple indices: " + indices);

        if (close) {
            // tidy up
        	conn.close();
            myRepository.shutDown();
            return null;
        }
        return conn;
    }
    
    /**
     * Add triples....
     */
    public static void addTripples(AGRepositoryConnection agConn) throws Exception{
		try{
	    	Connection jdbcConn  = connectJdbc(JDBC_URL);

	    	System.out.println("Loading Excel dataset....");
	        java.sql.Statement stmtData = jdbcConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
	        java.sql.Statement stmtMap = jdbcConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);	       
	        
            String sqlDataSet = "select * from "+DATA_TABLE;
            String sqlMapping = "select * from "+MAP_TABLE;
            
            
            ResultSet rsDataSet = stmtData.executeQuery(sqlDataSet);
            ResultSet rsMapping = stmtMap.executeQuery(sqlMapping);
            AGValueFactory f = agConn.getValueFactory();
            
            System.out.println("Excel loaded....");
            while(rsDataSet.next()){
            	System.out.println("Adding..." + rsDataSet.getObject(2));
            	
            	while (rsMapping.next()){
            		if (rsMapping.getString("TRIGGER_COLUMN") != null && (rsMapping.getString("TRIGGER_COLUMN").equals("N") 
            				|| rsMapping.getString("TRIGGER_VALUE").equals(rsDataSet.getString(rsMapping.getString("TRIGGER_COLUMN"))))){

            			if (!rsMapping.getString("TRIGGER_COLUMN").equals("N") ) System.out.println(rsDataSet.getString(rsMapping.getString("TRIGGER_COLUMN")));
            			if (rsMapping.getString("FLAG").equals("Literal")){
            				agConn.add( f.createURI(BASE_URI + rsDataSet.getString(rsMapping.getString("SOURCE")))
                					, f.createURI(rsMapping.getString("PREDICATE"))
                					 , f.createLiteral(rsDataSet.getString(rsMapping.getString("OBJECT"))/*,f.createURI(rsMapping.getString("DATATYPE"))*/)
                					 ,f.createURI(rsMapping.getString("CONTEXT")));
            			} else if (rsMapping.getString("FLAG").equals("Resource")){
            				agConn.add(f.createURI(BASE_URI + rsDataSet.getString(rsMapping.getString("SOURCE")))
                					, f.createURI(rsMapping.getString("PREDICATE"))
                					 , f.createURI(BASE_URI + rsDataSet.getString(rsMapping.getString("Object")))
                					 ,f.createURI(rsMapping.getString("Context")));
            			}else {
            				agConn.add(f.createURI(BASE_URI + rsDataSet.getString(rsMapping.getString("SOURCE")))
                					, f.createURI(rsMapping.getString("PREDICATE"))
                					 , f.createURI(rsMapping.getString("Object"))
                					 ,f.createURI(rsMapping.getString("Context")));
            			}
            			
            		}
            		
            		
            	}
            	rsMapping.first();
            	
            }
            agConn.commit();
			
		} catch(Exception e){
			throw e;
		} finally{
			  // jdbcConn.close();
			System.out.println("addTriple: Done");
		}
    }

    /**
     * Delete triples....
     */
    public static void deleteTripples(AGRepositoryConnection agConn, URI context) throws Exception{
    		try{
    			System.out.println("Removing triples...:"+context.toString());
    			agConn.remove( (Resource) null, (URI) null, (Value) null, context);
    			agConn.commit();
    		} catch(Exception e){
    			throw e;
    		} finally{
             
    		}
    }
    
    
    /***
     * Open JDBC connection
     */
    public static Connection connectJdbc(String JbdcUrl) throws Exception{
    	Class.forName(JDBC_DRIVER);
    	return DriverManager.getConnection(JbdcUrl,"","");
    	//return null;
    }
    
    /**
     * Main
     */
    public static void main(String[] args) throws Exception {
    	long now = System.currentTimeMillis();
    	AGRepositoryConnection agConn = null;
    	
    	init(args[0]);
    	
        try {
        	// BEGIN: Initializing Connections....
            agConn = connectAg(false);


            

            
            // END: Initializing Connections....
            
            AGRepository repository = (AGRepository) agConn.getRepository();
            ValueFactory f = repository.getValueFactory();
            URI context = f.createURI(CONTEXT);

            addTripples(agConn );
            //deleteTripples(agConn, context);
            

        } finally {
            closeAll();

            println("Elapsed time: " + (System.currentTimeMillis() - now)/1000.00 + " seconds.");
        }
    }
    
    public static void init(String arg){
    	Properties prop = new Properties();
    	InputStream input = null;
  
    	try {

    		input = new FileInputStream(arg);

    		// load a properties file
    		prop.load(input);

    		// get the property values
        	SERVER_URL = prop.getProperty("SERVER_URL");
            CATALOG_ID = prop.getProperty("CATALOG_ID");
            REPOSITORY_ID= prop.getProperty("REPOSITORY_ID");
            USERNAME= prop.getProperty("USERNAME");
            PASSWORD= prop.getProperty("PASSWORD");
            JDBC_DRIVER= prop.getProperty("JDBC_DRIVER");
            JDBC_URL= prop.getProperty("JDBC_URL");
            BASE_URI= prop.getProperty("BASE_URI");
            CONTEXT= prop.getProperty("CONTEXT");
            DATA_TABLE= prop.getProperty("DATA_TABLE");
            MAP_TABLE= prop.getProperty("MAP_TABLE");
            System.out.println(DATA_TABLE);
    		
    	} catch (IOException ex) {
    		ex.printStackTrace();
    	} finally {
    		if (input != null) {
    			try {
    				input.close();
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
    	}

    }

    
    
    public static void println(Object x) {
        System.out.println(x);
    }
    
    static void printRows(RepositoryResult<Statement> rows) throws Exception {
        while (rows.hasNext()) {
            println(rows.next());
        }
        rows.close();
    }

    static void printRows(String headerMsg, int limit, RepositoryResult<Statement> rows) throws Exception {
    	println(headerMsg);
        int count = 0;
        while (count < limit && rows.hasNext()) {
            println(rows.next());
            count++;
        }
        println("Number of results: " + count);
        rows.close();
    }

    static void printRows(String headerMsg, TupleQueryResult rows) throws Exception {
    	println(headerMsg);
    	try {
    		while (rows.hasNext()) {
    			println(rows.next());
    		}
    	} finally {
    		rows.close();
    	}
    }

    static void close(RepositoryConnection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            System.err.println("Error closing repository connection: " + e);
            e.printStackTrace();
        }
    }
    
    private static List<RepositoryConnection> toClose = new ArrayList<RepositoryConnection>();
    
    /**
     * This is just a quick mechanism to make sure all connections get closed.
     */
    private static void closeBeforeExit(RepositoryConnection conn) {
        toClose.add(conn);
    }
    
    private static void closeAll() {
        while (toClose.isEmpty() == false) {
            RepositoryConnection conn = toClose.get(0);
            close(conn);
            while (toClose.remove(conn)) {}
        }
    }
    
}
