package com.msrm.mongodb;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static java.util.Arrays.asList;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;;


public class MongoDBBasic {

    public static void main(String[] args) {
	MongoClient client = new MongoClient();
	MongoDatabase db = client.getDatabase("test");

	// counting number of objects in the collection
	System.out.printf("Initial count: %d%n", db.getCollection("restaurants").count());
	// insert(db);

	System.out.println("fetching : 1");
	// filtering object by its primary key i.e. _id
	printDocument(db.getCollection("restaurants").find(new Document("_id", 1)));
	printDocument(db.getCollection("restaurants").find(new Document("_id", "572b5cd46c6ec68351427e2c")));

	// Applying filter class to reduce search data
	printDocument(db.getCollection("restaurants").find(eq("_id", 1)));

	// Document object to filter data
	// printDocument(db.getCollection("restaurants").find(new
	// Document("borough", "Manhattan")));

	// Filter by Filters.eq() method
	System.out.println("filter by restaurant_id");
	printDocument(db.getCollection("restaurants").find(eq("restaurant_id", "41704624")));

	System.out.println("\nfilter by street");
	printDocument(db.getCollection("restaurants").find(eq("address.street", "Rajaji nagar")));

	System.out.println("\nfilter by zipcode");
	printDocument(db.getCollection("restaurants").find(eq("address.zipcode", "600041")));

	// Filtering array data from collections
	System.out.println("\nfiltering array data : score value is 102");
	printDocument(db.getCollection("restaurants").find(eq("grades.score", "102")));

	// Comparison operator
	System.out.println("\nGT LT EQ opeator");
	System.out.println("\nGreater than");
	printDocument(db.getCollection("restaurants").find(gt("grades.score", "100")));

	System.out.println("\nLess than or equal");
	printDocument(db.getCollection("restaurants").find(lte("grades.score", "2")));

	System.out.println("\nEqual to");
	printDocument(db.getCollection("restaurants").find(eq("grades.score", "102")));

	// Combination operator
	System.out.println("\nAND clause");
	printDocument(
		db.getCollection("restaurants").find(and(eq("grades.score", "2"), eq("address.zipcode", "600041"))));

	System.out.println("\nOR Clause");
	printDocument(
		db.getCollection("restaurants").find(or(gt("grades.score", "1"), eq("address.zipcode", "600041"))));

	// all documents in the restaurants collection, sorted first by the
	// borough field in ascending order, and then, within each borough, by
	// the "address.zipcode" field in ascending order:
	System.out.println("\nSorting data");
	printDocument(
		db.getCollection("restaurants").find(eq("address.zipcode", "600041"))
		.sort(new Document("borough", 1).append("address.zipcode", 1)));
	
	System.out.println("\nSorts class usage");
	printDocument(db.getCollection("restaurants").find(eq("address.zipcode", "600041"))
		.sort(ascending("borough", "address.zipcode")));


	// // printing all documents
	// printDocument(db.getCollection("restaurants").find());

	System.out.printf("%nlast count : %d%n", db.getCollection("restaurants").count());
	client.close();
    }

    public static void insert(MongoDatabase db) {
	DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
	//@formatter:off
	try {
	    db.getCollection("restaurants")
	    	.insertOne(
	    		new Document("_id", 2).append("address", new Document()
	    		.append("street", "Rajaji nagar")
	                    .append("zipcode", "600041")
	                    .append("building", "1480")
	                    .append("coord", asList(72.154521f, 454.545874f)
	                   )).append("borough", "test")
	    		.append("cuisine", "chinees")
	    		.append("grades", asList(
	    			new Document()
	    			.append("date", format.parse("2014-10-01T00:00:00Z"))
	    			.append("grade", "A1")
	    			.append("score", "102")
	    			, new Document()
	    			.append("date", format.parse("2014-10-11T00:00:00Z"))
	    			.append("grade", "D")
	    			.append("score", "1"), 
	    			new Document()
	    			.append("date", format.parse("2014-10-21T00:00:00Z"))
	    			.append("grade", "D")
	    			.append("score", "2")))
	    		.append("name", "Vella")
	                .append("restaurant_id", "41704624"));
	} catch (ParseException e) {
	    e.printStackTrace();
	}
	//@formatter:on
    }

    public static void printDocument(FindIterable<Document> docs) {
	docs.forEach(new Block<Document>() {
	    public void apply(Document doc) {
		System.out.println(doc);
	    }
	});
    }

}
