package casstest;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class cassajava {


	public static void main(String[] args) {
		
		System.out.print("***** Cassandra - Java Connection Tester ******");
		Cluster cluster;
		Session session;
		// Connect to the cluster and keyspace "nhom11" 127.0.0.1:9042/9160
		cluster = Cluster.builder().addContactPoint("localhost").build();	
		session = cluster.connect("nhom11");

		String createBooksTable = "CREATE TABLE IF NOT EXISTS books (" +
		        "book_id INT PRIMARY KEY," +
		        "title TEXT," +
		        "author TEXT," +
		        "published INT," +
		        "copies INT" +
		        ");";
		session.execute(createBooksTable);

		//Members
		String createMembersTable = "CREATE TABLE IF NOT EXISTS members (" +
		        "mem_id INT PRIMARY KEY," +
		        "name TEXT," +
		        "mail TEXT," +
		        "phone INT" +
		        ");";
		session.execute(createMembersTable);

		//BorrowedBooks
		String createBorrowedBooksTable = "CREATE TABLE IF NOT EXISTS borrow (" +
		        "borrow_id INT PRIMARY KEY," +
		        "book_id INT," +
		        "mem_id INT," +
		        "borrow_date INT," +
		        "return_date INT" +
		        ");";
		session.execute(createBorrowedBooksTable);
		
		// insert table books
		System.out.println("Inserting into Cassandra Database...");
		session.execute("INSERT INTO books (book_id, title, author, published, copies) VALUES (1, 'Memoirs of a Geisha', 'Arthur Sulzberger Golden', 1997, 3)");
		session.execute("INSERT INTO books (book_id, title, author, published, copies) VALUES (2, 'Twilight', 'Stephenie Meyer', 2005, 5)");
		session.execute("INSERT INTO books (book_id, title, author, published, copies) VALUES (3, 'The Princess Bride', 'William Goldman', 1973, 7)");  		
		session.execute("INSERT INTO books (book_id, title, author, published, copies) VALUES (4, 'Que noi', 'Vo Quang', 1973, 3)");
		session.execute("INSERT INTO books (book_id, title, author, published, copies) VALUES (5, 'Chiec Luoc Nga', 'Nguyen Quang Sang', 1966, 1)");
		
		session.execute("INSERT INTO members (mem_id, name, mail, phone) VALUES (1,'Nguyen Trong Thanh', 'thanh@mail', 0123456789)");
		session.execute("INSERT INTO members (mem_id, name, mail, phone) VALUES (2,'Tran Truong Huyen Linh', 'hlinh@mail', 0123456789)");
		session.execute("INSERT INTO members (mem_id, name, mail, phone) VALUES (3,'Le Thuy Linh', 'llinh@mail', 0123456789)");
		session.execute("INSERT INTO members (mem_id, name, mail, phone) VALUES (4,'Nguyen Thanh Binh', 'binh@mail', 0123456789)");
		
		session.execute("INSERT INTO borrow (borrow_id, book_id, mem_id, borrow_date, return_date) VALUES (1, 3, 3, 2024, null)");
		session.execute("INSERT INTO borrow (borrow_id, book_id, mem_id, borrow_date, return_date) VALUES (2, 2, 1, 2024, null)");		
		
		// Use select statement
		String bookid = null, author = null, copies = null, published = null, title = null,
				memid = null, name = null, mail= null, phone= null, 
				borrowid= null, borrowdate= null, returndate= null;
	
		// Use select statement
		//ResultSet results = session.execute("SELECT * FROM members WHERE mem_id = 3");
		
		
		//them info mem_id = 5
		//session.execute("INSERT INTO members (mem_id, name, mail, phone) VALUES (5,'unknown', 'unknow@mail', 0123456789)");
		//ResultSet results = session.execute("SELECT * FROM members WHERE mem_id = 5");
		
		//delete mem_id =5
		//ResultSet results = session.execute("delete from members where mem_id = 5");
		//System.out.println("Deleted row with mem_id = 5");
		
		// delete row
	//	session.execute("DELETE FROM books WHERE book_id = 3");
	//	System.out.println("Deleted row with book_id = 3");
//		
//		//update 
//		session.execute("UPDATE books SET copies = 30 WHERE book_id = 2");
//		ResultSet results = session.execute("SELECT * FROM books where book_id = 2");
//		
		//alter add
//		session.execute("ALTER TABLE borrow ADD note text;");
//		ResultSet results = session.execute("SELECT * FROM borrow where borrow_id = 1");
		
		//alter drop
		session.execute("ALTER TABLE borrow drop note;");
		ResultSet results = session.execute("SELECT * FROM borrow where borrow_id = 1");
		
		for (Row row : results) {
//			bookid = Integer.toString(row.getInt("book_id"));
//			title = row.getString("title");
//			author = row.getString("author");
//			published = Integer.toString(row.getInt("published"));
//			copies = Integer.toString(row.getInt("copies"));
			
//			memid = Integer.toString(row.getInt("mem_id"));
//			name = row.getString("name");
//			mail = row.getString("mail");
//			phone = Integer.toString(row.getInt("phone"));
			
			borrowid = Integer.toString(row.getInt("borrow_id"));;
			borrowdate = Integer.toString(row.getInt("borrow_date"));;
			returndate = Integer.toString(row.getInt("return_date"));;
			
//		System.out.println("Book ID: " + bookid);
//		System.out.println("Title: " + title);
//		System.out.println("Author: " + author);
//		System.out.println("Published: "+ published);
//		System.out.println("Copies: "+ copies);
//			
//		System.out.println("Member ID: "+memid);
//		System.out.println("Name: "+name);
//		System.out.println("Mail: "+mail);
//		System.out.println("Phone: "+phone);
		
		System.out.println("Borrow ID: "+borrowid);
		System.out.println("Borrow Date: "+borrowdate);
		System.out.println("Return Date: "+returndate);
		
		}
		// Clean up the connection by closing it
		cluster.close();
}
}
