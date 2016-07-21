package mycrawler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Myhbase {

static Configuration cfg = HBaseConfiguration.create();
public static void createTable() {
	String tableName= "commodity";
    System.out.println("start create table ......");  
    try {  
        HBaseAdmin hBaseAdmin = new HBaseAdmin(cfg);  
        if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表则提示  
        	 hBaseAdmin.disableTable(tableName);  
             hBaseAdmin.deleteTable(tableName);  
            System.out.println(tableName + " table is exist.");  
        }  
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
        tableDescriptor.addFamily(new HColumnDescriptor("n"));						//name  
        tableDescriptor.addFamily(new HColumnDescriptor("c"));						//category
        tableDescriptor.addFamily(new HColumnDescriptor("fm"));					//favorable means
        tableDescriptor.addFamily(new HColumnDescriptor("p"));						//price:o 	(original)  			:f 		(favorable)
        tableDescriptor.addFamily(new HColumnDescriptor("u"));						//url:o 		(original url)  	:ju 	(ju hua suan url)
        tableDescriptor.addFamily(new HColumnDescriptor("s"));						//sale:b		(before)  			:a 	(after)
        hBaseAdmin.createTable(tableDescriptor);  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  
    System.out.println("end create table ......");  
}  

/** 
 * @param args 
 */
public void test() {
    // TODO Auto-generated method stub
	String tablename = "hbase_tb";
    String columnFamily = "cf";
    try {
    	Myhbase.create(tablename, columnFamily);
    	Myhbase.put(tablename, "row1", columnFamily, "cl1", "hello world!");
    	Myhbase.get(tablename, "row1");
        Myhbase.scan(tablename);
        if(true==Myhbase.delete(tablename)) {
            System.out.println("Delete table:"+tablename+" success!");
        }
    } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
}

/**
 * create a table :table_name(columnFamily) 
 * @param tablename
 * @param columnFamily
 * @throws Exception
 */
public static void create(String tablename, String columnFamily) throws Exception {
    HBaseAdmin admin = new HBaseAdmin(cfg);
    if(admin.tableExists(tablename)) {
        System.out.println("table exists!");
        System.exit(0);
    }
    else {
        HTableDescriptor tableDesc = new HTableDescriptor(tablename);
        tableDesc.addFamily(new HColumnDescriptor(columnFamily));
        admin.createTable(tableDesc);
        System.out.println("create table success!");
    }
}

/**
 * put a row data into table 
 * @param tablename
 * @param row
 * @param columnFamily
 * @param column
 * @param data
 * @throws Exception
 */
public static void put(String tablename, String row, String columnFamily, String column, String data) throws Exception{
    HTable table = new HTable(cfg, tablename);
    Put p1 = new Put(Bytes.toBytes(row));
    p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
    table.put(p1);
    System.out.println("put '"+row+"', '"+columnFamily+":"+column+"', '"+data+"'");

}

/**
 * get a row data from a table
 * @param tablename
 * @param row
 * @throws Exception
 */
public static void get(String tablename, String row) throws Exception {
    HTable table = new HTable(cfg, tablename);
    Get get = new Get(Bytes.toBytes(row));
    Result result = table.get(get);
    System.out.println("Get: "+result);
}

/**
 * show all data from a table
 * @param tablename
 * @throws Exception
 */
public static void scan(String tablename) throws Exception {
    HTable table = new HTable(cfg, tablename);
    Scan s =new Scan();
    ResultScanner rs = table.getScanner(s);
    for(Result r:rs) {
        System.out.println("Scan: "+r);
    }
}

/**
 * delete a table's data
 * @param tablename
 * @return
 * @throws IOException
 */
public static boolean delete(String tablename) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(cfg);
    if(admin.tableExists(tablename)) {
        try {
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        	} catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            return false;
        	}
    	}
    return true;
	}

/** 
 * 根据 rowkey删除一条记录 
 *  
 * @param tablename 
 * @param rowkey 
 */  
public static void deleteRow(String tablename, String rowkey)  throws Exception{  
		HTable table = new HTable(cfg, tablename); 
		try {  
			List list = new ArrayList();  
			Delete d1 = new Delete(rowkey.getBytes());  
			list.add(d1);  
			
			table.delete(list);  
			System.out.println("删除行成功!");  

		} catch (IOException e) {  
			e.printStackTrace();  
		}  

	}

/** 
 * 单条件查询,根据rowkey查询唯一一条记录 
 *  
 * @param tableName 
 * @throws IOException 
 */  
public static void QueryByCondition(String tablename) throws IOException {  

	HTable table = new HTable(cfg, tablename); 
    try {  
        Get scan = new Get("531115135635".getBytes());// 根据rowkey查询  
        Result r = table.get(scan);  
        System.out.println("获得到rowkey:" + new String(r.getRow()));  
        for (KeyValue keyValue : r.raw()) {  
            System.out.println("列：" + new String(keyValue.getFamily())  +":"+  new String(keyValue.getQualifier())
                    + "====值:" + new String(keyValue.getValue()));  
        }  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  
}  


/** 
 * 单条件查询,根据rowkey和列名查询具体列的value 
 *  
 * @param tableName 
 * @throws IOException 
 */  
public static String QueryByCondition1(String tablename) throws IOException {

		HTable table = new HTable(cfg, tablename);
    	try {
        	Get scan = new Get("com.taobao.ju.detail:https/home.htm?id=10000022178228&item_id=531115135635".getBytes());// 根据rowkey查询
        	Result r = table.get(scan);
        	System.out.println("获得到rowkey:" + new String(r.getRow()));
        	for (KeyValue keyValue : r.raw()) {
        		if( "f".equalsIgnoreCase(new String(keyValue.getFamily())) && "cnt".equalsIgnoreCase(new String(keyValue.getQualifier())))
        		return  new String(keyValue.getValue());
        	}
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
	return " ";
	}
}