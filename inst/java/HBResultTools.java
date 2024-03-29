package Rpkg.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.Cell;

public class HBResultTools {
    Result res;
    ResultScanner scanner;
    Cell[] cells;
    
    public String[] newResult(Result r) {
	res = r;
	cells = r.rawCells();
	int n = cells.length;
	String[] val = new String[n];
	for (int i = 0; i < n; i++) {
	    Cell c = cells[i];
	    val[i] = new String(c.getValueArray(), c.getValueOffset(), c.getValueLength() /*, "UTF-8" */);
	}
	return val;
    }

    public String[] newScanner(ResultScanner s) throws java.io.IOException {
	scanner = s;
	return next();
    }
    
    public String[] next() throws java.io.IOException {
	res = scanner.next();
	if (res == null) {
	    scanner.close();
	    return null;
	}
	return newResult(res);
    }

    public String[] columns() {
	int n = cells.length;
	String[] val = new String[n];
	for (int i = 0; i < n; i++) {
	    Cell c = cells[i];
	    val[i] = new String(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength() /*, "UTF-8" */);
	}
	return val;
    }

    public void flush() {
	cells = null;
	res = null;
    }
}
