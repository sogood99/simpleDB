package cn.edu.thssdb.query;


import cn.edu.thssdb.exception.ValueFormatInvalidException;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.QueryResultType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Designed to hold general query result:
 * In SQL result, the returned answer could be QueryTable OR an error message
 * For errors, resultType = QueryResultType.MESSAGE, see Construct method.
 * For results, it will hold QueryTable.
 */

public class QueryResult {

    public final QueryResultType resultType;
    public final String errorMessage; // If it is an error.

    private List<MetaInfo> metaInfoInfos;
    private List<String> columnNames;

    public List<Row> results;

    public QueryResult(QueryTable[] queryTables) {
        this.resultType = QueryResultType.SELECT;
        this.errorMessage = null;
        // TODO : finished
        // initialize variable
        columnNames = new ArrayList<>();
        results = new ArrayList<>();
        metaInfoInfos = new ArrayList<>();

        if (queryTables.length != 0) {
            for (Column col : queryTables[0].getColumns()) {
                columnNames.add(col.getColumnName());
            }
        }
        for (QueryTable qt : queryTables) {
            while (qt.hasNext()) {
                results.add(qt.next());
            }
        }
    }

    public QueryResult(String errorMessage) {
        resultType = QueryResultType.MESSAGE;
        this.errorMessage = errorMessage;
    }

    public static Row combineRow(LinkedList<Row> rows) {
        // TODO
        List<Cell> cellList = new ArrayList<>();
        for (Row r : rows) {
            cellList.addAll(r.getEntries());
        }
        Row resultRow = new Row((ArrayList<Cell>) cellList);
        return resultRow;
    }

    public Row generateQueryRecord(Row row) {
        // TODO: probably no use
//        metaInfoInfos.add(new MetaInfo("tempTable", columnNames));
        throw new ValueFormatInvalidException("Not doing this rn");
//        return null;
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }
}
