package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.*;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {
    private List<Row> rows;
    private List<String> columnNames;
    private Iterator<Row> rowListIterator;

    private List<String> column2ColumnName(List<Column> columns) {
        List<String> columnNames = new ArrayList<>();
        for (Column c : columns) {
            columnNames.add(c.getColumnName());
        }
        return columnNames;
    }

    public QueryTable(List<Row> tRows, List<String> tColumnNames) {
        // TODO: finished
        rows = new ArrayList<>(tRows);
        columnNames = tColumnNames;

        rowListIterator = rows.iterator();
    }

//    public QueryTable(List<Row> tRows, List<Column> columns) {
//        // TODO
//        rows = new ArrayList<>(tRows);
//        columnNames = column2ColumnName(columns);
//
//        rowListIterator = rows.iterator();
//    }

    public QueryTable(Table t) {
        // TODO: finished
        rows = new ArrayList<>();
        columnNames = column2ColumnName(t.columns);

        Iterator<Row> rowIterator = t.iterator();
        while (rowIterator.hasNext()) {
            Row r = rowIterator.next();
            rows.add(r);
        }

        rowListIterator = rows.iterator();
    }

    @Override
    public boolean hasNext() {
        // TODO: finished
        return rowListIterator.hasNext();
    }

    @Override
    public Row next() {
        // TODO: finished
        return rowListIterator.next();
    }

    public Row getFirst() {
        if (rows.isEmpty()) {
            return null;
        }
        return rows.get(0);
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Row> getRow() {
        return rows;
    }

    public Row getRow(int i) {
        return rows.get(i);
    }
}