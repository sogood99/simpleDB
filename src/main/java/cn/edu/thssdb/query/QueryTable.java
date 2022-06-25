package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;

import java.util.*;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {
    private List<Row> rows;
    private List<Column> columns;
    private Iterator<Row> rowListIterator;

    QueryTable(List<Row> tRows, List<Column> tColumns) {
        // TODO :
        rows = new ArrayList<>(tRows);
        columns = new ArrayList<>(tColumns);
        rowListIterator = rows.iterator();
    }

    @Override
    public boolean hasNext() {
        // TODO:
        return rowListIterator.hasNext();
    }

    @Override
    public Row next() {
        // TODO
        return rowListIterator.next();
    }

    public Row getFirst() {
        if (rows.isEmpty()) {
            return null;
        }
        return rows.get(0);
    }

    public List<Column> getColumns() {
        return columns;
    }
}