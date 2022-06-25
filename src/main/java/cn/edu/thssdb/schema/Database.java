package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.common.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;


// TODO: lock control
// TODO Query: please also add other functions needed at Database level.

public class Database {

    private String databaseName;
    private HashMap<String, Table> tableMap;
    ReentrantReadWriteLock lock;

    public Database(String databaseName) {
        this.databaseName = databaseName;
        this.tableMap = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        File tableFolder = new File(this.getDatabaseTableFolderPath());
        if (!tableFolder.exists())
            tableFolder.mkdirs();
        recover();
    }


    // Operations: (basic) persist, create tables
    private void persist() {
        // 把各表的元数据写到磁盘上
        for (Table table : this.tableMap.values()) {
            String filename = table.getTableMetaPath();
            ArrayList<Column> columns = table.columns;
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(filename);
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
                for (Column column : columns)
                    outputStreamWriter.write(column.toString() + "\n");
                outputStreamWriter.close();
                fileOutputStream.close();
            } catch (Exception e) {
                throw new FileIOException(filename);
            }
        }
    }

    public void create(String tableName, Column[] columns) {
        try {
            // TODO add lock control. finished
            lock.writeLock().lock();
            if (this.tableMap.containsKey(tableName))
                throw new DuplicateTableException(tableName);
            Table table = new Table(this.databaseName, tableName, columns);
            this.tableMap.put(tableName, table);
            this.persist();
        } finally {
            // TODO add lock control. finished
            lock.writeLock().unlock();
        }
    }

    public Table get(String tableName) {
        try {
            // TODO add lock control. finished
            lock.readLock().lock();
            if (!this.tableMap.containsKey(tableName))
                throw new TableNotExistException(tableName);
            return this.tableMap.get(tableName);
        } finally {
            // TODO add lock control. finished
            lock.readLock().unlock();
        }
    }

    public void drop(String tableName) {
        try {
            // TODO add lock control. finished
            lock.writeLock().lock();
            if (!this.tableMap.containsKey(tableName))
                throw new TableNotExistException(tableName);
            Table table = this.tableMap.get(tableName);
            String filename = table.getTableMetaPath();
            File file = new File(filename);
            if (file.isFile() && !file.delete())
                throw new FileIOException(tableName + " _meta  when drop a table in database");

            table.dropTable();
            this.tableMap.remove(tableName);
        } finally {
            // TODO add lock control. finished
            lock.writeLock().unlock();
        }
    }

    public void dropDatabase() {
        try {
            // TODO add lock control. finished
            lock.writeLock().lock();
            for (Table table : this.tableMap.values()) {
                File file = new File(table.getTableMetaPath());
                if (file.isFile() && !file.delete())
                    throw new FileIOException(this.databaseName + " _meta when drop the database");
                table.dropTable();
            }
            this.tableMap.clear();
            this.tableMap = null;
        } finally {
            // TODO add lock control. finished
            lock.writeLock().unlock();
        }
    }

    private void recover() {
        System.out.println("! try to recover database " + this.databaseName);
        File tableFolder = new File(this.getDatabaseTableFolderPath());
        File[] files = tableFolder.listFiles();
//        for(File f: files) System.out.println("...." + f.getName());
        if (files == null) return;

        for (File file : files) {
            if (!file.isFile() || !file.getName().endsWith(Global.META_SUFFIX)) continue;
            try {
                String fileName = file.getName();
                String tableName = fileName.substring(0, fileName.length() - Global.META_SUFFIX.length());
                if (this.tableMap.containsKey(tableName))
                    throw new DuplicateTableException(tableName);

                ArrayList<Column> columnList = new ArrayList<>();
                InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
                BufferedReader bufferedReader = new BufferedReader(reader);
                String readLine;
                while ((readLine = bufferedReader.readLine()) != null)
                    columnList.add(Column.parseColumn(readLine));
                bufferedReader.close();
                reader.close();
                Table table = new Table(this.databaseName, tableName, columnList.toArray(new Column[0]));
                System.out.println(table.toString());
                for (Row row : table)
                    System.out.println(row.toString());
                this.tableMap.put(tableName, table);
            } catch (Exception ignored) {
            }
        }
    }

    public void quit() {
        try {
            this.lock.writeLock().lock();
            for (Table table : this.tableMap.values())
                table.persist();
            this.persist();
        } finally {
            this.lock.writeLock().unlock();
        }
    }


    // TODO Query: please also add other functions needed at Database level.
    public QueryResult select(QueryTable[] queryTables) {
        // TODO: support select operations
        // return combined row from select from query table
        try {
            lock.readLock().lock();
            List<Row> rowList = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();

            if (queryTables.length == 1) {
                rowList.addAll(queryTables[0].getRow());
                columnNames.addAll(queryTables[0].getColumnNames());
            } else if (queryTables.length == 2) {
                List<Row> concatedRow = new ArrayList<>();

                LinkedList<List<String>> columnNameList = new LinkedList<>(List.of(queryTables[0].getColumnNames(),
                        queryTables[1].getColumnNames()));
                for (int i = 0; i < queryTables[0].getRow().size(); i++) {
                    for (int j = 0; j < queryTables[1].getRow().size(); j++) {
                        LinkedList<Row> rowPair = new LinkedList<>();

                        rowPair.add(queryTables[0].getRow(i));
                        rowPair.add(queryTables[1].getRow(j));
                        concatedRow.add(QueryResult.combineRow(rowPair));
                    }
                }
                rowList.addAll(concatedRow);
                columnNames.addAll(QueryResult.combineColumn(columnNameList));
            } else {
                return new QueryResult("Doesnt support select from more than 2 tables");
            }
            return new QueryResult(new QueryTable(rowList, columnNames));
        } finally {
            lock.readLock().unlock();
        }
    }


    // Find position
    public String getDatabasePath() {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + this.databaseName;
    }

    public String getDatabaseTableFolderPath() {
        return this.getDatabasePath() + File.separator + "tables";
    }

    public String getDatabaseLogFilePath() {
        return this.getDatabasePath() + File.separator + "log";
    }

    public static String getDatabaseLogFilePath(String databaseName) {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "log";
    }

    // Other utils.
    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getTableInfo(String tableName) {
        return get(tableName).toString();
    }

    public String toString() {
        if (this.tableMap.isEmpty()) return "{\n[DatabaseName: " + databaseName + "]\n" + Global.DATABASE_EMPTY + "}\n";
        StringBuilder result = new StringBuilder("{\n[DatabaseName: " + databaseName + "]\n");
        for (Table table : this.tableMap.values())
            if (table != null)
                result.append(table.toString());
        return result.toString() + "}\n";
    }
}
