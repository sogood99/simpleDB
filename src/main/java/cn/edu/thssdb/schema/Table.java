package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.common.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.type.ColumnType.STRING;


// TODO lock control, variables init.

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Cell, Row> index;
    private int primaryIndex;

    // ADD lock variables for S, X locks and etc here.

    private Set<Long> sLockSet;
    private Set<Long> xLockSet;

    // TODO: table/tuple level locks
//    private Boolean hasLock(Long sessionId) {
//        // check if session already has a lock on table
//        return sLockSet.contains(sessionId) || xLockSet.contains(sessionId);
//    }

    public Boolean testSLock(Long sessionId) {
        return xLockSet.isEmpty() || xLockSet.contains(sessionId);
    }

    public void takeSLock(Long sessionId) {
        sLockSet.add(sessionId);
    }

    public void releaseSLock(Long sessionId) {
        sLockSet.remove(sessionId);
    }

    public Boolean testXLock(Long sessionId) {
        // check if sLock is empty or only contains themselves and there is no xlock on only by themselves
        return (sLockSet.isEmpty() || (sLockSet.size() == 1 && sLockSet.contains(sessionId))) && (xLockSet.isEmpty() || xLockSet.contains(sessionId));
    }

    public Boolean takeXLock(Long sessionId) {
        if (xLockSet.contains(sessionId)) {
            return false;
        }
        // check just in case
        assert xLockSet.isEmpty();

        xLockSet.add(sessionId);
        return true;
    } // 在test成功前提下拿X锁。返回值false表示session之前已拥有这个表的X锁。

    public void releaseXLock(Long sessionId) {
        xLockSet.remove(sessionId);
    }


    // Initiate: Table, recover
    public Table(String databaseName, String tableName, Column[] columns) {
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        this.primaryIndex = -1;

        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).isPrimary()) {
                if (this.primaryIndex >= 0)
                    throw new MultiPrimaryKeyException(this.tableName);
                this.primaryIndex = i;
            }
        }
        if (this.primaryIndex < 0)
            throw new MultiPrimaryKeyException(this.tableName);

        // TODO initiate lock status.
        xLockSet = new HashSet<>();
        sLockSet = new HashSet<>();

        recover();
    }

    private void recover() {
        // read from disk for recovering
        try {
            // TODO lock control : finished
            lock.writeLock().lock();
            ArrayList<Row> rowsOnDisk = deserialize();
            for (Row row : rowsOnDisk)
                this.index.put(row.getEntries().get(this.primaryIndex), row);
        } finally {
            // TODO lock control : finished
            lock.writeLock().unlock();
        }
    }


    // Operations: get, insert, delete, update, dropTable, you can add other operations.
    // remember to use locks to fill the TODOs

    public Row get(Cell primaryCell) {
        try {
            // TODO lock control : finished
            lock.readLock().lock();
            return this.index.get(primaryCell);
        } finally {
            // TODO lock control : finished
            lock.readLock().unlock();
        }
    }

    public void insert(Row row) {
        try {
            // TODO lock control : finished
            lock.writeLock().lock();
            this.checkRowValidInTable(row);
            if (this.containsRow(row))
                throw new DuplicateKeyException();
            this.index.put(row.getEntries().get(this.primaryIndex), row);
        } finally {
            // TODO lock control : finished
            lock.writeLock().unlock();
        }
    }

    public void delete(Row row) {
        try {
            // TODO lock control. : finished
            lock.writeLock().lock();
            this.checkRowValidInTable(row);
            if (!this.containsRow(row))
                throw new KeyNotExistException();
            this.index.remove(row.getEntries().get(this.primaryIndex));
        } finally {
            // TODO lock control. finished
            lock.writeLock().unlock();
        }
    }

    public void update(Cell primaryCell, Row newRow) {
        try {
            // TODO lock control. : finished
            lock.writeLock().lock();
            this.checkRowValidInTable(newRow);
            Row oldRow = this.get(primaryCell);
            if (this.containsRow(newRow))
                throw new DuplicateKeyException();   // 要么删并插入，要么抛出异常
            this.index.remove(primaryCell);
            this.index.put(newRow.getEntries().get(this.primaryIndex), newRow);
        } finally {
            // TODO lock control. : finished
            lock.writeLock().unlock();
        }
    }

    private void serialize() {
        try {
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " on serializing table in folder");
            File tableFile = new File(this.getTablePath());
            if (!tableFile.exists() ? !tableFile.createNewFile() : !tableFile.isFile())
                throw new FileIOException(this.getTablePath() + " on serializing table to file");
            FileOutputStream fileOutputStream = new FileOutputStream(this.getTablePath());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            for (Row row : this)
                objectOutputStream.writeObject(row);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            throw new FileIOException(this.getTablePath() + " on serializing");
        }
    }

    private ArrayList<Row> deserialize() {
        try {
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " when deserialize");
            File tableFile = new File(this.getTablePath());
            if (!tableFile.exists())
                return new ArrayList<>();
            FileInputStream fileInputStream = new FileInputStream(this.getTablePath());
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            ArrayList<Row> rowsOnDisk = new ArrayList<>();
            Object tmpObj;
            while (fileInputStream.available() > 0) {
                tmpObj = objectInputStream.readObject();
                rowsOnDisk.add((Row) tmpObj);
            }
            objectInputStream.close();
            fileInputStream.close();
            return rowsOnDisk;
        } catch (IOException e) {
            throw new FileIOException(this.getTablePath() + " when deserialize");
        } catch (ClassNotFoundException e) {
            throw new FileIOException(this.getTablePath() + " when deserialize(serialized object cannot be found)");
        }
    }

    public void persist() {
        try {
            // TODO add lock control. : finished
            lock.readLock().lock();
            serialize();
        } finally {
            // TODO add lock control. : finished
            lock.readLock().unlock();
        }
    }

    public void dropTable() { // remove table data file
        try {
            // TODO lock control. : finished
            lock.writeLock().lock();
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " when dropTable");
            File tableFile = new File(this.getTablePath());
            if (tableFile.exists() && !tableFile.delete())
                throw new FileIOException(this.getTablePath() + " when dropTable");
        } finally {
            // TODO lock control. : finished
            lock.writeLock().unlock();
        }
    }


    // Operations involving logic expressions.


    // Operations

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Cell, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().right;
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }

    private void checkRowValidInTable(Row row) {
        if (row.getEntries().size() != this.columns.size())
            throw new SchemaLengthMismatchException(this.columns.size(), row.getEntries().size(), "when check Row Valid In table");
        for (int i = 0; i < row.getEntries().size(); i++) {
            String entryValueType = row.getEntries().get(i).getValueType();
            Column column = this.columns.get(i);
            if (entryValueType.equals(Global.ENTRY_NULL)) {
                if (column.cantBeNull()) throw new NullValueException(column.getColumnName());
            } else {
                if (!entryValueType.equals(column.getColumnType().name()))
                    throw new ValueFormatInvalidException("(when check row valid in table)");
                Comparable entryValue = row.getEntries().get(i).value;
                if (entryValueType.equals(STRING.name()) && ((String) entryValue).length() > column.getMaxLength())
                    throw new ValueExceedException(column.getColumnName(), ((String) entryValue).length(), column.getMaxLength(), "(when check row valid in table)");
            }
        }
    }

    private Boolean containsRow(Row row) {
        return this.index.contains(row.getEntries().get(this.primaryIndex));
    }

    public String getTableFolderPath() {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "tables";
    }

    public String getTablePath() {
        return this.getTableFolderPath() + File.separator + this.tableName;
    }

    public String getTableMetaPath() {
        return this.getTablePath() + Global.META_SUFFIX;
    }

    public String toString() {
        StringBuilder s = new StringBuilder("Table " + this.tableName + ": ");
        for (Column column : this.columns) s.append("\t(").append(column.toString()).append(')');
        return s.toString() + "\n";
    }

}
