package cn.edu.thssdb.parser;

// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.ValueFormatInvalidException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 * K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 * K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 * <p>
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 * <p>
 * We give you a few examples to convert the tree, including create/drop/quit.
 * You need to finish the codes for parsing the other rooted trees marked TODO.
 */

public class ImpVisitor extends SQLBaseVisitor<Object> {
    private Manager manager;
    private long session;

    public ImpVisitor(Manager manager, long session) {
        super();
        this.manager = manager;
        this.session = session;
    }

    private Database GetCurrentDB() {
        Database currentDB = manager.getCurrentDatabase();
        if (currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null) return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        return null;
    }

    /**
     * 创建数据库
     */
    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        try {
            manager.createDatabaseIfNotExists(ctx.database_name().getText().toLowerCase());
            manager.persist();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create database " + ctx.database_name().getText() + ".";
    }

    /**
     * 删除数据库
     */
    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            manager.deleteDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop database " + ctx.database_name().getText() + ".";
    }

    /**
     * 切换数据库
     */
    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        try {
            manager.switchDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Switch to database " + ctx.database_name().getText() + ".";
    }

    /**
     * 删除表格
     */
    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        try {
            GetCurrentDB().drop(ctx.table_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop table " + ctx.table_name().getText() + ".";
    }

    /**
     * TODO: finished
     * 创建表格 create
     * supports create table table_name ( attr1 type1, attr2 type2, attr3 type3 not null primary key)
     *          create table table_name ( attr1 type1, attr2 type2, attr3 type3 not null, PRIMARY KEY (attr1) )
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        try {
            Set<String> primaryKeys = new HashSet<>();
            if (ctx.table_constraint() != null && ctx.table_constraint().K_PRIMARY() != null && ctx.table_constraint().K_KEY() != null) {
                // PRIMARY KEY (a,b)
                for (SQLParser.Column_nameContext nameContext : ctx.table_constraint().column_name()) {
                    primaryKeys.add(nameContext.getText());
                }
            }

            List<Column> columnList = new ArrayList<>();
            for (int i = 0; i < ctx.column_def().size(); i++) {
                SQLParser.Type_nameContext typeName = ctx.column_def(i).type_name();
                ColumnType columnType = ColumnType.LONG;
                int isPrimary = 0;
                boolean notNull = false;
                int maxLen = 1;
                if (typeName.T_STRING() != null) {
                    columnType = ColumnType.STRING;
                    maxLen = Integer.parseInt(ctx.column_def(i).type_name().NUMERIC_LITERAL().toString());
                } else if (typeName.T_INT() != null) {
                    columnType = ColumnType.INT;
                } else if (typeName.T_LONG() != null) {
                    columnType = ColumnType.LONG;
                } else if (typeName.T_FLOAT() != null) {
                    columnType = ColumnType.FLOAT;
                } else if (typeName.T_DOUBLE() != null) {
                    columnType = ColumnType.DOUBLE;
                }
                if (primaryKeys.contains(ctx.column_def(i).column_name().getText())) {
                    isPrimary = 1;
                }

                for (SQLParser.Column_constraintContext columnConstraint : ctx.column_def(i).column_constraint()) {
                    System.out.println(columnConstraint);
                    if (columnConstraint.K_NOT() != null && columnConstraint.K_NULL() != null) {
                        notNull = true;
                    }
                    if (columnConstraint.K_PRIMARY() != null && columnConstraint.K_KEY() != null) {
                        isPrimary = 1;
                    }
                }
                Column column = new Column(ctx.column_def(i).column_name().getText(), columnType, isPrimary, notNull, maxLen);
                columnList.add(column);
            }
            GetCurrentDB().create(ctx.table_name().getText(), columnList.toArray(new Column[0]));
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create table " + ctx.table_name().getText() + ".";
    }

    /**
     * TODO: finished
     * 表格项插入 insert
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        List<Column> column = manager.currentDatabase.get(ctx.table_name().getText()).columns;
        for (int i = 0; i < ctx.value_entry().size(); i++) {
            SQLParser.Value_entryContext valueEntry = ctx.value_entry(i);

            List<Cell> cells = new ArrayList<>();
            for (int j = 0; j < valueEntry.literal_value().size(); j++) {
                SQLParser.Literal_valueContext value = valueEntry.literal_value(j);
                Column currentCol = column.get(j);
                if (value.STRING_LITERAL() != null) {
                    String v = value.STRING_LITERAL().getText();
                    cells.add(new Cell(v));
                } else if (value.NUMERIC_LITERAL() != null) {
                    String numericStr = value.NUMERIC_LITERAL().getText();
                    switch (currentCol.getColumnType()) {
                        case INT:
                            Integer vInt = Integer.parseInt(numericStr);
                            cells.add(new Cell(vInt));
                            break;
                        case LONG:
                            Long vLong = Long.parseLong(numericStr);
                            cells.add(new Cell(vLong));
                            break;
                        case FLOAT:
                            Float vFloat = Float.parseFloat(numericStr);
                            cells.add(new Cell(vFloat));
                            break;
                        case DOUBLE:
                            Double vDouble = Double.parseDouble(numericStr);
                            cells.add(new Cell(vDouble));
                            break;
                        case STRING:
                            cells.add(new Cell(numericStr));
                            break;
                    }
                } else if (value.K_NULL() != null) {
                    cells.add(new Cell(null));
                } else {
                    throw new ValueFormatInvalidException("Type not found");
                }

            }
            Row row = new Row(cells.toArray(new Cell[0]));
            GetCurrentDB().get(ctx.table_name().getText()).insert(row);
        }
        return "Inserted into " + ctx.table_name().getText() + ".";
    }

    /**
     * TODO
     * 表格项删除 delete
     * DELETE FROM table1;
     * DELETE FROM table1 WHERE attr1=value1;
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        return "Delete";
    }

    /**
     * TODO
     * 表格项更新 update
     * UPDATE tableName SET attr1=value1;
     * UPDATE tableName SET attr1=value1 WHERE attr2=value2;
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        return "update";
    }

    /**
     * TODO
     * 表格查询 select
     * SELECT column1 FROM table1
     * SELECT column1, column2 FROM table1, table2 WHERE attr1=value1
     * SELECT table1.column1, table2.column2 FROM table1, table2 WHERE table1.attr1=table2.attr1
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        List<QueryTable> queryTables = new ArrayList<>();
        for (int i = 0; i < ctx.table_query().size(); i++) {
            QueryTable qt = new QueryTable(GetCurrentDB().get(ctx.table_query(i).getText()));
            queryTables.add(qt);
            if (ctx.K_WHERE() != null) {
                // parse where
            }
            if (ctx.multiple_condition() != null) {
                System.out.println(ctx.multiple_condition().getText());
            }
            System.out.println(ctx.result_column(0).getText());
        }
        return GetCurrentDB().select(queryTables.toArray(new QueryTable[0]));
    }

    /**
     * 退出
     */
    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        try {
            manager.quit();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Quit.";
    }

    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }
}
