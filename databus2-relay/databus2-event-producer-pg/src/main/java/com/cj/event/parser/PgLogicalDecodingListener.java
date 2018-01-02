package com.cj.event.parser;
// Generated from PgLogicalDecoding.g4 by ANTLR 4.5.2
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link PgLogicalDecodingParser}.
 */
public interface PgLogicalDecodingListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#logline}.
	 * @param ctx the parse tree
	 */
	void enterLogline(PgLogicalDecodingParser.LoglineContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#logline}.
	 * @param ctx the parse tree
	 */
	void exitLogline(PgLogicalDecodingParser.LoglineContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#txStatement}.
	 * @param ctx the parse tree
	 */
	void enterTxStatement(PgLogicalDecodingParser.TxStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#txStatement}.
	 * @param ctx the parse tree
	 */
	void exitTxStatement(PgLogicalDecodingParser.TxStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#commitTimestamp}.
	 * @param ctx the parse tree
	 */
	void enterCommitTimestamp(PgLogicalDecodingParser.CommitTimestampContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#commitTimestamp}.
	 * @param ctx the parse tree
	 */
	void exitCommitTimestamp(PgLogicalDecodingParser.CommitTimestampContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#dmlStatement}.
	 * @param ctx the parse tree
	 */
	void enterDmlStatement(PgLogicalDecodingParser.DmlStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#dmlStatement}.
	 * @param ctx the parse tree
	 */
	void exitDmlStatement(PgLogicalDecodingParser.DmlStatementContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#insertOp}.
	 * @param ctx the parse tree
	 */
	void enterInsertOp(PgLogicalDecodingParser.InsertOpContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#insertOp}.
	 * @param ctx the parse tree
	 */
	void exitInsertOp(PgLogicalDecodingParser.InsertOpContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#updateOp}.
	 * @param ctx the parse tree
	 */
	void enterUpdateOp(PgLogicalDecodingParser.UpdateOpContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#updateOp}.
	 * @param ctx the parse tree
	 */
	void exitUpdateOp(PgLogicalDecodingParser.UpdateOpContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#updateOpOriginal}.
	 * @param ctx the parse tree
	 */
	void enterUpdateOpOriginal(PgLogicalDecodingParser.UpdateOpOriginalContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#updateOpOriginal}.
	 * @param ctx the parse tree
	 */
	void exitUpdateOpOriginal(PgLogicalDecodingParser.UpdateOpOriginalContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#deleteOp}.
	 * @param ctx the parse tree
	 */
	void enterDeleteOp(PgLogicalDecodingParser.DeleteOpContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#deleteOp}.
	 * @param ctx the parse tree
	 */
	void exitDeleteOp(PgLogicalDecodingParser.DeleteOpContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#oldKeyValuePair}.
	 * @param ctx the parse tree
	 */
	void enterOldKeyValuePair(PgLogicalDecodingParser.OldKeyValuePairContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#oldKeyValuePair}.
	 * @param ctx the parse tree
	 */
	void exitOldKeyValuePair(PgLogicalDecodingParser.OldKeyValuePairContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#newKeyValuePair}.
	 * @param ctx the parse tree
	 */
	void enterNewKeyValuePair(PgLogicalDecodingParser.NewKeyValuePairContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#newKeyValuePair}.
	 * @param ctx the parse tree
	 */
	void exitNewKeyValuePair(PgLogicalDecodingParser.NewKeyValuePairContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(PgLogicalDecodingParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(PgLogicalDecodingParser.ValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#table}.
	 * @param ctx the parse tree
	 */
	void enterTable(PgLogicalDecodingParser.TableContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#table}.
	 * @param ctx the parse tree
	 */
	void exitTable(PgLogicalDecodingParser.TableContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#schemaname}.
	 * @param ctx the parse tree
	 */
	void enterSchemaname(PgLogicalDecodingParser.SchemanameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#schemaname}.
	 * @param ctx the parse tree
	 */
	void exitSchemaname(PgLogicalDecodingParser.SchemanameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#tablename}.
	 * @param ctx the parse tree
	 */
	void enterTablename(PgLogicalDecodingParser.TablenameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#tablename}.
	 * @param ctx the parse tree
	 */
	void exitTablename(PgLogicalDecodingParser.TablenameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#columnname}.
	 * @param ctx the parse tree
	 */
	void enterColumnname(PgLogicalDecodingParser.ColumnnameContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#columnname}.
	 * @param ctx the parse tree
	 */
	void exitColumnname(PgLogicalDecodingParser.ColumnnameContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#typedef}.
	 * @param ctx the parse tree
	 */
	void enterTypedef(PgLogicalDecodingParser.TypedefContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#typedef}.
	 * @param ctx the parse tree
	 */
	void exitTypedef(PgLogicalDecodingParser.TypedefContext ctx);
	/**
	 * Enter a parse tree produced by {@link PgLogicalDecodingParser#quotedValue}.
	 * @param ctx the parse tree
	 */
	void enterQuotedValue(PgLogicalDecodingParser.QuotedValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link PgLogicalDecodingParser#quotedValue}.
	 * @param ctx the parse tree
	 */
	void exitQuotedValue(PgLogicalDecodingParser.QuotedValueContext ctx);
}