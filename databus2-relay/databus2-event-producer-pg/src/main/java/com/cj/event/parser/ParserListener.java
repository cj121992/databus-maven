/*
Copyright (c) 2016 Sebastian Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
package com.cj.event.parser;

import org.antlr.v4.runtime.misc.NotNull;

import com.cj.event.Cell;
import com.cj.event.DmlEvent;
import com.cj.event.Event;
import com.cj.event.TxEvent;
import com.cj.event.parser.PgLogicalDecodingParser.DeleteOpContext;
import com.cj.event.parser.PgLogicalDecodingParser.InsertOpContext;
import com.cj.event.parser.PgLogicalDecodingParser.TxStatementContext;
import com.cj.event.parser.PgLogicalDecodingParser.UpdateOpContext;
import com.cj.event.parser.PgLogicalDecodingParser.UpdateOpOriginalContext;


public class ParserListener extends PgLogicalDecodingBaseListener {


	private Event currentEvent;
	
	private String tableName;
	private String schemaName;
	
	private Cell currentKeyValuePair;

	@Override
	public void enterTxStatement(TxStatementContext ctx) {
		currentEvent = new TxEvent();
	}
	
	@Override
	public void exitCommitTimestamp(PgLogicalDecodingParser.CommitTimestampContext ctx) {
		((TxEvent)currentEvent).setCommitTime(ctx.getText());
	}

	@Override
	public void enterInsertOp(InsertOpContext ctx) {
		currentEvent = new DmlEvent(schemaName, tableName, DmlEvent.Type.insert);
	}

	@Override
	public void enterUpdateOp(UpdateOpContext ctx) {
		currentEvent = new DmlEvent(schemaName, tableName, DmlEvent.Type.update);
	}
	
	@Override
	public void enterUpdateOpOriginal(UpdateOpOriginalContext ctx) {
		currentEvent = new DmlEvent(schemaName, tableName, DmlEvent.Type.update);
	}
	
	@Override
	public void enterDeleteOp(DeleteOpContext ctx) {
		currentEvent = new DmlEvent(schemaName, tableName, DmlEvent.Type.delete);
	}

	public void enterSchemaname(@NotNull PgLogicalDecodingParser.SchemanameContext ctx) { 
		 schemaName = ctx.Identifier().getText();
	}
	
	public void enterTablename(@NotNull PgLogicalDecodingParser.TablenameContext ctx) {
		tableName = ctx.Identifier().getText();
	}
	
	@Override public void enterOldKeyValuePair(@NotNull PgLogicalDecodingParser.OldKeyValuePairContext ctx) {
		currentKeyValuePair = new Cell();
	}
	
	@Override public void enterNewKeyValuePair(@NotNull PgLogicalDecodingParser.NewKeyValuePairContext ctx) {
		currentKeyValuePair = new Cell();
	}
	
	@Override public void exitNewKeyValuePair(@NotNull PgLogicalDecodingParser.NewKeyValuePairContext ctx) {
		((DmlEvent)currentEvent).getNewValues().add(currentKeyValuePair);
	}
	
	@Override public void exitOldKeyValuePair(@NotNull PgLogicalDecodingParser.OldKeyValuePairContext ctx) {
		((DmlEvent)currentEvent).getOldValues().add(currentKeyValuePair);
	}
	
	@Override public void enterColumnname(@NotNull PgLogicalDecodingParser.ColumnnameContext ctx) {
		currentKeyValuePair.setName(ctx.Identifier().getText());
	}
	
	@Override public void enterTypedef(@NotNull PgLogicalDecodingParser.TypedefContext ctx) {
		currentKeyValuePair.setType(ctx.getText());
	}
	
	@Override public void enterValue(@NotNull PgLogicalDecodingParser.ValueContext ctx) { 
		currentKeyValuePair.setValue(ctx.getText());
	}
	
	@Override public void enterQuotedValue(@NotNull PgLogicalDecodingParser.QuotedValueContext ctx) { 
		String value = ctx.getText();
		if (value.startsWith("'") && value.endsWith("'")) {
			int length = value.length();
			value = value.substring(1, length - 1);
		}
		currentKeyValuePair.setValue(value);
	}
	
	public Event getEvent() {
		return currentEvent;
	}
	
}
