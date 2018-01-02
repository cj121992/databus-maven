package com.cj.event.parser;

// Generated from PgLogicalDecoding.g4 by ANTLR 4.5.2
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PgLogicalDecodingParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, Number=16, 
		Date=17, Time=18, Identifier=19, QuotedString=20;
	public static final int
		RULE_logline = 0, RULE_txStatement = 1, RULE_commitTimestamp = 2, RULE_dmlStatement = 3, 
		RULE_insertOp = 4, RULE_updateOp = 5, RULE_updateOpOriginal = 6, RULE_deleteOp = 7, 
		RULE_oldKeyValuePair = 8, RULE_newKeyValuePair = 9, RULE_value = 10, RULE_table = 11, 
		RULE_schemaname = 12, RULE_tablename = 13, RULE_columnname = 14, RULE_typedef = 15, 
		RULE_quotedValue = 16;
	public static final String[] ruleNames = {
		"logline", "txStatement", "commitTimestamp", "dmlStatement", "insertOp", 
		"updateOp", "updateOpOriginal", "deleteOp", "oldKeyValuePair", "newKeyValuePair", 
		"value", "table", "schemaname", "tablename", "columnname", "typedef", 
		"quotedValue"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'BEGIN '", "'COMMIT '", "' (at '", "')'", "' '", "'table '", "': '", 
		"'INSERT: '", "'UPDATE: old-key: '", "'new-tuple: '", "'UPDATE: '", "'DELETE: '", 
		"'['", "']:'", "'.'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, "Number", "Date", "Time", "Identifier", "QuotedString"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "PgLogicalDecoding.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public PgLogicalDecodingParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class LoglineContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(PgLogicalDecodingParser.EOF, 0); }
		public TxStatementContext txStatement() {
			return getRuleContext(TxStatementContext.class,0);
		}
		public DmlStatementContext dmlStatement() {
			return getRuleContext(DmlStatementContext.class,0);
		}
		public LoglineContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logline; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterLogline(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitLogline(this);
		}
	}

	public final LoglineContext logline() throws RecognitionException {
		LoglineContext _localctx = new LoglineContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_logline);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(36);
			switch (_input.LA(1)) {
			case T__0:
			case T__1:
				{
				setState(34);
				txStatement();
				}
				break;
			case T__5:
				{
				setState(35);
				dmlStatement();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(38);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TxStatementContext extends ParserRuleContext {
		public TerminalNode Number() { return getToken(PgLogicalDecodingParser.Number, 0); }
		public CommitTimestampContext commitTimestamp() {
			return getRuleContext(CommitTimestampContext.class,0);
		}
		public TxStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_txStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterTxStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitTxStatement(this);
		}
	}

	public final TxStatementContext txStatement() throws RecognitionException {
		TxStatementContext _localctx = new TxStatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_txStatement);
		int _la;
		try {
			setState(50);
			switch (_input.LA(1)) {
			case T__0:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(40);
				match(T__0);
				setState(41);
				match(Number);
				}
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(42);
				match(T__1);
				setState(43);
				match(Number);
				setState(48);
				_la = _input.LA(1);
				if (_la==T__2) {
					{
					setState(44);
					match(T__2);
					setState(45);
					commitTimestamp();
					setState(46);
					match(T__3);
					}
				}

				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CommitTimestampContext extends ParserRuleContext {
		public TerminalNode Date() { return getToken(PgLogicalDecodingParser.Date, 0); }
		public TerminalNode Time() { return getToken(PgLogicalDecodingParser.Time, 0); }
		public CommitTimestampContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commitTimestamp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterCommitTimestamp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitCommitTimestamp(this);
		}
	}

	public final CommitTimestampContext commitTimestamp() throws RecognitionException {
		CommitTimestampContext _localctx = new CommitTimestampContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_commitTimestamp);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(52);
			match(Date);
			setState(53);
			match(T__4);
			setState(54);
			match(Time);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DmlStatementContext extends ParserRuleContext {
		public TableContext table() {
			return getRuleContext(TableContext.class,0);
		}
		public InsertOpContext insertOp() {
			return getRuleContext(InsertOpContext.class,0);
		}
		public UpdateOpContext updateOp() {
			return getRuleContext(UpdateOpContext.class,0);
		}
		public DeleteOpContext deleteOp() {
			return getRuleContext(DeleteOpContext.class,0);
		}
		public UpdateOpOriginalContext updateOpOriginal() {
			return getRuleContext(UpdateOpOriginalContext.class,0);
		}
		public DmlStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dmlStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterDmlStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitDmlStatement(this);
		}
	}

	public final DmlStatementContext dmlStatement() throws RecognitionException {
		DmlStatementContext _localctx = new DmlStatementContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_dmlStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(56);
			match(T__5);
			setState(57);
			table();
			setState(58);
			match(T__6);
			setState(63);
			switch (_input.LA(1)) {
			case T__7:
				{
				setState(59);
				insertOp();
				}
				break;
			case T__8:
				{
				setState(60);
				updateOp();
				}
				break;
			case T__11:
				{
				setState(61);
				deleteOp();
				}
				break;
			case T__10:
				{
				setState(62);
				updateOpOriginal();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertOpContext extends ParserRuleContext {
		public List<NewKeyValuePairContext> newKeyValuePair() {
			return getRuleContexts(NewKeyValuePairContext.class);
		}
		public NewKeyValuePairContext newKeyValuePair(int i) {
			return getRuleContext(NewKeyValuePairContext.class,i);
		}
		public InsertOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertOp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterInsertOp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitInsertOp(this);
		}
	}

	public final InsertOpContext insertOp() throws RecognitionException {
		InsertOpContext _localctx = new InsertOpContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_insertOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(65);
			match(T__7);
			setState(69);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==Identifier) {
				{
				{
				setState(66);
				newKeyValuePair();
				}
				}
				setState(71);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UpdateOpContext extends ParserRuleContext {
		public List<OldKeyValuePairContext> oldKeyValuePair() {
			return getRuleContexts(OldKeyValuePairContext.class);
		}
		public OldKeyValuePairContext oldKeyValuePair(int i) {
			return getRuleContext(OldKeyValuePairContext.class,i);
		}
		public List<NewKeyValuePairContext> newKeyValuePair() {
			return getRuleContexts(NewKeyValuePairContext.class);
		}
		public NewKeyValuePairContext newKeyValuePair(int i) {
			return getRuleContext(NewKeyValuePairContext.class,i);
		}
		public UpdateOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateOp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterUpdateOp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitUpdateOp(this);
		}
	}

	public final UpdateOpContext updateOp() throws RecognitionException {
		UpdateOpContext _localctx = new UpdateOpContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_updateOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(72);
			match(T__8);
			setState(76);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==Identifier) {
				{
				{
				setState(73);
				oldKeyValuePair();
				}
				}
				setState(78);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(79);
			match(T__9);
			setState(83);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==Identifier) {
				{
				{
				setState(80);
				newKeyValuePair();
				}
				}
				setState(85);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UpdateOpOriginalContext extends ParserRuleContext {
		public List<NewKeyValuePairContext> newKeyValuePair() {
			return getRuleContexts(NewKeyValuePairContext.class);
		}
		public NewKeyValuePairContext newKeyValuePair(int i) {
			return getRuleContext(NewKeyValuePairContext.class,i);
		}
		public UpdateOpOriginalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_updateOpOriginal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterUpdateOpOriginal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitUpdateOpOriginal(this);
		}
	}

	public final UpdateOpOriginalContext updateOpOriginal() throws RecognitionException {
		UpdateOpOriginalContext _localctx = new UpdateOpOriginalContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_updateOpOriginal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(86);
			match(T__10);
			setState(90);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==Identifier) {
				{
				{
				setState(87);
				newKeyValuePair();
				}
				}
				setState(92);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DeleteOpContext extends ParserRuleContext {
		public List<OldKeyValuePairContext> oldKeyValuePair() {
			return getRuleContexts(OldKeyValuePairContext.class);
		}
		public OldKeyValuePairContext oldKeyValuePair(int i) {
			return getRuleContext(OldKeyValuePairContext.class,i);
		}
		public DeleteOpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deleteOp; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterDeleteOp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitDeleteOp(this);
		}
	}

	public final DeleteOpContext deleteOp() throws RecognitionException {
		DeleteOpContext _localctx = new DeleteOpContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_deleteOp);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(93);
			match(T__11);
			setState(97);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==Identifier) {
				{
				{
				setState(94);
				oldKeyValuePair();
				}
				}
				setState(99);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OldKeyValuePairContext extends ParserRuleContext {
		public ColumnnameContext columnname() {
			return getRuleContext(ColumnnameContext.class,0);
		}
		public TypedefContext typedef() {
			return getRuleContext(TypedefContext.class,0);
		}
		public TerminalNode EOF() { return getToken(PgLogicalDecodingParser.EOF, 0); }
		public QuotedValueContext quotedValue() {
			return getRuleContext(QuotedValueContext.class,0);
		}
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public OldKeyValuePairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oldKeyValuePair; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterOldKeyValuePair(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitOldKeyValuePair(this);
		}
	}

	public final OldKeyValuePairContext oldKeyValuePair() throws RecognitionException {
		OldKeyValuePairContext _localctx = new OldKeyValuePairContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_oldKeyValuePair);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(100);
			columnname();
			setState(101);
			match(T__12);
			setState(102);
			typedef();
			setState(103);
			match(T__13);
			setState(106);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				{
				setState(104);
				quotedValue();
				}
				break;
			case 2:
				{
				setState(105);
				value();
				}
				break;
			}
			setState(108);
			_la = _input.LA(1);
			if ( !(_la==EOF || _la==T__4) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NewKeyValuePairContext extends ParserRuleContext {
		public ColumnnameContext columnname() {
			return getRuleContext(ColumnnameContext.class,0);
		}
		public TypedefContext typedef() {
			return getRuleContext(TypedefContext.class,0);
		}
		public TerminalNode EOF() { return getToken(PgLogicalDecodingParser.EOF, 0); }
		public QuotedValueContext quotedValue() {
			return getRuleContext(QuotedValueContext.class,0);
		}
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public NewKeyValuePairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_newKeyValuePair; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterNewKeyValuePair(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitNewKeyValuePair(this);
		}
	}

	public final NewKeyValuePairContext newKeyValuePair() throws RecognitionException {
		NewKeyValuePairContext _localctx = new NewKeyValuePairContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_newKeyValuePair);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(110);
			columnname();
			setState(111);
			match(T__12);
			setState(112);
			typedef();
			setState(113);
			match(T__13);
			setState(116);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				{
				setState(114);
				quotedValue();
				}
				break;
			case 2:
				{
				setState(115);
				value();
				}
				break;
			}
			setState(118);
			_la = _input.LA(1);
			if ( !(_la==EOF || _la==T__4) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ValueContext extends ParserRuleContext {
		public ValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitValue(this);
		}
	}

	public final ValueContext value() throws RecognitionException {
		ValueContext _localctx = new ValueContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(123);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << Number) | (1L << Date) | (1L << Time) | (1L << Identifier) | (1L << QuotedString))) != 0)) {
				{
				{
				setState(120);
				_la = _input.LA(1);
				if ( _la <= 0 || (_la==T__4) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				}
				setState(125);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TableContext extends ParserRuleContext {
		public SchemanameContext schemaname() {
			return getRuleContext(SchemanameContext.class,0);
		}
		public TablenameContext tablename() {
			return getRuleContext(TablenameContext.class,0);
		}
		public TableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterTable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitTable(this);
		}
	}

	public final TableContext table() throws RecognitionException {
		TableContext _localctx = new TableContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_table);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(126);
			schemaname();
			setState(127);
			match(T__14);
			setState(128);
			tablename();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SchemanameContext extends ParserRuleContext {
		public TerminalNode Identifier() { return getToken(PgLogicalDecodingParser.Identifier, 0); }
		public SchemanameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schemaname; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterSchemaname(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitSchemaname(this);
		}
	}

	public final SchemanameContext schemaname() throws RecognitionException {
		SchemanameContext _localctx = new SchemanameContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_schemaname);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(130);
			match(Identifier);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TablenameContext extends ParserRuleContext {
		public TerminalNode Identifier() { return getToken(PgLogicalDecodingParser.Identifier, 0); }
		public TablenameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tablename; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterTablename(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitTablename(this);
		}
	}

	public final TablenameContext tablename() throws RecognitionException {
		TablenameContext _localctx = new TablenameContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_tablename);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(132);
			match(Identifier);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnnameContext extends ParserRuleContext {
		public TerminalNode Identifier() { return getToken(PgLogicalDecodingParser.Identifier, 0); }
		public ColumnnameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnname; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterColumnname(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitColumnname(this);
		}
	}

	public final ColumnnameContext columnname() throws RecognitionException {
		ColumnnameContext _localctx = new ColumnnameContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_columnname);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(134);
			match(Identifier);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TypedefContext extends ParserRuleContext {
		public TypedefContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_typedef; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterTypedef(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitTypedef(this);
		}
	}

	public final TypedefContext typedef() throws RecognitionException {
		TypedefContext _localctx = new TypedefContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_typedef);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(139);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__1) | (1L << T__2) | (1L << T__3) | (1L << T__4) | (1L << T__5) | (1L << T__6) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << T__11) | (1L << T__12) | (1L << T__14) | (1L << Number) | (1L << Date) | (1L << Time) | (1L << Identifier) | (1L << QuotedString))) != 0)) {
				{
				{
				setState(136);
				_la = _input.LA(1);
				if ( _la <= 0 || (_la==T__13) ) {
				_errHandler.recoverInline(this);
				} else {
					consume();
				}
				}
				}
				setState(141);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QuotedValueContext extends ParserRuleContext {
		public TerminalNode QuotedString() { return getToken(PgLogicalDecodingParser.QuotedString, 0); }
		public QuotedValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quotedValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).enterQuotedValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PgLogicalDecodingListener ) ((PgLogicalDecodingListener)listener).exitQuotedValue(this);
		}
	}

	public final QuotedValueContext quotedValue() throws RecognitionException {
		QuotedValueContext _localctx = new QuotedValueContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_quotedValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(142);
			match(QuotedString);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\26\u0093\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\3\2\3\2\5\2\'\n\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3\63\n\3"+
		"\5\3\65\n\3\3\4\3\4\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5B\n\5\3\6\3"+
		"\6\7\6F\n\6\f\6\16\6I\13\6\3\7\3\7\7\7M\n\7\f\7\16\7P\13\7\3\7\3\7\7\7"+
		"T\n\7\f\7\16\7W\13\7\3\b\3\b\7\b[\n\b\f\b\16\b^\13\b\3\t\3\t\7\tb\n\t"+
		"\f\t\16\te\13\t\3\n\3\n\3\n\3\n\3\n\3\n\5\nm\n\n\3\n\3\n\3\13\3\13\3\13"+
		"\3\13\3\13\3\13\5\13w\n\13\3\13\3\13\3\f\7\f|\n\f\f\f\16\f\177\13\f\3"+
		"\r\3\r\3\r\3\r\3\16\3\16\3\17\3\17\3\20\3\20\3\21\7\21\u008c\n\21\f\21"+
		"\16\21\u008f\13\21\3\22\3\22\3\22\2\2\23\2\4\6\b\n\f\16\20\22\24\26\30"+
		"\32\34\36 \"\2\5\3\3\7\7\3\2\7\7\3\2\20\20\u0090\2&\3\2\2\2\4\64\3\2\2"+
		"\2\6\66\3\2\2\2\b:\3\2\2\2\nC\3\2\2\2\fJ\3\2\2\2\16X\3\2\2\2\20_\3\2\2"+
		"\2\22f\3\2\2\2\24p\3\2\2\2\26}\3\2\2\2\30\u0080\3\2\2\2\32\u0084\3\2\2"+
		"\2\34\u0086\3\2\2\2\36\u0088\3\2\2\2 \u008d\3\2\2\2\"\u0090\3\2\2\2$\'"+
		"\5\4\3\2%\'\5\b\5\2&$\3\2\2\2&%\3\2\2\2\'(\3\2\2\2()\7\2\2\3)\3\3\2\2"+
		"\2*+\7\3\2\2+\65\7\22\2\2,-\7\4\2\2-\62\7\22\2\2./\7\5\2\2/\60\5\6\4\2"+
		"\60\61\7\6\2\2\61\63\3\2\2\2\62.\3\2\2\2\62\63\3\2\2\2\63\65\3\2\2\2\64"+
		"*\3\2\2\2\64,\3\2\2\2\65\5\3\2\2\2\66\67\7\23\2\2\678\7\7\2\289\7\24\2"+
		"\29\7\3\2\2\2:;\7\b\2\2;<\5\30\r\2<A\7\t\2\2=B\5\n\6\2>B\5\f\7\2?B\5\20"+
		"\t\2@B\5\16\b\2A=\3\2\2\2A>\3\2\2\2A?\3\2\2\2A@\3\2\2\2B\t\3\2\2\2CG\7"+
		"\n\2\2DF\5\24\13\2ED\3\2\2\2FI\3\2\2\2GE\3\2\2\2GH\3\2\2\2H\13\3\2\2\2"+
		"IG\3\2\2\2JN\7\13\2\2KM\5\22\n\2LK\3\2\2\2MP\3\2\2\2NL\3\2\2\2NO\3\2\2"+
		"\2OQ\3\2\2\2PN\3\2\2\2QU\7\f\2\2RT\5\24\13\2SR\3\2\2\2TW\3\2\2\2US\3\2"+
		"\2\2UV\3\2\2\2V\r\3\2\2\2WU\3\2\2\2X\\\7\r\2\2Y[\5\24\13\2ZY\3\2\2\2["+
		"^\3\2\2\2\\Z\3\2\2\2\\]\3\2\2\2]\17\3\2\2\2^\\\3\2\2\2_c\7\16\2\2`b\5"+
		"\22\n\2a`\3\2\2\2be\3\2\2\2ca\3\2\2\2cd\3\2\2\2d\21\3\2\2\2ec\3\2\2\2"+
		"fg\5\36\20\2gh\7\17\2\2hi\5 \21\2il\7\20\2\2jm\5\"\22\2km\5\26\f\2lj\3"+
		"\2\2\2lk\3\2\2\2mn\3\2\2\2no\t\2\2\2o\23\3\2\2\2pq\5\36\20\2qr\7\17\2"+
		"\2rs\5 \21\2sv\7\20\2\2tw\5\"\22\2uw\5\26\f\2vt\3\2\2\2vu\3\2\2\2wx\3"+
		"\2\2\2xy\t\2\2\2y\25\3\2\2\2z|\n\3\2\2{z\3\2\2\2|\177\3\2\2\2}{\3\2\2"+
		"\2}~\3\2\2\2~\27\3\2\2\2\177}\3\2\2\2\u0080\u0081\5\32\16\2\u0081\u0082"+
		"\7\21\2\2\u0082\u0083\5\34\17\2\u0083\31\3\2\2\2\u0084\u0085\7\25\2\2"+
		"\u0085\33\3\2\2\2\u0086\u0087\7\25\2\2\u0087\35\3\2\2\2\u0088\u0089\7"+
		"\25\2\2\u0089\37\3\2\2\2\u008a\u008c\n\4\2\2\u008b\u008a\3\2\2\2\u008c"+
		"\u008f\3\2\2\2\u008d\u008b\3\2\2\2\u008d\u008e\3\2\2\2\u008e!\3\2\2\2"+
		"\u008f\u008d\3\2\2\2\u0090\u0091\7\26\2\2\u0091#\3\2\2\2\17&\62\64AGN"+
		"U\\clv}\u008d";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}