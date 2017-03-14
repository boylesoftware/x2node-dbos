'use strict';

const common = require('x2node-common');

const Translatable = require('./translatable.js');


/**
 * Parsed, translatable value expression element.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~Translatable
 * @abstract
 */
class ValueExpressionElement extends Translatable {

	constructor() {
		super();
	}

	isSinglePropRef() { return false; }
}

/**
 * String literal expression.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~ValueExpressionElement
 */
class StringLiteral extends ValueExpressionElement {

	constructor(val) {
		super();

		this._val = val;
	}

	translate(ctx) {

		return ctx.dbDriver.stringLiteral(this._val);
	}
}

/**
 * Boolean literal expression.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~ValueExpressionElement
 */
class BooleanLiteral extends ValueExpressionElement {

	constructor(val) {
		super();

		this._val = val;
	}

	translate(ctx) {

		return ctx.dbDriver.booleanLiteral(this._val);
	}
}

/**
 * Numeric literal expression.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~ValueExpressionElement
 */
class NumberLiteral extends ValueExpressionElement {

	constructor(val) {
		super();

		this._val = val;
	}

	translate() {

		return String(this._val);
	}
}

/**
 * Sum of terms expression.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~ValueExpressionElement
 */
class SumExpression extends ValueExpressionElement {

	constructor() {
		super();

		this._negated = false;
		this._firstTerm = null;
		this._additionalTerms = new Array();
	}

	makeNegated() {

		this._negated = true;
	}

	setFirstTerm(term) {

		this._firstTerm = term;

		this.addUsedPropertyPaths(term.usedPropertyPaths);
	}

	addTerm(op, term) {

		this._additionalTerms.push({
			op: op,
			term: term
		});

		this.addUsedPropertyPaths(term.usedPropertyPaths);
	}

	get multiElementSum() { return (this._additionalTerms.length > 0); }

	isSinglePropRef() {

		return (
			!this._negated &&
				(this._additionalTerms.length === 0) &&
				this._firstTerm.isSinglePropRef()
		);
	}

	translate(ctx) {

		let res = '';

		if (this._negated)
			res += '-';

		if (this._firstTerm.multiElementSum)
			res += '(' + this._firstTerm.translate(ctx) + ')';
		else
			res += this._firstTerm.translate(ctx);

		this._additionalTerms.forEach(term => {
			res += ' ' + term.op + ' ';
			if (term.term.multiElementSum)
				res += '(' + term.term.translate(ctx) + ')';
			else
				res += term.term.translate(ctx);
		});

		return res;
	}
}

/**
 * Product of factors expression.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~ValueExpressionElement
 */
class ProductExpression extends ValueExpressionElement {

	constructor() {
		super();

		this._firstFactor = null;
		this._additionalFactors = new Array();
	}

	setFirstFactor(factor) {

		this._firstFactor = factor;

		this.addUsedPropertyPaths(factor.usedPropertyPaths);
	}

	addFactor(op, factor) {

		this._additionalFactors.push({
			op: op,
			factor: factor
		});

		this.addUsedPropertyPaths(factor.usedPropertyPaths);
	}

	isSinglePropRef() {

		return (
			(this._additionalFactors.length === 0) &&
				this._firstFactor.isSinglePropRef()
		);
	}

	translate(ctx) {

		let res = '';

		if (this._firstFactor.multiElementSum)
			res += '(' + this._firstFactor.translate(ctx) + ')';
		else
			res += this._firstFactor.translate(ctx);

		this._additionalFactors.forEach(factor => {
			res += ' ' + factor.op + ' ';
			if (factor.factor.multiElementSum)
				res += '(' + factor.factor.translate(ctx) + ')';
			else
				res += factor.factor.translate(ctx);
		});

		return res;
	}
}

/**
 * Property reference expression.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~ValueExpressionElement
 */
class PropertyRef extends ValueExpressionElement {

	constructor(normalizedPropRef) {
		super();

		this._propRef = normalizedPropRef;

		this._usedPropertyPaths.add(normalizedPropRef);
	}

	isSinglePropRef() { return true; }

	translate(ctx) {

		return ctx.translatePropPath(this._propRef);
	}
}

/**
 * Get error for invalid number of value function arguments.
 *
 * @private
 * @returns {module:x2node-common.X2UsageError} Error to throw.
 */
function invalidValueFuncArgs() {
	return new common.X2UsageError(
		'Invalid value expression: wrong number of value function arguments.');
}

/**
 * Value function translators.
 *
 * @private
 * @enum {function}
 */
const VALUE_FUNCTIONS = {
	length: function(ctx, args) {
		if (args.length !== 1)
			throw invalidValueFuncArgs();
		return ctx.dbDriver.stringLength(args[0].translate(ctx));
	},
	lower: function(ctx, args) {
		if (args.length !== 1)
			throw invalidValueFuncArgs();
		return ctx.dbDriver.stringLowercase(args[0].translate(ctx));
	},
	upper: function(ctx, args) {
		if (args.length !== 1)
			throw invalidValueFuncArgs();
		return ctx.dbDriver.stringUppercase(args[0].translate(ctx));
	},
	substring: function(ctx, args) {
		if ((args.length < 2) || (args.length > 3))
			throw invalidValueFuncArgs();
		return ctx.dbDriver.stringSubstring(
			args[0].translate(ctx), args[1].translate(ctx),
			(args[2] && args[2].translate(ctx)));
	},
	lpad: function(ctx, args) {
		if (args.length !== 3)
			throw invalidValueFuncArgs();
		return ctx.dbDriver.stringLeftPad(
			args[0].translate(ctx), args[1].translate(ctx),
			args[2].translate(ctx));
	},
	concat: function(ctx, args) {
		if (args.length === 0)
			throw invalidValueFuncArgs();
		return ctx.dbDriver.nullableConcat.apply(
			ctx.dbDriver, args.map(arg => arg.translate(ctx)));
	},
	coalesce: function(ctx, args) {
		if (args.length === 0)
			throw invalidValueFuncArgs();
		return ctx.dbDriver.coalesce.apply(
			ctx.dbDriver, args.map(arg => arg.translate(ctx)));
	}
};

/**
 * Value function call expression.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~ValueExpressionElement
 */
class FunctionCall extends ValueExpressionElement {

	constructor(funcName) {
		super();

		switch (funcName.toLowerCase()) {
		case 'len':
		case 'length':
			this._func = VALUE_FUNCTIONS['length'];
			break;
		case 'lc':
		case 'lower':
		case 'lcase':
		case 'lowercase':
			this._func = VALUE_FUNCTIONS['lower'];
			break;
		case 'uc':
		case 'upper':
		case 'ucase':
		case 'uppercase':
			this._func = VALUE_FUNCTIONS['upper'];
			break;
		case 'sub':
		case 'mid':
		case 'substr':
		case 'substring':
			this._func = VALUE_FUNCTIONS['substring'];
			break;
		case 'lpad':
			this._func = VALUE_FUNCTIONS['lpad'];
			break;
		case 'cat':
		case 'concat':
			this._func = VALUE_FUNCTIONS['concat'];
			break;
		case 'coalesce':
			this._func = VALUE_FUNCTIONS['coalesce'];
			break;
		default:
			throw new common.X2UsageError(
				'Invalid value expression: unknown value function "' +
					funcName + '".');
		}

		this._args = new Array();
	}

	addArgument(arg) {

		this._args.push(arg);

		this.addUsedPropertyPaths(arg.usedPropertyPaths);
	}

	translate(ctx) {

		return this._func(ctx, this._args);
	}
}


/**
 * Regular expression pattern used to extract tokens from the expression when
 * parsing it.
 *
 * @private
 * @constant {string}
 */
const LEXER_PATTERN = (
	'(?:^\\s*)?(?:' +
		'([+-])\\s*' +
		'|([*/])\\s*' +
		'|(\\()\\s*' +
		'|(\\))\\s*' +
		'|(,)\\s*' +
		'|(true|false)\\s*' +
		'|((?:"[^"]*")|(?:\'[^\']*\'))\\s*' +
		'|([0-9]+(?:\\.[0-9]+)?)\\s*' +
		'|((?:\\^(?:\\.\\^)*\\.)?' +
			'[a-z_$][a-z_$0-9]*(?:\\.[a-z_$][a-z_$0-9]*)*)\\s*' +
		'|(.)' +
	')'
);

/**
 * Lexer token types.
 *
 * @private
 * @enum {Symbol}
 */
const TOKENS = {
	EOD: Symbol('EOD'),
	PM: Symbol('PM'),
	MD: Symbol('MD'),
	LP: Symbol('LP'),
	RP: Symbol('RP'),
	CM: Symbol('CM'),
	BOL: Symbol('BOL'),
	STR: Symbol('STR'),
	NUM: Symbol('NUM'),
	REF: Symbol('REF'),
	IDN: Symbol('IDN')
};

/**
 * Functions that make tokens from lexemes.
 *
 * @private
 * @type {Array.<function>}
 */
const TOKEN_MAKERS = [
	function() {
		return {
			type: TOKENS.EOD
		};
	},
	function(input) {
		return {
			type: TOKENS.PM,
			op: input
		};
	},
	function (input) {
		return {
			type: TOKENS.MD,
			op: input
		};
	},
	function () {
		return {
			type: TOKENS.LP
		};
	},
	function () {
		return {
			type: TOKENS.RP
		};
	},
	function () {
		return {
			type: TOKENS.CM
		};
	},
	function (input) {
		return {
			type: TOKENS.BOL,
			val: (input.toLowerCase() === 'true')
		};
	},
	function (input) {
		return {
			type: TOKENS.STR,
			val: input.substring(1, input.length - 1)
		};
	},
	function (input) {
		const val = Number(input);
		if (!Number.isFinite(val))
			throw new common.X2UsageError(
				'Invalid number ' + input + ' in value expression.');
		return {
			type: TOKENS.NUM,
			val: val
		};
	},
	function (input) {
		return {
			type: (/[.^]/.test(input) ? TOKENS.REF : TOKENS.IDN),
			ref: input
		};
	},
	function(input) {
		throw new common.X2UsageError(
			'Invalid value expression syntax: unexpected character "' +
				input + '".');
	}
];

/**
 * Expression parser.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 */
class ValueExpressionParser {

	/**
	 * Create new parser for the specified expression.
	 *
	 * @param {string} expr The expression text.
	 */
	constructor(ctx, expr) {

		this._ctx = ctx;
		this._input = expr;
		this._lexer = new RegExp(LEXER_PATTERN, 'gi');

		this._curToken = null;
		this._nextToken = null;

		this._advance();
	}

	/**
	 * Advance to the next token.
	 *
	 * @private
	 */
	_advance() {

		let token;
		if (this._nextToken && (this._nextToken.type === TOKENS.EOD)) {
			token = null;
		} else {
			const match = this._lexer.exec(this._input);
			if (match === null) {
				token = TOKEN_MAKERS[0]();
			} else for (let i = 1; i < 11; i++) {
				const g = match[i];
				if (g !== undefined) {
					token = TOKEN_MAKERS[i](g);
					break;
				}
			}
		}

		this._curToken = this._nextToken;
		this._nextToken = token;
	}

	/**
	 * Try to accept the current token if it is of the specified type and advance
	 * to the next token if accepted. Do nothing if the current token does not
	 * match.
	 *
	 * @private
	 * @param {Symbol} tokenType Type of the token to accept.
	 * @param {Symbol} [nextTokenType] If specified, accept the token only if it
	 * is followed by a token of the specified type.
	 * @param {function} [action] Action function to call if accepted. The
	 * function receives the accepted token as its only argument.
	 * @returns {boolean} <code>true</code> if token accepted.
	 */
	_accept(tokenType, nextTokenType, action) {

		if ((this._curToken.type === tokenType) && (
			(nextTokenType === undefined) ||
				(this._nextToken.type === nextTokenType))) {
			if (action)
				action(this._curToken);
			this._advance();
			return true;
		}

		return false;
	}

	/**
	 * Accept the current token and fail if could not be accepted.
	 *
	 * @private
	 * @param {Symbol} tokenType Type of the token to accept.
	 */
	_expect(tokenType) {

		if (!this._accept(tokenType))
			throw new common.X2UsageError(
				'Invalid value expression syntax: expected ' +
					String(tokenType) + ' but received ' +
					String(this._curToken.type) + '.');
	}

	/**
	 * Parse "expression" non-terminal.
	 *
	 * @private
	 * @returns {module:x2node-dbos~ValueExpressionElement} Parsed expression
	 * element.
	 */
	_expression() {

		let res;

		if (this._accept(TOKENS.STR, undefined, token => {
			res = new StringLiteral(token.val);
		}))
			return res;

		if (this._accept(TOKENS.BOL, undefined, token => {
			res = new BooleanLiteral(token.val);
		}))
			return res;

		res = new SumExpression();
		this._accept(TOKENS.PM, undefined, token => {
			if (token.op === '-')
				res.makeNegated();
		});
		res.setFirstTerm(this._term());
		let op;
		while (this._accept(TOKENS.PM, undefined, token => {
			op = token.op;
		})) {
			res.addTerm(op, this._term());
		}

		return res;
	}

	/**
	 * Parse "term" non-terminal.
	 *
	 * @private
	 * @returns {module:x2node-dbos~ValueExpressionElement} Parsed expression
	 * element.
	 */
	_term() {

		const res = new ProductExpression();
		res.setFirstFactor(this._factor());
		let op;
		while (this._accept(TOKENS.MD, undefined, token => {
			op = token.op;
		})) {
			res.addFactor(op, this._factor());
		}

		return res;
	}

	/**
	 * Parse "factor" non-terminal.
	 *
	 * @private
	 * @returns {module:x2node-dbos~ValueExpressionElement} Parsed expression
	 * element.
	 */
	_factor() {

		let res;

		if (this._accept(TOKENS.LP)) {
			res = this._expression();
			this._expect(TOKENS.RP);
			return res;
		}

		if (this._accept(TOKENS.NUM, undefined, token => {
			res = new NumberLiteral(token.val);
		}))
			return res;

		if (this._accept(TOKENS.REF, undefined, token => {
			res = new PropertyRef(this._ctx.normalizePropertyRef(token.ref));
		}))
			return res;

		if (this._accept(TOKENS.IDN, TOKENS.LP, token => {
			res = new FunctionCall(token.ref);
		})) {
			this._advance();
			if (!this._accept(TOKENS.RP)) {
				res.addArgument(this._expression());
				while (this._accept(TOKENS.CM))
					res.addArgument(this._expression());
				this._expect(TOKENS.RP);
			}
			return res;
		}

		if (this._accept(TOKENS.IDN, undefined, token => {
			res = new PropertyRef(this._ctx.normalizePropertyRef(token.ref));
		}))
			return res;

		throw new common.X2UsageError('Invalid value expression.');
	}

	/**
	 * Parse the expression.
	 *
	 * @returns {module:x2node-dbos~ValueExpressionElement} Parsed expression.
	 */
	parse() {

		this._advance();
		const res = this._expression();
		this._expect(TOKENS.EOD);

		return res;
	}
}


/**
 * Value expression.
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~Translatable
 */
class ValueExpression extends Translatable {

	/**
	 * <strong>The constructor is not accessible from the client code. Value
	 * expression objects are created internally by the module and are made
	 * available through the public API where appropriate.</strong>
	 *
	 * @param {module:x2node-dbos~ValueExpressionContext} ctx Context for the
	 * property references in the expression.
	 * @param {string} expr The expression to compile.
	 * @throws {module:x2node-common.X2UsageError} If the expression is invalid.
	 */
	constructor(ctx, expr) {
		super();

		this._topExpr = (new ValueExpressionParser(ctx, expr)).parse();
		this._singlePropRef = this._topExpr.isSinglePropRef();

		this.addUsedPropertyPaths(this._topExpr.usedPropertyPaths);
	}

	/**
	 * Tell if the whole expression is just a reference to a single property.
	 *
	 * @returns {boolean} <code>true</code> if singler property reference
	 * expression.
	 */
	isSinglePropRef() {

		return this._singlePropRef;
	}

	/**
	 * Translate expression to SQL.
	 *
	 * @private
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @returns {string} Expression SQL.
	 */
	translate(ctx) {

		return this._topExpr.translate(ctx);
	}
}

// export the value expression class
module.exports = ValueExpression;
