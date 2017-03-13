'use strict';

const Translatable = require('./translatable.js');


/**
 * Regular expression pattern used to extract tokens from the expression when
 * parsing it.
 *
 * @private
 * @type {string}
 */
const LEXER_PATTERN = (
	'^\\s*' +
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
		'|(.)'
);

class PropertyRefTerm {

	constructor(propRef) {

		this._propRef = propRef;
	}

	translate(ctx) {

		return ctx.translatePropPath(this._propRef);
	}
}


/**
 * Value expression.
 *
 * @memberof module:x2node-queries
 * @inner
 * @extends module:x2node-queries~Translatable
 */
class ValueExpression extends Translatable {

	/**
	 * <b>The constructor is not accessible from the client code. Value
	 * expression objects are created internally by the module and are made
	 * available through the public API where appropriate.</b>
	 *
	 * @param {module:x2node-queries~ValueExpressionContext} ctx Context for the
	 * property references in the expression.
	 * @param {string} expr The expression to compile.
	 * @throws {module:x2node-common.X2UsageError} If the expression is invalid.
	 */
	constructor(ctx, expr) {
		super();

		// TODO: implement
		const propRef = ctx.normalizePropertyRef(expr);
		this._usedPropertyPaths.add(propRef);
		this._topTerm = new PropertyRefTerm(propRef);
	}

	/**
	 * Tell if the whole expression is just a reference to a single property.
	 *
	 * @returns {boolean} <code>true</code> if singler property reference
	 * expression.
	 */
	isSinglePropRef() {

		// TODO: implement
		return true;
	}

	/**
	 * Translate expression to SQL.
	 *
	 * @private
	 * @param {module:x2node-queries~TranslationContext} ctx Translation context.
	 * @returns {string} Expression SQL.
	 */
	translate(ctx) {

		// TODO: implement
		return this._topTerm.translate(ctx);
	}
}

// export the value expression class
module.exports = ValueExpression;
