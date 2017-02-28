'use strict';


/**
 * Value expression.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class ValueExpression {

	/**
	 * Create value expression.
	 *
	 * @param {module:x2node-queries~ValueExpressionContext} ctx Context for the
	 * property references in the expression.
	 * @param {string} expr The expression to compile.
	 * @throws {module:x2node-common.X2UsageError} If the expression is invalid.
	 */
	constructor(ctx, expr) {

		this._usedPropertyPaths = new Set();

		// TODO: implement
		this._usedPropertyPaths.add(ctx.normalizePropertyRef(expr));
		//...
	}

	/**
	 * Paths of all properties referred in the expression. The paths include the
	 * base path from the value expression context.
	 *
	 * @type {external:Set.<string>}
	 * @readonly
	 */
	get usedPropertyPaths() { return this._usedPropertyPaths; }

	/**
	 * Translate expression to SQL.
	 *
	 * @param {module:x2node-queries~TranslationContext} ctx Translation context.
	 * @returns {string} Expression SQL.
	 */
	translate(ctx) {

		// TODO: implement
		return ctx.translatePropPath(this._usedPropertyPaths.values().next().value);
		//...
	}
}

// export the value expression class
module.exports = ValueExpression;
