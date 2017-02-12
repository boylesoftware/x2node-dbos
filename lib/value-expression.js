'use strict';

const common = require('x2node-common');


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
		this._usedPropertyPaths.add(
			ctx.basePath.length > 0 ? ctx.basePath + '.' + expr : expr);
		//...
	}

	get usedPropertyPaths() { return this._usedPropertyPaths; }

	translate(propsResolver, funcResolvers) {

		// TODO: implement
		return propsResolver(this._usedPropertyPaths.values().next().value);
		//...
	}
}

module.exports = ValueExpression;
