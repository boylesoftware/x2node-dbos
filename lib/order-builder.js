'use strict';

const common = require('x2node-common');

const ValueExpression = require('./value-expression.js');


/**
 * Single query order specification element. The full specification is an array
 * of these.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QueryOrderSpec {

	constructor(valueExpr, reverse) {

		this._valueExpr = valueExpr;
		this._reverse = reverse;
	}

	get valueExpr() { return this._valueExpr; }

	isReverse() { return this._reverse; }
}

/**
 * Parse raw query order specification.
 *
 * @private
 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Value
 * expression context to use.
 * @param {string[]} orderSpecs Raw order specification.
 * @param {external:Set.<string>} usedPropPaths Set, to which to add paths of
 * all properties referred in all value expressions.
 * @returns {module:x2node-queries~QueryOrderSpec[]} Parsed order specification
 * elements. Returns <code>undefined</code> if the raw specifications array is
 * empty.
 * @throws {module:x2node-common.X2UsageError} If the specification is invalid.
 */
exports.parseOrderSpec = function(valueExprCtx, orderSpecs, usedPropPaths) {

	// error function
	function error(msg) {
		return new common.X2UsageError(
			'Invalid order specification' +
				(
					valueExprCtx.basePath.length > 0 ?
						' on ' + valueExprCtx.basePath : ''
				) + ': ' + msg);
	}

	// validate order specification array
	if (!Array.isArray(orderSpecs))
		throw error('order specification must be an array.');

	// check if empty
	if (orderSpecs.length === 0)
		return undefined;

	// parse order specifications
	return orderSpecs.map(orderSpec => {

		// validate the specification element
		if ((typeof orderSpec) !== 'string')
			throw error('order specification element is not a string.');

		// parse the specification element
		const orderSpecParts = orderSpec.match(
				/^\s*([^=\s].*?)\s*(?:=>\s*(asc|desc)\s*)?$/i);
		if (orderSpecParts === null)
			throw error(
				'order specification element "' + orderSpec +
					'" has invalid syntax.');

		// parse the value expression
		const valueExpr = new ValueExpression(valueExprCtx, orderSpecParts[1]);

		// add used properties to the tree
		valueExpr.usedPropertyPaths.forEach(p => { userPropPaths.add(p); });

		// return order specification element object
		return new QueryOrderSpec(
			valueExpr,
			(orderSpecParts[2] && (orderSpecParts[2].toLowerCase() === 'desc')));
	});
};
