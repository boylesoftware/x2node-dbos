'use strict';

const common = require('x2node-common');

const ValueExpression = require('./value-expression.js');
const Translatable = require('./translatable.js');


/**
 * Records order specification.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~Translatable
 */
class RecordsOrder extends Translatable {

	/**
	 * Create new empty specification.
	 */
	constructor() {
		super();

		this._elements = new Array();
	}

	// implementation
	rebase(basePropPath) {

		const rebased = super.rebase(basePropPath);
		for (let i = 0, len = rebased._elements.length; i < len; i++)
			rebased._elements[i] = rebased._elements[i].rebase(basePropPath);

		return rebased;
	}

	/**
	 * Add element to the order specification.
	 *
	 * @param {module:x2node-dbos~RecordsOrderElement} element Element to add.
	 * @returns {module:x2node-dbos~RecordsOrder} This order specification.
	 */
	addElement(element) {

		this._elements.push(element);
		element.usedPropertyPaths.forEach(p => {
			this._usedPropertyPaths.add(p);
		});

		return this;
	}

	/**
	 * Order elements.
	 *
	 * @member {Array.<module:x2node-dbos~RecordsOrderElement>}
	 * @readonly
	 */
	get elements() { return this._elements; }
}

/**
 * Single records order element.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~Translatable
 */
class RecordsOrderElement extends Translatable {

	/**
	 * Create new element.
	 *
	 * @param {module:x2node-dbos~ValueExpression} valueExpr Value exression.
	 * @param {boolean} reverse <code>true</code> for descending order.
	 */
	constructor(valueExpr, reverse) {
		super();

		this._valueExpr = valueExpr;
		this._reverse = reverse;

		valueExpr.usedPropertyPaths.forEach(p => {
			this._usedPropertyPaths.add(p);
		});
	}

	// translate this element into SQL
	translate(ctx) {

		return (this._valueExpr.translate(ctx) + (this._reverse ? ' DESC' : ''));
	}
}

/**
 * Parse records order specification and build the order.
 *
 * @protected
 * @param {module:x2node-dbos~ValueExpressionContext} valueExprCtx Value
 * expression context to use.
 * @param {Array.<string>} orderSpecs Raw order specification.
 * @returns {module:x2node-dbos~RecordsOrder} Parsed order specification, or
 * <code>undefined</code> if the raw specifications array is empty.
 * @throws {module:x2node-common.X2UsageError} If the specification is invalid.
 */
exports.buildOrder = function(valueExprCtx, orderSpecs) {

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
	return orderSpecs.reduce((order, elementSpec) => {

		// validate the specification element
		if ((typeof elementSpec) !== 'string')
			throw error('order specification element is not a string.');

		// parse the specification element
		const specParts = elementSpec.match(
				/^\s*([^=\s].*?)\s*(?:=>\s*(asc|desc)\s*)?$/i);
		if (specParts === null)
			throw error(
				'order specification element "' + elementSpec +
					'" has invalid syntax.');

		// create add order specification element object
		return order.addElement(new RecordsOrderElement(
			new ValueExpression(valueExprCtx, specParts[1]),
			(specParts[2] && (specParts[2].toLowerCase() === 'desc'))));

	}, new RecordsOrder());
};
