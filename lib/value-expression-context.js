'use strict';


/**
 * Value expression context.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class ValueExpressionContext {

	/**
	 * Create new context.
	 *
	 * @param {string} basePath Property path added as a prefix to all properties
	 * involved in the expressions.
	 * @param {module:x2node-records~PropertiesContainer[]} containerChain List
	 * of parent containers used to resolve references parent record properties.
	 * Properties used in the expressions are resolved against the last container
	 * in the chain.
	 */
	constructor(basePath, containerChain) {

		this._basePath = basePath;
		this._containerChain = containerChain;
	}

	/**
	 * Context base path. May be empty.
	 *
	 * @type {string}
	 * @readonly
	 */
	get basePath() { return this._basePath; }

	/**
	 * Base container (the last container in the chain).
	 *
	 * @type {module:x2node-records~PropertiesContainer}
	 * @readonly
	 */
	get baseContainer() {

		return this._containerChain[this._containerChain.length - 1];
	}
}

module.exports = ValueExpressionContext;
