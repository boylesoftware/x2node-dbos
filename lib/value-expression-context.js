'use strict';

const common = require('x2node-common');


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

	/**
	 * The full context container chain.
	 *
	 * @type {module:x2node-records~PropertiesContainer[]}
	 * @readonly
	 */
	get containerChain() { return this._containerChain; }

	/**
	 * Normalize specified property reference againt the context and return the
	 * resulting property path.
	 *
	 * @param {string} propRef Property reference from a value expression.
	 * @returns {string} Resolved property path.
	 * @throws {module:x2node-common.X2UsageError} If the reference is invalid.
	 */
	normalizePropertyRef(propRef) {

		const propRefSections = propRef.match(/^((?:\^\.)*)?(.*)$/);

		const numUps = (
			propRefSections[1] ? propRefSections[1].length / 2 : 0);
		if (numUps >= this._containerChain.length)
			throw new common.X2UsageError(
				'Invalid parent property reference "' + propRef +
					'": too many ups.');

		let basePath = this._basePath;
		for (let i = 0; i < numUps; i++)
			basePath = basePath.substring(0, basePath.lastIndexOf('.'));

		return (basePath.length > 0 ? basePath + '.' : '') + propRefSections[2];
	}
}

module.exports = ValueExpressionContext;
