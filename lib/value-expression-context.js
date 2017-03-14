'use strict';

const common = require('x2node-common');


/**
 * Value expression context.
 *
 * @private
 * @memberof module:x2node-dbos
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
	 * @member {string}
	 * @readonly
	 */
	get basePath() { return this._basePath; }

	/**
	 * Base container (the last container in the chain).
	 *
	 * @member {module:x2node-records~PropertiesContainer}
	 * @readonly
	 */
	get baseContainer() {

		return this._containerChain[this._containerChain.length - 1];
	}

	/**
	 * The full context container chain.
	 *
	 * @member {Array.<module:x2node-records~PropertiesContainer>}
	 * @readonly
	 */
	get containerChain() { return this._containerChain; }

	/**
	 * Descriptor of the context's base property.
	 *
	 * @member {module:x2node-records~PropertyDescriptor}
	 * @readonly
	 */
	get basePropertyDesc() {

		return this._containerChain[this._containerChain.length - 2]
			.getPropertyDesc(
				this._basePath.substring(this._basePath.lastIndexOf('.') + 1));
	}

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

		let numUps = (
			propRefSections[1] ? propRefSections[1].length / 2 : 0);
		if (numUps >= this._containerChain.length)
			throw new common.X2UsageError(
				'Invalid parent property reference "' + propRef +
					'": too many ups.');

		let basePath = this._basePath;
		while (numUps-- > 0)
			basePath = basePath.substring(0, basePath.lastIndexOf('.'));

		return (basePath.length > 0 ? basePath + '.' : '') + propRefSections[2];
	}

	/**
	 * Get context based at the property specified by a reference within this
	 * context.
	 *
	 * @param {string} propRef Property reference in this context.
	 * @returns {module:x2node-dbos~ValueExpressionContext} New context.
	 * @throws {module:x2node-common.X2UsageError} If the reference is invalid.
	 */
	getRelativeContext(propRef) {

		// parse the property reference
		const propRefSections = propRef.match(/^((?:\^\.)*)?(.*)$/);

		// identify number of ups
		let numUps = (
			propRefSections[1] ? propRefSections[1].length / 2 : 0);
		if (numUps >= this._containerChain.length)
			throw new common.X2UsageError(
				'Invalid parent property reference "' + propRef +
					'": too many ups.');

		// go up the chain
		const newContainerChain = Array.from(this._containerChain);
		let newBasePath = this._basePath;
		while (numUps-- > 0) {
			newContainerChain.pop();
			newBasePath = newBasePath.substring(0, newBasePath.lastIndexOf('.'));
		}

		// go down the chain
		propRefSections[2].split('.').forEach(propName => {
			const container = newContainerChain[newContainerChain.length - 1];
			if (container === null)
				throw new common.X2UsageError(
					'Invalid property reference "' + propRef +
						'": non container property in the middle of the path.');
			if (!container.hasProperty(propName))
				throw new common.X2UsageError(
					'Invalid property reference "' + propRef +
						'": record type ' + String(container.recordTypeName) +
						' does not have property ' + container.nestedPath +
						propName + '.');
			newContainerChain.push(
				container.getPropertyDesc(propName).nestedProperties/* || null*/);
			if (newBasePath.length > 0)
				newBasePath += '.';
			newBasePath += propName;
		});

		// create and return the new context
		return new ValueExpressionContext(newBasePath, newContainerChain);
	}
}

// export the class
module.exports = ValueExpressionContext;
