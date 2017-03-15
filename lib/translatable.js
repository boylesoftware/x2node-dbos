'use strict';


const REBASED = Symbol('REBASED');

/**
 * Base class for items that are translatable into SQL.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 * @abstract
 */
class Translatable {

	/**
	 * Create new translatable base.
	 */
	constructor() {

		this._usedPropertyPaths = new Set();
	}

	/**
	 * Create and return new translatable that is identical to this one except
	 * that all used property references are prefixed with the new base property
	 * path.
	 *
	 * <p>The method does not modify this translatable.
	 *
	 * @param {string} basePropPath New base property path. If empty string, this
	 * translable instance is returned as is.
	 * @returns {module:x2node-dbos~Translatable} Rebased translatable.
	 */
	rebase(basePropPath) {

		if (basePropPath.length === 0)
			return this;

		const rebased = Object.create(this);
		rebased[REBASED] = true;

		rebased._usedPropertyPaths = new Set();
		this._usedPropertyPaths.forEach(p => {
			rebased._usedPropertyPaths.add(basePropPath + '.' + p);
		});

		const origTranslate = this.translate;
		rebased.translate = function(ctx) {
			return origTranslate.call(rebased, ctx.rebase(basePropPath));
		};

		return rebased;
	}

	/**
	 * Add property paths to the used property paths set.
	 *
	 * @param {(Array.<string>|Set.<string>)} propPaths Property paths to add.
	 */
	addUsedPropertyPaths(propPaths) {

		propPaths.forEach(propPath => {
			this._usedPropertyPaths.add(propPath);
		});
	}

	/**
	 * Paths of all properties referred by the translatable. The paths include
	 * the base path from the value expression context used to build the
	 * translatable. If the translatable is rebased, the paths also include the
	 * base path.
	 *
	 * @member {Set.<string>}
	 * @readonly
	 */
	get usedPropertyPaths() { return this._usedPropertyPaths; }

	/**
	 * Translate this translatable into SQL.
	 *
	 * @function module:x2node-dbos~Translatable#translate
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @returns {string} The SQL.
	 */
}

// export the class
module.exports = Translatable;
