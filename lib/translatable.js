'use strict';


const REBASED = Symbol('REBASED');

/**
 * Base class for items that are translatable into SQL.
 *
 * @memberof module:x2node-queries
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
	 * @returns {module:x2node-queries~Translatable} Rebased translatable.
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
	 * Paths of all properties referred by the translatable. The paths include
	 * the base path from the value expression context used to build the
	 * translatable. If the translatable is rebased, the paths also include the
	 * base path.
	 *
	 * @type {external:Set.<string>}
	 * @readonly
	 */
	get usedPropertyPaths() { return this._usedPropertyPaths; }

	/**
	 * Translate this translatable into SQL.
	 *
	 * @param {module:x2node-queries~TranslationContext} ctx Translation context.
	 * @returns {string} The SQL.
	 */
	translate(ctx) {

		throw new Error('Not implemented.');
	}
}

// export the class
module.exports = Translatable;
