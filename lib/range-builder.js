'use strict';

const common = require('x2node-common');


/**
 * Record collection range.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 */
class RecordsRange {

	/**
	 * Create new range specification.
	 *
	 * @param {number} offset Zero-based range offset.
	 * @param {number} limit The range limit.
	 */
	constructor(offset, limit) {

		this._offset = offset;
		this._limit = limit;
	}

	/**
	 * Zero-based range offset.
	 *
	 * @member {number}
	 * @readonly
	 */
	get offset() { return this._offset; }

	/**
	 * The range limit.
	 *
	 * @member {number}
	 * @readonly
	 */
	get limit() { return this._limit; }
}

/**
 * Parse records collection range specification and build the range object.
 *
 * @protected
 * @param {Array.<number>} rangeSpec Two-element number array of the raw range
 * specification.
 * @returns {module:x2node-dbos~RecordsRange} Range object.
 * @throws {module:x2node-common.X2SyntaxError} If the specification is invalid.
 */
exports.buildRange = function(rangeSpec) {

	// validate range specification array
	if (!Array.isArray(rangeSpec) || (rangeSpec.length !== 2))
		throw new common.X2SyntaxError(
			'Records range specification is not a two-element array.');
	const offset = rangeSpec[0];
	const limit = rangeSpec[1];

	// validate range values
	if (((typeof offset) !== 'number') || !Number.isInteger(offset) ||
		(offset < 0) ||
		((typeof limit) !== 'number') || !Number.isInteger(limit) || (limit < 0))
		throw new common.X2SyntaxError(
			'Invalid records range specification: the offset or the limit' +
				' value is not a non-negative integer.');

	// create and return the range object
	return new RecordsRange(offset, limit);
};
