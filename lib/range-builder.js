'use strict';

const common = require('x2node-common');


/**
 * Query range.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QueryRange {

	constructor(offset, limit) {

		this._offset = offset;
		this._limit = limit;
	}

	get offset() { return this._offset; }

	get limit() { return this._limit; }
}

/**
 * Parse query range specification and build the range object.
 *
 * @private
 * @param {number[]} rangeSpec Two-element number array of the raw range
 * specification.
 * @returns {module:x2node-queries~QueryRange} Range object.
 * @throws {module:x2node-common.X2UsageError} If the specification is invalid.
 */
exports.buildRange = function(rangeSpec) {

	// validate range specification array
	if (!Array.isArray(rangeSpec) || (rangeSpec.length !== 2))
		throw new common.X2UsageError(
			'Query range specification is not a two-element array.');
	const offset = rangeSpec[0];
	const limit = rangeSpec[1];

	// validate range values
	if (((typeof offset) !== 'number') || !Number.isFinite(offset) ||
		(offset < 0) ||
		((typeof limit) !== 'number') || !Number.isFinite(limit) || (limit < 0))
		throw new common.X2UsageError(
			'Invalid query range specification: the offset or the limit' +
				' value is not a number or is negative.');

	// create and return the range object
	return new QueryRange(offset, limit);
};
