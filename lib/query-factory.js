'use strict';

const Query = require('./query.js');
const QuerySpec = require('./query-spec.js');


/**
 * Query factory.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class QueryFactory {

	/**
	 * <b>The constructor is not accessible from the client code. Instances are
	 * created using module's
	 * [createQueryFactory]{@link module:x2node-queries.createQueryFactory}
	 * function.</b>
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver Database driver to use.
	 */
	constructor(dbDriver) {

		// lookup the driver
		this._dbDriver = dbDriver;
	}

	/**
	 * Build a query. Once built, the query can be used multiple times.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} topRecordTypeName Name of the top record type to fetch.
	 * @param {Object} [querySpec] Query specification. If unspecified (same as
	 * an empty object), all records are fetched with all properties that are
	 * fetched by default and the records are returned in no particular order.
	 * @param {string[]} [querySpec.props] Record properties to include. If
	 * unspecified, all properties that are fetched by default are included
	 * (equivalent to <code>['*']</code>). Note, that record id property is
	 * always included.
	 * @param {Object} [querySpec.filter] The filter specification. If
	 * unspecified, all records are included.
	 * @param {string[]} [querySpec.order] The records order specification. If
	 * unspecified, the records are returned in no particular order.
	 * @param {number[]} [querySpec.range] The range specification, which is a
	 * two-element array where the first element is the first record index
	 * starting from zero and the second element is the maximum number of records
	 * to return. If the range is not specified, all matching records are
	 * returned.
	 * @returns {module:x2node-queries~Query} The query object.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid, the top record type is unknown or the provided
	 * record types library is not suitable for the query specification.
	 */
	build(recordTypes, topRecordTypeName, querySpec) {

		return new Query(this._dbDriver, new QuerySpec(
			recordTypes, topRecordTypeName, (querySpec ? querySpec : {})));
	}
}

module.exports = QueryFactory;
