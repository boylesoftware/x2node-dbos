'use strict';

const common = require('x2node-common');

const FetchQuery = require('./fetch-query.js');
const placeholders = require('./placeholders.js');
const Transaction = require('./transaction.js');


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

		this._dbDriver = dbDriver;
	}

	/**
	 * Build a query. Once built, the query can be used multiple times.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} recordTypeName Name of the top record type to fetch.
	 * @param {Object} [querySpec] Query specification. If unspecified (same as
	 * an empty object), all records are fetched with all properties that are
	 * fetched by default and the records are returned in no particular order.
	 * @param {string[]} [querySpec.props] Record properties to include. If
	 * unspecified, all properties that are fetched by default are included
	 * (equivalent to <code>['*']</code>). Note, that record id property is
	 * always included. To include super-properties, the super-property name is
	 * included among the patterns starting with a dot. Records are not fetched
	 * if only super-properties are provided. To include records as well, a "*"
	 * can be added.
	 * @param {Array[]} [querySpec.filter] The filter specification. If
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
	 * record types library is not suitable for the specified query.
	 */
	buildFetch(recordTypes, recordTypeName, querySpec) {

		// check if record types library is compatible
		if (!placeholders.isTagged(recordTypes))
			throw new common.X2UsageError(
				'Record types library does not have the queries extension.');

		// check top record type existense
		if (!recordTypes.hasRecordType(recordTypeName))
			throw new common.X2UsageError(
				'Requested top record type ' + recordTypeName +
					' is unknown.');

		// parse records query specification
		let selectedPropPatterns, selectedSuperProps;
		let filterSpec, orderSpec, rangeSpec;
		if (querySpec) {
			if (querySpec.props) {
				if (querySpec.props.length === 0)
					selectedPropPatterns = new Array();
				else for (let propPattern of querySpec.props) {
					if (propPattern.startsWith('.')) {
						if (!selectedSuperProps)
							selectedSuperProps = new Array();
						selectedSuperProps.push(propPattern.substring(1));
					} else {
						if (!selectedPropPatterns)
							selectedPropPatterns = new Array();
						selectedPropPatterns.push(propPattern);
					}
				}
				if (selectedPropPatterns) {
					orderSpec = querySpec.order;
					rangeSpec = querySpec.range;
				}
			} else {
				selectedPropPatterns = [ '*' ];
				orderSpec = querySpec.order;
				rangeSpec = querySpec.range;
			}
			filterSpec = querySpec.filter;
		} else {
			selectedPropPatterns = [ '*' ];
		}

		// build and return the query
		return new FetchQuery(
			this._dbDriver, recordTypes, recordTypeName, selectedPropPatterns,
			selectedSuperProps, filterSpec, orderSpec, rangeSpec);
	}

	/**
	 * Get new transaction handler. The transaction has not been started.
	 *
	 * @param {*} connection The database connection compatible with the database
	 * driver.
	 * @returns {module:x2node-queries~Transaction} The transaction handler.
	 */
	newTransaction(connection) {

		return new Transaction(this._dbDriver, connection);
	}
}

// export the class
module.exports = QueryFactory;
