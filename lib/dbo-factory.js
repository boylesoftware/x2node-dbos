'use strict';

const common = require('x2node-common');

const FetchDBO = require('./fetch-dbo.js');
const InsertDBO = require('./insert-dbo.js');
const Transaction = require('./transaction.js');


/**
 * Database operations (DBO) factory.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class DBOFactory {

	/**
	 * <b>The constructor is not accessible from the client code. Instances are
	 * created using module's
	 * [createDBOFactory]{@link module:x2node-queries.createDBOFactory}
	 * function.</b>
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver Database driver to use.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library to build queries against.
	 */
	constructor(dbDriver, recordTypes) {

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
	}

	/**
	 * Build a database <em>fetch</em> operation, which queries the database with
	 * <code>SELECT</code> statements and returns the results. Once built, the
	 * DBO can be executed multiple times.
	 *
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
	 * @returns {module:x2node-queries~FetchDBO} The DBO object.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the top record type is unknown.
	 */
	buildFetch(recordTypeName, querySpec) {

		// check top record type existense
		if (!this._recordTypes.hasRecordType(recordTypeName))
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
						if (propPattern.indexOf('.', 1) > 0)
							throw new common.X2UsageError(
								'Super-property name may not contain dots.');
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

		// build and return the DBO
		return new FetchDBO(
			this._dbDriver, this._recordTypes, recordTypeName,
			selectedPropPatterns, selectedSuperProps, filterSpec, orderSpec,
			rangeSpec);
	}

	/**
	 * Build a database <em>insert</em> operation, which creates new records with
	 * <code>INSERT</code> statements.
	 *
	 * @param {string} recordTypeName Name of the record type to insert.
	 * @param {*} record The record to insert.
	 * @returns {module:x2node-queries~InsertDBO} The DBO object.
	 * @throws {module:x2node-common.X2UsageError} If the provided record data is
	 * invalid or the record type is unknown.
	 */
	buildInsert(recordTypeName, record) {

		// check top record type existense
		if (!this._recordTypes.hasRecordType(recordTypeName))
			throw new common.X2UsageError(
				'Specified record type ' + recordTypeName + ' is unknown.');

		// build and return the DBO
		return new InsertDBO(
			this._dbDriver, this._recordTypes, recordTypeName, record);
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
module.exports = DBOFactory;
