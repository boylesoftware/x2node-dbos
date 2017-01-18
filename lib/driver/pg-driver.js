'use strict';

const StandardDBDriver = require('./standard-driver.js');


/**
 * Symbol used to indicate that there is an error on the query object.
 *
 * @private
 */
const HAS_ERROR = Symbol();

/**
 * Symbol used to indicate that header needs to be passed to the callback.
 *
 * @private
 */
const SET_HEADER = Symbol();

/**
 * PostgreSQL database driver.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @extends {module:x2node-queries~StandardDBDriver}
 * @implements {module:x2node-queries.DBDriver}
 */
class PostgreSQLDBDriver extends StandardDBDriver {

	execute(connection, statement, handler) {
		;console.log('>>> SQL: [' + statement + ']');

		const querySpec = {
			text: statement
		};
		if (!handler.noRowsAsArrays)
			querySpec.rowMode = 'array';

		const query = connection.query(querySpec);

		query[HAS_ERROR] = false;

		query.on('error', err => {
			if (query[HAS_ERROR])
				return;
			query[HAS_ERROR] = true;
			handler.onError(err);
		});

		query.on('end', () => {
			if (!query[HAS_ERROR])
				handler.onSuccess();
		});

		if (handler.onHeader || handler.onRow) {
			query[SET_HEADER] = handler.onHeader;
			query.on('row', (row, result) => {
				if (query[HAS_ERROR])
					return;
				try {
					if (query[SET_HEADER]) {
						handler.onHeader(result.fields.map(field => field.name));
						query[SET_HEADER] = false;
					}
					handler.onRow(row);
				} catch (err) {
					query[HAS_ERROR] = true;
					handler.onError(err);
				}
			});
		}
	}
};

module.exports = PostgreSQLDBDriver;
