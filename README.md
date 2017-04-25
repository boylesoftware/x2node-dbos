# X2 Framework for Node.js | Database Operations

This module provides a SQL query builder that together with [x2node-rsparser](https://www.npmjs.com/package/x2node-rsparser) module allows applications to perform complex SQL database operations while concentrating on the high level data structures and the business logic. The module is a part of X2 Framework.

## Table of Contents

* [Usage](#usage)
  * [Fetching Records](#fetching-records)
  * [Creating Records](#creating-records)
  * [Updating Records](#updating-records)
  * [Deleting Records](#deleting-records)
* [Record Type Definition Extentions](#record-type-definition-extentions)
  * [Mapping Record Types](#mapping-record-types)
  * [Mapping Stored Record Properties](#mapping-stored-record-properties)
    * [Scalar Properties](#scalar-properties)
    * [Collection Properties](#collection-properties)
  * [Dependent Record References](#dependent-record-references)
  * [Shared Link Tables](#shared-link-tables)
  * [Calculated Properties](#calculated-properties)
  * [Aggregate Properties](#aggregate-properties)
  * [Embedded Objects](#embedded-objects)
  * [Polymorphic Objects](#polymorphic-objects)
  * [Ordered Collections](#ordered-collections)
  * [Filtered Collection Views](#filtered-collection-views)
  * [Record Meta-Info Properties](#record-meta-info-properties)
  * [Super-Properties](#!super-properties)
* [Fetch DBO](#fetch-dbo)
  * [Selected Properties Specification](#selected-properties-specification)
  * [Filter Specification](#filter-specification)
  * [Order Specification](#order-specification)
  * [Range Specification](#range-specification)
* [Insert DBO](#insert-dbo)
* [Update DBO](#update-dbo)
* [Delete DBO](#delete-dbo)
* [Transactions](#!transactions)
* [Data Sources](#data-sources)
* [Database Drivers](#database-drivers)

## Usage

Let's say we have the following schema in a _MySQL_ database:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	fname VARCHAR(30) NOT NULL,
	lname VARCHAR(30) NOT NULL
);

CREATE TABLE products (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(30) NOT NULL UNIQUE,
	price DECIMAL(5,2) NOT NULL
);

CREATE TABLE orders (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	account_id INTEGER UNSIGNED NOT NULL,
	placed_on TIMESTAMP DEFAULT 0,
	status VARCHAR(10) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE TABLE order_items (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	order_id INTEGER UNSIGNED NOT NULL,
	product_id INTEGER UNSIGNED NOT NULL,
	quantity TINYINT UNSIGNED NOT NULL,
	FOREIGN KEY (order_id) REFERENCES orders (id),
	FOREIGN KEY (product_id) REFERENCES products (id),
	UNIQUE (order_id, product_id)
);
```

The module allows linking a record types library (see [x2node-records](https://www.npmjs.com/package/x2node-records) module) to the database table and then constructing _Database Operations_ or _DBOs_ against the described records. When executed, the DBOs are translated to SQL statements that are run against the database and the results are parsed and returned back to the application in a form of records and other types of result objects.

The DBOs are constructed using a _DBO Factory_ provided by the module. The factory is normally created once by the application for the given record types library and the database driver and used to construct DBOs throught the application's lifecycle. The database drivers supported out-of-the-box are [mysql](https://www.npmjs.com/package/mysql) (and other compatible implementations) and [pg](https://www.npmjs.com/package/pg). Custom driver implementations can be provided to the DBO factory as well.

The DBO factory is capable of constructing four types of DBOs: _fetch DBO_ for loading records from the database, _insert DBO_ for creating new records, _update DBO_ for patching existing records and _delete DBO_ for deleting records. Some most basic examples for the four follow.

### Fetching Records

Let's assume we want to load order records from the database defined above:

```javascript
// use MySQL database driver
const mysql = require('mysql');

// load the framework modules
const records = require('x2node-records');
const dbos = require('x2node-dbos');

// construct our sample record types library with DBOs extensions
const recordTypes = records.with(dbos).buildLibrary({
	recordTypes: {
		'Account': {
			table: 'accounts',
			properties: {
				'id': {
					valueType: 'number',
					role: 'id'
				},
				'firstName': {
					valueType: 'string',
					column: 'fname'
				},
				'lastName': {
					valueType: 'string',
					column: 'lname'
				},
				'orderRefs': {
					valueType: 'ref(Order)[]',
					reverseRefProperty: 'accountRef'
				}
			}
		},
		'Product': {
			table: 'products',
			properties: {
				'id': {
					valueType: 'number',
					role: 'id'
				},
				'name': {
					valueType: 'string'
				},
				'price': {
					valueType: 'number'
				}
			}
		},
		'Order': {
			table: 'orders',
			properties: {
				'id': {
					valueType: 'number',
					role: 'id'
				},
				'accountRef': {
					valueType: 'ref(Account)',
					column: 'account_id',
					modifiable: false
				},
				'placedOn': {
					valueType: 'datetime',
					column: 'placed_on',
					modifiable: false
				},
				'status': {
					valueType: 'string'
				},
				'items': {
					valueType: 'object[]',
					table: 'order_items',
					parentIdColumn: 'order_id',
					properties: {
						'id': {
							valueType: 'number',
							role: 'id'
						},
						'productRef': {
							valueType: 'ref(Product)',
							column: 'product_id',
							modifiable: false
						},
						'quantity': {
							valueType: 'number'
						}
					}
				}
			}
		}
	}
});

// create DBO factory against the record types library and the DB driver
const dboFactory = dbos.createDBOFactory(recordTypes, 'mysql');

// build a fetch DBO for selecting 5 recent pending orders for a specific account
const fetchDBO = dboFactory.buildFetch('Order', {

	// select all order record properties
	props: [ '*' ],

	// filter by status and account id passed in as a query parameter when executed
	filter: [
		[ 'status => is', 'PENDING' ],
		[ 'accountRef => is', dbos.param('accountId') ]
	],

	// order in descending order by order placement timestamp
	order: [
		'placedOn => desc'
	],

	// select first 5 matched order records (that's right, records, not rows!)
	range: [ 0, 5 ]
});

// configure database connection and connect
const connection = mysql.createConnection({
	host: 'localhost',
	database: 'mydatabase',
	user: 'myuser',
	password: 'mypassword'
});
connection.connect(err => {
	if (err) {
		console.error('connection error');
		throw err;
	}
});

// execute the fetch DBO on the connection
fetchDBO.execute(connection, null, {
	accountId: 10 // for example we want orders for the account id #10
}).then(
	result => {
		console.log('result:\n' + JSON.stringify(result, null, '  '));
		connection.end();
	},
	err => {
		console.error('error:', err);
		connection.end();
	}
);
```

The `result` object will include a `records` property, which will be an array with all matched order records (up to 5 in our example).

The `null` passed into the DBO's `execute()` method as the second argument is for the actor performing the operation. This is described later. For now, the `null` makes the operation execution anonymous.

### Creating Records

To create a new order record we could use the following code:

```javascript
// create the DBO and pass new order record template to it
const insertDBO = dboFactory.buildInsert('Order', {
	accountRef: 'Account#10',
	placedOn: (new Date()).toISOString(),
	status: 'PENDING',
	items: [
		{
			productRef: 'Product#1',
			quantity: 1
		},
		{
			productRef: 'Product#2',
			quantity: 10
		}
	]
});

// execute the DBO
insertDBO.execute(connection, null).then(
	recordId => {
		console.log('new Order id: ' + recordId);
	},
	err => {
		console.error('error:', err);
	}
);
```

### Updating Records

To update the order's first item's quantity, add a new item to the order and change the order status, the following code could be used:

```javascript
// build the patch specification
const patches = require('x2node-patches');
const patch = patches.build(recordTypes, 'Order', [
	{ op: 'replace', path: '/items/0/quantity', value: 2 },
	{ op: 'add', path: '/items/-', value: {
		productRef: 'Product#3',
		quantity: 1
	} },
	{ op: 'replace', path: '/status', value: 'PROCESSING' }
]);

// create the DBO passing the patch and the order record selector filter
const updateDBO = dboFactory.buildUpdate('Order', patch, [
	[ 'id => is', dbos.param('orderId') ]
]);

// execute the DBO
updateDBO.execute(connection, null, null, {
	orderId: 1
}).then(
	result => {
		console.log('update operation status:\n' + JSON.stringify(result, null, '  '));
	},
	err => {
		console.error('error:', err);
	}
);
```

The example above uses X2 Framework's [x2node-patches](https://www.npmjs.com/package/x2node-rsparser) module to build the patch specification using [JSON Patch](https://tools.ietf.org/html/rfc6902) notation.

The third `null` argument to the DBO's `execute()` method is for an optional record validation function that allows validating records after the patch is applied but before the record is saved into the database. This functionality is described later in this manual.

The `result` object returned by the operation will contain information about what records were updated as well as the updated records data itself. This is also described in detail later.

### Deleting Records

Deleting an order record could be done like this:

```javascript
// build the DBO for the specific order id
const deleteDBO = dboFactory.buildDelete('Order', [
	[ 'id => is', dbos.param('orderId') ]
]);

// execute the DBO and pass the order id to it
deleteDBO.execute(connection, null, {
	orderId: 1
}).then(
	result => {
		console.log('delete operation status:\n' + JSON.stringify(result, null, '  '));
	},
	err => {
		console.error('error:', err);
	}
);
```

The `result` object will tell if any records were matched and actually deleted.

## Record Type Definition Extentions

The DBOs module introduces a number of attributes used in record types library definitions to map records and their properties to the database tables and columns. This mapping allows the DBO factory to construct the SQL queries. The DBOs module itself is a record types library extension and must be added to the library for the extended attributes to get processed:

```javascript
const records = require('x2node-records');
const dbos = require('x2node-dbos');

const recordTypes = records.with(dbos).buildLibrary({
	...
});
```

### Mapping Record Types

Every record type must have the main table it is mapped to. This is the table that has the record id as its primary key and normally stores the bulk of the scalar record properties as its columns. To associate a record type with a table, a `table` record type definition attribute is used:

```javascript
const recordTypes = records.with(dbos).buildLibrary({
	recordTypes: {
		...
		'Order': {
			table: 'orders',
			properties: {
				...
			}
		},
		...
	}
});
```

If `table` attribute is not specified, the record type name is used.

### Mapping Stored Record Properties

A number of DBOs module specific attributes is used to map record properties to the tables and columns used to store them.

#### Scalar Properties

The simples case of a record property is a scalar property stored in the record type table's column. To associate such property with the specific column, a `column` property definition attribute is used:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'firstName': {
				valueType: 'string',
				column: 'fname'
			},
			'zip': {
				valueType: 'string',
				optional: true
			}
		}
	},
	...
}
```

This will map the `firstName` property to the `fname` column of the `accounts` table and the `zip` property to its `zip` column (if `column` attribute is not specified, the property name is used):

```sql
CREATE TABLE accounts (
	...
	fname VARCHAR(30) NOT NULL,
	zip CHAR(5),
	...
);
```

The column type must match the property type. Also, the column may be nullable if the property is optional.

A property does not have to be stored in the main record type table. If a property is stored in a separate table, the property definition must have a `table` attribute and a `parentIdColumn` attribute, which specifies the column in the property table that points back at the parent record id. Normally it does not make much sense to store scalar, simple value properties in a separate table, but it will work nonetheless. It makes more sense when it comes to nested object properties. For example, if an account address is stored in a separate table in a one-to-one relation with the main record table:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED PRIMARY KEY,
	...
);

CREATE TABLE account_addresses (
	account_id INTEGER UNSIGNED NOT NULL UNIQUE,
	street VARCHAR(50) NOT NULL,
	...
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

then it can be mapped like this:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'address': {
				valueType: 'object',
				table: 'account_addresses',
				parentIdColumn: 'account_id',
				properties: {
					'street': {
						valueType: 'string'
					}
					...
				}
			}
		}
	},
	...
}
```

#### Collection Properties

When it comes to array and map properties, use of property tables becomes necessary due to the RDBMS nature. The example above can be modified to allow multiple addresses on an account:

```sql
CREATE TABLE account_addresses (
	id INTEGER UNSIGNED PRIMARY KEY,
	account_id INTEGER UNSIGNED NOT NULL,
	street VARCHAR(50) NOT NULL,
	...
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

and the record type definition:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'addresses': {
				valueType: 'object[]',
				table: 'account_addresses',
				parentIdColumn: 'account_id',
				properties: {
					'id': {
						valueType: 'number',
						role: 'id'
					},
					'street': {
						valueType: 'string'
					}
					...
				}
			}
		}
	},
	...
}
```

If an address has a type ("Home", "Work", etc.):

```sql
CREATE TABLE account_addresses (
	id INTEGER UNSIGNED PRIMARY KEY,
	account_id INTEGER UNSIGNED NOT NULL,
	type VARCHAR(10) NOT NULL,
	street VARCHAR(50) NOT NULL,
	...
	FOREIGN KEY (account_id) REFERENCES accounts (id),
	UNIQUE (account_id, type)
);
```

then we could have the addresses on the account as a map:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'addresses': {
				valueType: 'object{}',
				keyPropertyName: 'type',
				table: 'account_addresses',
				parentIdColumn: 'account_id',
				properties: {
					'id': {
						valueType: 'number',
						role: 'id'
					},
					'type': {
						valueType: 'string'
					},
					'street': {
						valueType: 'string'
					}
					...
				}
			}
		}
	},
	...
}
```

If we don't want to have the address type as a property on the address record, then the key column can be specified using `keyColumn` property definition attribute instead of using `keyPropertyName`:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'addresses': {
				valueType: 'object{}',
				table: 'account_addresses',
				parentIdColumn: 'account_id',
				keyColumn: 'type',
				keyValueType: 'string',
				properties: {
					'id': {
						valueType: 'number',
						role: 'id'
					},
					'street': {
						valueType: 'string'
					}
					...
				}
			}
		}
	},
	...
}
```

Note, that in that case the `keyValueType` attribute must be provided as well (see [x2node-rsparser](https://www.npmjs.com/package/x2node-rsparser) module for details).

Simple value collection properties utilize the `column` attribute to map the column in the collection table. For example, if we have a list of phone numbers associated with the account:

```sql
CREATE TABLE account_phones (
	account_id INTEGER UNSIGNED NOT NULL,
	phone CHAR(10) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

the definition will be:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'phones': {
				valueType: 'string[]',
				table: 'account_phones',
				parentIdColumn: 'account_id',
				column: 'phone'
			}
		}
	},
	...
}
```

And if we have a phone type as a map key:

```sql
CREATE TABLE account_phones (
	account_id INTEGER UNSIGNED NOT NULL,
	type VARCHAR(10) NOT NULL,
	phone CHAR(10) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id),
	UNIQUE (account_id, type)
);
```

the definition will be:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'phones': {
				valueType: 'string{}',
				table: 'account_phones',
				parentIdColumn: 'account_id',
				keyColumn: 'type',
				keyValueType: 'string',
				column: 'phone'
			}
		}
	},
	...
}
```

Note that use of `keyColumn` attribute becomes the only option for simple value map properties (no `keyPropertyName` is appropriate).

There is an important deviation between the `keyColumn` and `keyPropertyName` attributes notion when it comes to reference property maps. Such maps (as well as arrays) introduce a link table between the referrer and the referred. If the `keyPropertyName` is used to map the map key, the key is located in the referred record type table. For example, if we extract the address records in the examples above into a separate record type, the tables will be:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED PRIMARY KEY,
	...
);

CREATE TABLE account_addresses (
	account_id INTEGER UNSIGNED NOT NULL,
	address_id INTEGER UNSIGNED NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id),
	FOREIGN KEY (address_id) REFERENCES addresses (id),
	UNIQUE (account_id, address_id)
);

CREATE TABLE addresses (
	id INTEGER UNSIGNED PRIMARY KEY,
	type VARCHAR(10) NOT NULL,
	street VARCHAR(50) NOT NULL,
	...
);
```

and the definitions:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'addressRefs': {
				valueType: 'ref(Address){}',
				keyPropertyName: 'type',
				table: 'account_addresses',
				parentIdColumn: 'account_id',
				column: 'address_id'
			}
		}
	},
	'Address': {
		table: 'addresses',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			'type': {
				valueType: 'string'
			},
			'street': {
				valueType: 'string'
			},
			...
		}
	},
	...
}
```

So, the `type` property for the map key is taken from the `addresses` table. On the other hand, if `keyColumn` attribute is used, the column is mapped to the link table:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED PRIMARY KEY,
	...
);

CREATE TABLE account_addresses (
	account_id INTEGER UNSIGNED NOT NULL,
	type VARCHAR(10) NOT NULL,
	address_id INTEGER UNSIGNED NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id),
	FOREIGN KEY (address_id) REFERENCES addresses (id),
	UNIQUE (account_id, type)
);

CREATE TABLE addresses (
	id INTEGER UNSIGNED PRIMARY KEY,
	street VARCHAR(50) NOT NULL,
	...
);
```

and the definitions:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'addressRefs': {
				valueType: 'ref(Address){}',
				table: 'account_addresses',
				parentIdColumn: 'account_id',
				keyColumn: 'type',
				keyValueType: 'string',
				column: 'address_id'
			}
		}
	},
	'Address': {
		table: 'addresses',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			'street': {
				valueType: 'string'
			},
			...
		}
	},
	...
}
```

### Dependent Record References

Some reference properties are not stored with the referring record. Instead, there is a reference property in the referred record type that points back at the referring record. The referred record type in this case is called a _dependent_ record type, because its records can exist only in the context of the referring record (unless the reverse reference property is optional). That is the referring record must exist for the referred record to point at it.

For example, if we have records types `Account` and `Order` and the account has a list of references to the orders made for that account. The order, even though a separate record type, has a reference back to the account it belongs and can only exist in a context of an account. This makes the `Order` a dependent record type.

Here are the tables:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	...
);

CREATE TABLE orders (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	account_id INTEGER UNSIGNED NOT NULL,
	...
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

and the definitions:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'orderRefs': {
				valueType: 'ref(Order)[]',
				reverseRefProperty: 'accountRef'
			}
		}
	},
	'Order': {
		table: 'orders',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			'accountRef': {
				valueType: 'ref(Account)',
				column: 'account_id',
				modifiable: false
			},
			...
		}
	},
	...
}
```

The `orderRefs` property of the `Account` record type is not stored anywhere in any tables and columns associated with it. Instead, it is a property of the dependent `Order` record type and is stored there on the order record in `accountRef` property. This is why the `orderRefs` property does not have any `table` and `column` definition attributes. Instead, it has a `reverseRefProperty` attribute that names the property in the referred record type that points back at the account.

The dependent record reference properties are only allowed as top record type properties (not in nested objects). And the reverse reference properties in the referred record types are also only allowed to be top record type properties and also they are required to be scalar, non-polymorph and not be dependent record references themselves. They are, however, allowed to be stored in a separate table (this may be useful when `keyColumn` is used with a dependent record reference map property, in which case the name key column will reside in the separate link table).

As will be shown later in this manual, normally, when a record with dependent record reference properties is deleted, the operation is automatically cascaded and all dependent records are deleted as well. This is called _strong dependencies_. In the example above, it makes sense to delete all order records associated with an account when the account record is deleted from the database. Sometimes, however, a dependent reference mechanism needs to be used to import a reference property to a record (for data access convenience), but the referred record type is not exactly dependent. This is called a _weak dependence_. One side effect of a weak dependence is that the deletion operation is not cascaded over it (the operation will fail on the database level if referred records exist and foreign key constraints are properly utilized). To make a dependent reference property weak, a `weakDependency` property definition attribute can be added with value `true`.

### Shared Link Tables

Sometimes, two record types may have references at each other, but none of them is dependent on another. For example, if we have record types `Account` and `Address` and they are in a many-to-many relationship so that a single address can be shared by multiple accounts and an account can have multiple addresses:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED PRIMARY KEY,
	...
);

CREATE TABLE accounts_addresses (
	account_id INTEGER UNSIGNED NOT NULL,
	address_id INTEGER UNSIGNED NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id),
	FOREIGN KEY (address_id) REFERENCES addresses (id),
	UNIQUE (account_id, address_id)
);

CREATE TABLE addresses (
	id INTEGER UNSIGNED PRIMARY KEY,
	...
);
```

At the same time, we want to have a list of addresses on the account records and we want to have a list of accounts that use an address on the address record. It can be done like this:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'addressRefs': {
				valueType: 'ref(Address)[]',
				table: 'accounts_addresses',
				parentIdColumn: 'account_id',
				column: 'address_id'
			}
		}
	},
	'Address': {
		table: 'addresses',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'accountRefs': {
				valueType: 'ref(Account)[]',
				table: 'accounts_addresses',
				parentIdColumn: 'address_id',
				column: 'account_id'
			}
		}
	},
	...
}
```

Properties `addressRefs` on record type `Account` and `accountRefs` on record type `Address` share the same link table `accounts_addresses`. This case is unique in the sense that modification of a record of one record type may have a side-effect of making changes in stored properties of some records of a different record type.

### Calculated Properties

It is possible to have properties on a record type that are not stored in the database directly, but are calculated every time a record is fetched from the database. Such properties are called _calculated properties_. Instead of a column and table, a calculated property has a value expression associated with it using `valueExpr` property definition attribute. The `valueExpr` attribute is a string that uses a special expression syntax that includes value literals, references to other properties of the record, value transformation functions and operators. Here is the expression language EBNF definition:

```ebnf
Expr = [ PlusOrMinus ] , Term , { PlusOrMinus , Term }
	| String
	| Boolean
	;

Term = Factor , { MulOrDiv , Factor } ;

Factor = Function
	| PropertyRef
	| Number
	| "(" , Expr , ")"
	;

Function = FunctionName , "(" , Expr , { "," , Expr } , ")" ;

PlusOrMinus = "+" | "-" ;

MulOrDiv = "*" | "/" ;

Boolean = "true" | "false" ;

String = '"' , { CHAR } , '"' | "'" , { CHAR } , "'" ;

Number = DIGIT , { DIGIT } , [ "." , DIGIT , { DIGIT } ] ;

FunctionName = ? See Description ? ;

PropertyRef = ? See Description ? ;
```

The following value transformation functions are supported:

* `length`, `len` - Takes single string argument, yields string length.

* `lower`, `lc`, `lcase`, `lowercase` - Takes single string argument, yields all lower-case string.

* `upper`, `uc`, `ucase`, `uppercase` - Takes single string argument, yields all upper-case string.

* `substring`, `sub`, `mid`, `substr` - Yields substring of the string provided as the first argument. The second argument is the first substring character index, starting from zero. The optional third argument is the maximum substring length. If not provided, the end of the input string is used.

* `lpad` - Pads the string provided as the first argument on the left to achieve the minimum length provided as the second argument using the character provided as the third argument as the padding character.

* `concat`, `cat` - Concatenate provided string arguments.

* `coalesce` - Yield first non-null argument.

The properties in the expressions are referred using dot notation. In a calculated nested object property, to refer to a property in the parent object a caret character is used. Multiple caret references can be used to hop over multiple parent objects.

For example, given the record types library definition used in the opening [Usage](#usage) section, the order records could be augmented with some calculated properties:

```javascript
{
	...
	'Order': {
		...
		properties: {
			...
			'customerName': {
				valueType: 'string',
				valueExpr: 'concat(accountRef.firstName, " ", accountRef.lastName)'
			},
			...
			'items': {
				...
				properties: {
					...
					'totalCost': {
						valueType: 'number',
						valueExpr: 'productRef.price * quantity'
					},
					'orderStatus': {
						valueType: 'string',
						valueExpr: '^.status'
					}
				}
			}
		}
	},
	...
}
```

This example shows that reference properties can be hopped over. Also, if there were more nesting levels, jumping over multiple children (`prop.nestedProp.moreNestedProp`) and multiple parents (`^.^.grandparentProp.nestedProp`) is possible with the dot notation.

When the records with calculated properties are fetched from the database, the expressions are converted into SQL value expressions and are calculated on the database side.

### Aggregate Properties

A special case of a calculated property is an _aggregate property_. An aggregate property requires a collection property, elements of which it aggregates, a expression for the aggregated values and optionally a filter to include only certain elements of the aggregated collection. An `aggregate` property definition attribute is used to achieve that. For example, our order records could include properties that aggregate the order line items like the following:

```javascript
{
	...
	'Order': {
		table: 'orders',
		properties: {
			...
			'itemsCount': {
				valueType: 'number',
				aggregate: {
					collection: 'items',
					valueExpr: 'id => count'
				}
			},
			'orderTotal': {
				valueType: 'number',
				aggregate: {
					collection: 'items',
					valueExpr: 'productRef.price * quantity => sum'
				}
			},
			'expensiveProductsCount': {
				valueType: 'number',
				aggregate: {
					collection: 'items',
					valueExpr: 'quantity => sum',
					filter: [
						[ 'productRef.price => min', 1000 ]
					]
				}
			},
			...
			'items': {
				valueType: 'object[]',
				table: 'order_items',
				parentIdColumn: 'order_id',
				properties: {
					'id': {
						valueType: 'number',
						role: 'id'
					},
					'productRef': {
						valueType: 'ref(Product)',
						column: 'product_id',
						modifiable: false
					},
					'quantity': {
						valueType: 'number'
					}
				}
			}
		}
	},
	...
}
```

The value expressions for the aggregate properties have format:

`Expr => AggregateFunc`

Where `Expr` is a regular value expression calculated in the context of the aggregated collection property, and `AggregateFunc` is one of:

* `count` - Number of unique values yielded by the aggregated expression.

* `sum` - Sum of the values yielded by the aggregated expression.

* `min` - The smallest value yielded by the aggregated expression.

* `max` - The largest value yielded by the aggregated expression.

* `avg` - Average of the values yielded by the aggregated expression.

The optional filter specification format will be discussed later in this manual when we talk about fetch DBO.

### Embedded Objects

A scalar nested object property does not have to be stored in a separate table. The nested object's properties can be stored in the columns of the main record type table and the nested object can be used simply to organize the JSON representation of the record. For example, an address on an account record could be stored in the same table:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	fname VARCHAR(30) NOT NULL,
	lname VARCHAR(30) NOT NULL,
	street VARCHAR(50) NOT NULL,
	city VARCHAR(50) NOT NULL,
	state CHAR(2) NOT NULL,
	zip CHAR(5) NOT NULL
);
```

but be a nested object in the record type:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			'firstName': {
				valueType: 'string',
				column: 'fname'
			},
			'lastName': {
				valueType: 'string',
				column: 'lname'
			},
			'address': {
				valueType: 'object',
				properties: {
					'street': {
						valueType: 'string'
					},
					'city': {
						valueType: 'string'
					},
					'state': {
						valueType: 'string'
					},
					'zip': {
						valueType: 'string'
					}
				}
			}
		}
	},
	...
}
```

Since the `address` property does not have a `table` attribute, the framework knows that it is stored in the parent table.

In the example above, the `address` property is required. Things turn slightly more complicated if the property needs to be optional. First, the `NOT NULL` constraints are removed from the table:

```sql
CREATE TABLE accounts (
	...
	street VARCHAR(50),
	city VARCHAR(50),
	state CHAR(2),
	zip CHAR(5)
);
```

But now, the framework needs to be able to tell if the `address` nested object is present on a record or not. To do that, the `presenceTest` attribute is specified on the property definition. The presence test is a Boolean expression expressed as a filter specification discussed later in this manual. But here is our example:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'address': {
				valueType: 'object',
				optional: true, // address is optional now
				presenceTest: [
					[ 'state => present' ],
					[ 'zip => present' ]
				],
				properties: {
					'street': {
						valueType: 'string',
						optional: true
					},
					'city': {
						valueType: 'string',
						optional: true
					},
					'state': {
						valueType: 'string'
					},
					'zip': {
						valueType: 'string'
					}
				}
			}
		}
	},
	...
}
```

This definition makes `street` and `city` optional properties, but `state` and `zip` are still required. The presence test checks if `state` and `zip` are present on the record, and if they are not, the whole `address` property is considered to be absent.

### Polymorphic Objects

Polymorphic objects are treated very similarly to nested objects where each polymorphic object subtype is a nested object property on the base polymorphic object and the nested object's properties describe the subtype-specific properties. The table mappings work the same as for scalar nested objects. For example, if we have two types of payment information that can be associated with an account&mdash;credit cards and bank accounts&mdash;the tables could be:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	fname VARCHAR(30) NOT NULL,
	lname VARCHAR(30) NOT NULL
);

CREATE TABLE account_creditcards (
	account_id INTEGER UNSIGNED NOT NULL UNIQUE,
	association VARCHAR(20) NOT NULL,
	last4digits CHAR(4) NOT NULL,
	expdate CHAR(4) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE TABLE account_bankaccounts (
	account_id INTEGER UNSIGNED NOT NULL UNIQUE,
	routing_num CHAR(9) NOT NULL,
	account_type VARCHAR(10) NOT NULL,
	last4digits CHAR(4) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

then the record type definition would be:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'paymentInfo': {
				valueType: 'object',
				typePropertyName: 'type',
				subtypes: {
					'CREDIT_CARD': {
						table: 'account_creditcards',
						parentIdColumn: 'account_id',
						properties: {
							'association': {
								valueType: 'string'
							},
							'last4Digits': {
								valueType: 'string',
								column: 'last4digits'
							},
							'expirationDate': {
								valueType: 'string',
								column: 'expdate'
							}
						}
					},
					'BANK_ACCOUNT': {
						table: 'account_bankaccounts',
						parentIdColumn: 'account_id',
						properties: {
							'routingNumber': {
								valueType: 'string',
								column: 'routing_num'
							},
							'accountType': {
								valueType: 'string',
								column: 'account_type'
							},
							'last4Digits': {
								valueType: 'string',
								column: 'last4digits'
							}
						}
					}
				}
			}
		}
	},
	...
}
```

Properties common for all subtypes could be stored either in the parent table (in the example above the `last4digits` column is moved to the `accounts` table and the `last4Digits` property is moved to the properties of the `Account` record type), or it could be stored in a separate table:

```sql
CREATE TABLE accounts (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	fname VARCHAR(30) NOT NULL,
	lname VARCHAR(30) NOT NULL
);

CREATE TABLE account_paymentinfos (
	account_id INTEGER UNSIGNED NOT NULL UNIQUE,
	last4digits CHAR(4) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE TABLE account_creditcards (
	account_id INTEGER UNSIGNED NOT NULL UNIQUE,
	association VARCHAR(20) NOT NULL,
	expdate CHAR(4) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES account_paymentinfos (id)
);

CREATE TABLE account_bankaccounts (
	account_id INTEGER UNSIGNED NOT NULL UNIQUE,
	routing_num CHAR(9) NOT NULL,
	account_type VARCHAR(10) NOT NULL,
	FOREIGN KEY (account_id) REFERENCES account_paymentinfos (id)
);
```

and the definitions:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'paymentInfo': {
				valueType: 'object',
				typePropertyName: 'type',
				table: 'account_paymentinfos',
				parentIdColumn: 'account_id',
				properties: {
					'last4Digits': {
						valueType: 'string',
						column: 'last4digits'
					}
				},
				subtypes: {
					'CREDIT_CARD': {
						table: 'account_creditcards',
						parentIdColumn: 'account_id',
						properties: {
							'association': {
								valueType: 'string'
							},
							'expirationDate': {
								valueType: 'string',
								column: 'expdate'
							}
						}
					},
					'BANK_ACCOUNT': {
						table: 'account_bankaccounts',
						parentIdColumn: 'account_id',
						properties: {
							'routingNumber': {
								valueType: 'string',
								column: 'routing_num'
							},
							'accountType': {
								valueType: 'string',
								column: 'account_type'
							}
						}
					}
				}
			}
		}
	},
	...
}
```

When the framework builds queries for fetching account records in the examples above, it will join all the subtype tables and see which one has a record. Depending on that, it will figure out the nested object subtype. This logic relies on the database having only a single record of a single subtype for a given polymorphic record. In the second example, it is enforced by having a `UNIQUE` constraint on the `account_id` column of the `account_paymentinfos`. In the first example, however, it is not completely enforced on the database level.

But sometimes, a subtype does not have any subtype-specific properties and therefore thete is no need for a separate table for it. In that case, the framework cannot determine the subtype by joining the tables and simply checking which one exists. To solve it, the type property needs to be actually stored in a column on the base polymorphic object's table. For example, if we have a polymorphic `Event` record type. Subtype _OPENED_ has a subtype-specific property `openerName`, which, for the example sake, is stored in a separate table. Subtype _CLOSED_, however, does not have any specific properties. The table are:

```sql
CREATE TABLE events (
	id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	type VARCHAR(10) NOT NULL,
	happened_on TIMESTAMP
);

CREATE TABLE events_open (
	id INTEGER UNSIGNED NOT NULL UNIQUE,
	opener_name VARCHAR(50) NOT NULL,
	FOREIGN KEY (id) REFERENCES events (id)
);
```

The `type` column of the `events` table is used to store the event type (either "OPENED" or "CLOSED"). The definition should then be:

```javascript
{
	...
	'Event': {
		table: 'events',
		typePropertyName: 'type',
		typeColumn: 'type',
		properties: {
			'happenedOn': {
				valueType: 'datetime',
				column: 'happened_on'
			}
		},
		subtypes: {
			'OPENED': {
				table: 'events_open',
				parentIdColumn: 'id',
				properties: {
					'openerName': {
						valueType: 'string',
						column: 'opener_name'
					}
				}
			},
			'CLOSED': {
				properties: {}
			}
		}
	},
	...
}
```

Note the `typeColumn` attribute that tells the framework where to find/store the record subtype.

### Ordered Collections

When collection properties are fetched from the database, it is possible to specify the specific order in which they are returned. For that, any collection property definition can have a `order` attribute. For example:

```javascript
{
	...
	'Order': {
		table: 'orders',
		properties: {
			...
			'items': {
				valueType: 'object[]',
				table: 'order_items',
				parentIdColumn: 'order_id',
				order: [ 'productRef.name', 'quantity => desc' ],
				properties: {
					'id': {
						valueType: 'number',
						role: 'id'
					},
					'productRef': {
						valueType: 'ref(Product)',
						column: 'product_id',
						modifiable: false
					},
					'quantity': {
						valueType: 'number'
					}
				}
			}
		}
	},
	...
}
```

Or:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'orderRefs': {
				valueType: 'ref(Order)[]',
				reverseRefProperty: 'accountRef',
				order: [ 'placedOn => desc' ]
			}
		}
	},
	...
}
```

The order specification is described in detail later in this manual when we talk about the fetch DBO.

Note, that to order simple value collections by value, `$value` pseudo-property reference can be used.

### Filtered Collection Views

It is also possible to specify filtered views of collection properties that include only those elements that match a certain criteria. For example:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			...
			'orderRefs': {
				valueType: 'ref(Order)[]',
				reverseRefProperty: 'accountRef'
			},
			'pendingOrderRefs': {
				viewOf: 'orderRefs',
				filter: [
					[ 'status => is', 'PENDING' ]
				]
			}
		}
	},
	...
}
```

Note that the `filter` attribute can be specified only on a view property, not on the actual property itself, which always includes all of its elements.

The filter specification is described in detail later in this manual when we talk about the fetch DBO.

### Record Meta-Info Properties

The DBOs module introduces a number special _meta-info properties_ that can be specified on a record type and are maintained automatically by the framework. Such properties use `role` property definition attribute to specify their type. For example:

```javascript
{
	...
	'Account': {
		table: 'accounts',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			'version': {
				valueType: 'number',
				role: 'version'
			},
			'createdOn': {
				valueType: 'datetime',
				role: 'creationTimestamp',
				column: 'created_on'
			},
			'createdBy': {
				valueType: 'string',
				role: 'creationActor',
				column: 'created_by'
			},
			'modifiedOn': {
				valueType: 'datetime',
				role: 'modificationTimestamp',
				column: 'modified_on'
			},
			'modifiedBy': {
				valueType: 'string',
				role: 'modificationActor',
				column: 'modified_by'
			},
			...
		}
	},
	...
}
```

The following meta-info property roles are supported:

* `version` - Record version. A new record will have version 1. Every time a record is updated, the framework will increment the version property.

* `creationTimestamp` - Timestamp when the record was created.

* `creationActor` - Stamp of the actor that created the record (see [x2node-common](https://www.npmjs.com/package/x2node-common) module).

* `modificationTimestamp` - Timestamp when the record was last time updated. The property is by default optional.

* `modificationActor` - Stamp of the actor that modified the record last time. The property is by default optional.

All meta-info properties are marked as non-modifiable and for the purpose of the applications they are read only.

### Super-Properties

Sometimes it is necessary to define properties that are not specific to a given record, but apply to the collection of records of a certain record type that match a criteria. This is mostly useful for aggregate properties, such as the total count of matched records, or a sum of all order amounts, etc. Such properties are called _super-properties_ or, if they are aggregates, _super-aggregates_. Every record type automatically defines a super-aggregate called `count`, which reflects the number of records in the select set. It is possible to add custom super-aggregates as well. For example, if we want to be able to query the total amount for orders matching a certain criteria (or all orders available in the database, if no criteria), and the number of pending orders, corresponding super-aggregates can be defined like this:

```javascript
{
	...
	'Order': {
		table: 'orders',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			...
			'status': {
				valueType: 'string'
			},
			...
			'items': {
				valueType: 'object[]',
				table: 'order_items',
				parentIdColumn: 'order_id',
				properties: {
					'id': {
						valueType: 'number',
						role: 'id'
					},
					'productRef': {
						valueType: 'ref(Product)',
						column: 'product_id',
						modifiable: false
					},
					'quantity': {
						valueType: 'number'
					}
				}
			}
		},
		superProperties: {
			'countPending': {
				valueType: 'number',
				aggregate: {
					collection: 'records',
					valueExpr: 'id => count',
					filter: [
						[ 'status => is', 'PENDING' ]
					]
				}
			},
			'totalAmount': {
				valueType: 'number',
				aggregate: {
					collection: 'records.items',
					valueExpr: 'productRef.price * quantity => sum'
				}
			}
		}
	},
	...
}
```

Note that the records collection is referred in the super-aggregate expressions as `records`.

## Fetch DBO

The fetch DBO is used to search records of a given record type and fetch the requested record data. The DBO is created using DBO factory's `buildFetch()` method, which takes the record type name and the query specification:

```javascript
const fetchDBO = dboFactory.buildFetch('Order', {
	// the query specification goes here
});
```

Once built, the DBO can be used and reused multiple times, which may be helpful as, depending on the complexity of the query specification, the DBO construction may be a relatively costly operation.

To execute the DBO, its `execute()` method is called, which takes up to three arguments:

* `txOrCon` - Database connection (or transaction object, described later in this manual).

* `actor` - Optional actor performing the operation. If not provided, the operation is anonymous.

* `filterParams` - If filter specification used to construct the DBO utilizes query parameters, this object provides the values for the parameters. Using query parameters in filter specifications helps making DBOs reusable.

The `execute()` method returns a `Promise`, which is fulfilled with the query result object. The query result object includes:

* `records` array of matched records, or empty array if none matched.

* `referredRecords`, which is included if any referred records were requested to be fetched along with the main record set. The keys in the object are record references and the values are the records.

* any requested super-properties.

The promise is rejected if an error occurs.

The query specification consists of up to four sections: specification of what record properties, referred records and super-properties to include in the result, the filter specification that asks the DBO to include only those records that match the criteria, the order specification, which tells the DBO how to order the records in the result's `records` array, and the range specification that tells the DBO to return only the specified subrange of all the matched records. If no query specification is provided during the DBO construction, all records of the type are fetched with all the properties that are fetched by default (normally that includes all stored properties) and in no particular order.

### Selected Properties Specification

By default, the fetch DBO will fetch all stored properties of the matched records. Those include all properties that are not views, not calculated, not aggregates and not dependent record references. This default behavior can be overridden using `fetchByDefault` attribute on the property definition, which can be either `true` or `false`, but generally is not recommended. Instead, to select only specific properties, the query specification can include a `props` property that is an array of property pattern strings. Each pattern can be:

* A star "*" to include all properties fetched by default.

* Specific property path in dot notation. All intermediate properties are automatically fetched. If any of the intermediate properties is a reference, the referred record is fetched and added to the `referredRecords` collection in the result object.

* A wildcard pattern, which is a property path in dot notation ending with ".*". This instructs the DBO to fetch the property and all of its children that would be fetched by default. In particular, if the property path is for a reference property, this allows fetching referred records and returning them in the `referredRecords` collection in the result object with all of their default properties.

* Specific property path in dot notation prefixed with a dash "-" to exclude a property that would otherwise be included because of a wildcard pattern.

* A super-property name prefixed with a dot "." to include the super-property.

### Filter Specification

By default, all records of the requested record type are matched. To restrict the matched records set, a `filter` property is included in the query specification. The filter property is an array of filter terms. Each term is itself an array and can specify either a test to perform on a record, or a logical junction of nested filters.

TODO

### Order Specification

TODO

### Range Specification

TODO

## Insert DBO

TODO

## Update DBO

TODO

## Delete DBO

TODO

## Transactions

TODO

## Data Sources

TODO

## Database Drivers

TODO
