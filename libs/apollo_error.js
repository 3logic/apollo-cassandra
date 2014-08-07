var util = require('util');

var AERROR_TYPES = {
	'default': {
		msg: "Unspecified Apollo error ->"
	},
	'model.tablecreation.dbschemaquery': {
		msg: "Error while retrieveing Schema of DB Table -> %s"
	},
	'model.tablecreation.schemamismatch': {
		msg: "Given Schema does not match existing DB Table -> %s"
	},
	'model.tablecreation.dbdrop': {
		msg: "Error during drop of DB Table -> %s"
	},
	'model.tablecreation.dbcreate': {
		msg: "Error during creation of DB Table -> %s"
	},
	'model.tablecreation.dbindex': {
		msg: "Error during creation of index on DB Table -> %s"
	},
	'model.save.unsetkey': {
		msg: "Key Field: %s must be set"
	},
	'model.save.invalidvalue': {
		msg: "Invalid Value: \"%s\" for Field: %s (Type: %s)"
	}
}

var ERR_NAME_PREFIX = 'apollo';

/**
 * Builds a standardized Error object
 * 
 * @param {String} error type;
 * @varargs {String} parameters to fill in the error message template
 * @return {Apollo~Error} the built error object
 */
var build_error = function(){
	var argsarray = Array.prototype.slice.call(arguments);
	var name = argsarray.length ? argsarray.shift() : '_none_given_';
	
	var error_template = AERROR_TYPES[name] || AERROR_TYPES.default,
		error_msg;
	
	error_msg = argsarray.length ? 
		util.format.apply(this,[error_template.msg].concat(argsarray)) : 
		error_template.msg;

	var error = new Error(error_msg);
	error.name = ERR_NAME_PREFIX+'.'+name;
	
	return error;
}

 /**
  * Apollo standard Error object
  * 
  * @typedef {Object} Apollo~Error
  * @property {String} type - error type, as enumerated in AERROR_TYPES
  * @property {String} msg  - error message (with replaced parameters if any)
  */

module.exports = build_error