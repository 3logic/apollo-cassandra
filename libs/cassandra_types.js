var check = require('check-types'),
    build_error = require('./apollo_error.js');

var TYPE_MAP = {validators:{}};

TYPE_MAP.validators.is_integer = function (obj){
    return check.intNumber(obj);
};

TYPE_MAP.validators.is_boolean = function (obj){
    return obj === true || obj === false;
};

TYPE_MAP.validators.is_number = function (obj){
    return check.number(obj);
};

TYPE_MAP.validators.is_string = function (obj){
    return check.string(obj);
};    

TYPE_MAP.validators.is_datetime = function (obj){
    return check.date(new Date(obj));
};

TYPE_MAP.validators.is_anything = function (obj){
    return true;
};

TYPE_MAP.validators.is_uuid = function (obj){
    if(!check.string(obj))
        return false;

    //var pattern_uuid4 = /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i;
    var pattern_uuid1 = /^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$/i;
    return obj.test(pattern_uuid1);
};

TYPE_MAP.validators.is_inet = function (obj){
    if(!check.string(obj))
        return false;

    //var pattern_uuid4 = /^[0-9A-F]{8}-[0-9A-F]{4}-4[0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$/i;
    var patt_ip4 = /^(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$/i,
        patt_ip6_1 = /^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$/i,
        patt_ip6_2 = /^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$/i;
    return obj.test(patt_ip4) || obj.test(patt_ip6_1) || obj.test(patt_ip6_2) ;
};


TYPE_MAP = {    
    "ascii"     : {validator : TYPE_MAP.validators.is_string,   dbvalidator : "org.apache.cassandra.db.marshal.AsciiType"},    
    "bigint"    : {validator : TYPE_MAP.validators.is_integer,  dbvalidator : "org.apache.cassandra.db.marshal.LongType"},
    "blob"      : {validator : TYPE_MAP.validators.is_anything, dbvalidator : "org.apache.cassandra.db.marshal.BytesType"},
    "boolean"   : {validator : TYPE_MAP.validators.is_boolean,  dbvalidator : "org.apache.cassandra.db.marshal.BooleanType"},        
    "counter"   : {validator : TYPE_MAP.validators.is_integer,  dbvalidator : "org.apache.cassandra.db.marshal.CounterColumnType"},
    "decimal"   : {validator : TYPE_MAP.validators.is_number,   dbvalidator : "org.apache.cassandra.db.marshal.DecimalType"},        
    "double"    : {validator : TYPE_MAP.validators.is_number,   dbvalidator : "org.apache.cassandra.db.marshal.DoubleType"},
    "float"     : {validator : TYPE_MAP.validators.is_number,   dbvalidator : "org.apache.cassandra.db.marshal.FloatType"},
    "inet"      : {validator : TYPE_MAP.validators.is_inet,     dbvalidator : "org.apache.cassandra.db.marshal.InetAddressType"},
    "int"       : {validator : TYPE_MAP.validators.is_integer,  dbvalidator : "org.apache.cassandra.db.marshal.Int32Type"},
    "text"      : {validator : TYPE_MAP.validators.is_string,   dbvalidator : "org.apache.cassandra.db.marshal.UTF8Type"},
    "timestamp" : {validator : TYPE_MAP.validators.is_datetime, dbvalidator : "org.apache.cassandra.db.marshal.TimestampType"},
    "timeuuid"  : {validator : TYPE_MAP.validators.is_uuid,     dbvalidator : "org.apache.cassandra.db.marshal.TimeUUIDType"},
    "uuid"      : {validator : TYPE_MAP.validators.is_uuid,     dbvalidator : "org.apache.cassandra.db.marshal.UUIDType"},
    "varchar"   : {validator : TYPE_MAP.validators.is_string,   dbvalidator : "org.apache.cassandra.db.marshal.UTF8Type"},    
    "varint"    : {validator : TYPE_MAP.validators.is_integer,  dbvalidator : "org.apache.cassandra.db.marshal.IntegerType"}
};

TYPE_MAP.generic_type_validator = function(validator){
    return {
        validator   : validator,
        message     : function( value, prop_name, fieldtype){
           return build_error('model.set.invalidvalue',value,prop_name,fieldtype)
        }
    }
};

TYPE_MAP.find_type_by_dbvalidator = function(val){
    for(var t in this){            
        if (this[t].dbvalidator == val)
            return t;
    }
    return null;
};

module.exports = TYPE_MAP;