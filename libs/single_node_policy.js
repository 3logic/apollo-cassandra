var util = require('util'),
    LoadBalancingPolicy = require('cassandra-driver').policies.loadBalancing.LoadBalancingPolicy;

var SingleNodePolicy = function( default_connection_index ){
    LoadBalancingPolicy.apply(this,Array.prototype.slice.call(arguments));
    this._default_connection_index = default_connection_index || 0;
};

util.inherits(SingleNodePolicy, LoadBalancingPolicy);

/**
 * @param {String} keyspace Name of the keyspace
 * @param queryOptions options evaluated for this execution
 * @param {Function} callback
 */
SingleNodePolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
    if (!this.hosts) {
        callback(new Error('Load balancing policy not initialized'));
    }
    var conn_index = this._default_connection_index,
        hosts = this.hosts.slice(0),
        counter = 0;
    var next_iterator = function(){
        return ++counter > 1 ? {done: true} : { value: hosts[conn_index], done: false};
    };
    
    callback( null, { 'next': next_iterator } );
};

module.exports = SingleNodePolicy;
