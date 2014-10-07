var util = require('util'),
    LoadBalancingPolicy = require('cassandra-driver').policies.loadBalancing.LoadBalancingPolicy;

var SingleNodePolicy = function(){
    LoadBalancingPolicy.apply(this,Array.prototype.slice.call(arguments));
    this.name = 'single_node_policy';
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
    var self = this,
        hosts = this.hosts.slice(0);
    var next_iterator = function(){
            return { value: hosts[0], done: false};
        };
    
    callback( null, { 'next': next_iterator } );
};

module.exports = SingleNodePolicy;
