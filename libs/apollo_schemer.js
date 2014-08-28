var TYPE_MAP = require('./cassandra_types');

/*
    {
        fields : { //obbligatorio
            column1 : "tipo",
            column2 : "tipo2",
            column3 : "tipo3"
        },
        key : ["column1","column2"],
        indexes : ["column1","column3"] 
    }
*/


var schemer = {

    normalize_model_schema: function(model_schema){
        if(typeof model_schema.key[0] === 'string'){
            model_schema.key[0] = [model_schema.key[0]];
        }

        if(model_schema.indexes){
            var index_sort = function(a,b){
                return a > b ? 1 : (a < b ? -1 : 0);
            };

            model_schema.indexes.sort(index_sort);
        }
    },

    validate_model_schema: function(model_schema){
        if(!model_schema)
            throw("A schema must be specified");

        if(typeof(model_schema.fields) != "object" || Object.keys(model_schema.fields).length === 0 )
            throw('Schema must contain a non-empty "fields" map object');
        if(!model_schema.key || !(model_schema.key instanceof Array))
            throw('Schema must contain "key" in the form: [ [partitionkey1, ...], clusteringkey1, ...]');

        for( var k in model_schema.fields){
            if (!(model_schema.fields[k] in TYPE_MAP))
                throw("Schema Field type unknown for: " + k);
        }

        if( typeof(model_schema.key[0]) == "string" ){
            if(!(model_schema.key[0] in model_schema.fields)) 
                throw("Partition Key as string must match a column name");
        }
        else if(model_schema.key[0] instanceof Array){
            for(var j in model_schema.key[0]){
                if((typeof(model_schema.key[0][j]) != "string") || !(model_schema.key[0][j] in model_schema.fields))
                        throw("Partition Key array must contain only column names");
            }
        }
        else {
            throw("Partition Key must be a column name string, or array of");
        }
        
        for(var i in model_schema.key){
            if(i>0){
                if((typeof(model_schema.key[i]) != "string") || !(model_schema.key[i] in model_schema.fields))
                    throw("Clustering Keys must match column names");
            }
        }

        if(model_schema.indexes){
            if(!(model_schema.indexes instanceof Array))
                throw("Indexes must be an array of column name strings");
            for(var l in model_schema.indexes)
                if((typeof(model_schema.indexes[l]) != "string") || !(model_schema.indexes[l] in model_schema.fields))
                    throw("Indexes must be an array of column name strings");
        }
    }
};

module.exports = schemer;