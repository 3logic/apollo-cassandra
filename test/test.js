var chai = require('chai');
var assert = chai.assert;
var async = require('async');
var Apollo = require(__dirname +'/../libs/apollo');


var connection;
switch(process.env.TRAVIS){
    case 'true':
        connection = {
            "hosts": [
                "127.0.0.1"
            ],
            "keyspace": "tests"
        };
        break;
    default:
        connection = {
            "hosts": [
                "192.168.100.61",
                "192.168.100.62"
            ],
            "keyspace": "tests"
        };
        break;
}

describe('Apollo > ', function(){
    
    describe('Global library', function(){

        var apollo;

        describe('New Apollo > ', function(){
            
            it('is a valid instance', function(){
                apollo = new Apollo(connection);
                assert.instanceOf(apollo, Apollo, 'apollo is an instance of Apollo');
                assert.isFunction(apollo.connect, 'connect is a function of apollo');
            });

            it('connect to cassandra', function(done){
                apollo.connect(done);
            });
        });
    });

    
    describe('On apollo instances > ',function(){

        var ap;

        beforeEach(function(done) {
            if(ap)
                ap.close();

            ap = new Apollo(connection);

            // Setup
            ap.connect(function(err){      
                if(err) return done(err);      
                var BaseModel = ap.add_model("test1", model_test1);
                BaseModel._drop_table(done);
            });
        });

        var model_test1 = { 
                fields:{v1:"int",v2:"int",v3:"int"}, 
                key:["v1"],
                indexes : ["v2"]
            };

        var model_test2 = { 
                fields:{v1:"int",v2:"int",v3:"text"}, 
                key:["v1"],
                indexes : ["v2"]
            };

        var model_test3 = { 
                fields:{v1:"int",v2:"int",v3:"int"}, 
                key:["v1"],
                indexes : ["v3"]
            };

        var faulty_model_test1 = { 
                fields:{v1:"int",v2:"int",v3:"foo"}, 
                key:["v1"],
                indexes : ["v2"]
            };

        it('add model', function(){
            var TestModel = ap.add_model("test1", model_test1);
            assert.isFunction(TestModel);
            assert.property(TestModel,'find');
            assert.isFalse(TestModel.is_table_ready());
        });


        it('add faulty model (silly type)', function(){
            assert.throws(function(){
                var TestModel = ap.add_model("test1", faulty_model_test1);
            });
        });

        it('instance model', function(){
            var TestModel = ap.add_model("test1", model_test1);
            var ins = new TestModel({'v1': 500});

            assert.propertyVal(ins,'v1',500);
            assert.notProperty(ins,'v2');
            assert.property(ins,'save');
        });

        it('Init works even if not already connected', function(done){
            var ap2 =  new Apollo(connection);
            var Model = ap2.add_model("test1", model_test1);
            Model.find( {'v1' : 1}, done);            
        });

        it('Find works if not already connected', function(done){
            var ap2 =  new Apollo(connection);
            var FindModel = ap2.add_model("test1", model_test1);
            FindModel.find( {'v1' : 1}, done);
        });

         
        describe('Schema conflicts on init > ',function(){
            
            beforeEach(function(done) {
                var BaseModel = ap.add_model("test1", model_test1);
                BaseModel._drop_table(function(err){
                    if(err) return done(err);
                    BaseModel.init(done);
                });
            });

            var conflict_model = model_test3;
            
            it('mismatch_behaviour:default(fail)', function(done){
                var TestModel = ap.add_model("test1", conflict_model);

                TestModel.init(function(err,result){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.tablecreation.schemamismatch');                      
                    done();
                });
               
            });
            
            it('mismatch_behaviour:fail', function(done){
                var TestModel = ap.add_model("test1", conflict_model,{mismatch_behaviour:"fail"});
                TestModel.init(function(err,result){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.tablecreation.schemamismatch');                    
                    done();
                });
               
            });
            
            it('mismatch_behaviour:drop', function(done){
                var TestModel = ap.add_model("test1", conflict_model,{mismatch_behaviour:"drop"});
                TestModel.init(function(err,result){
                    assert.notOk(err);                    
                    done();
                });
               
            });
            
            it('mismatch_behaviour invalid', function(){
                assert.throw(
                    function(){ 
                        var TestModel = ap.add_model("test1", conflict_model,{mismatch_behaviour:"foo"});
                    }, /mismatch_behaviour.+foo/ 
                );
            });

            it('same name, same schema', function(done){
                var TestModel = ap.add_model("test1", model_test1);
                TestModel.init(function(err,result){
                    assert.notOk(err);
                    done();
                });
            });

        });


        
        describe('Save > ',function(){
            var TestModel;

            beforeEach(function(done) {
                TestModel = ap.add_model("test1", model_test1);
                TestModel.init(done);
            });

            it('successful basic save', function(done){
                var ins = new TestModel({'v1': 500});
                ins.save(function(err,result){
                    assert.notOk(err);
                    done();
                });
            });

            it('failing basic save (unset key)', function(done){
                var ins = new TestModel({'v2': 42});
                ins.save(function(err,result){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.save.unsetkey');
                    done();
                });
            });

            it('failing basic save (wrong type)', function(done){
                var ins = new TestModel({'v1': 500, 'v2': 'foo'});
                ins.save(function(err,result){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.save.invalidvalue');
                    done();
                });
            });

        });

        describe('Find > ',function(){
            var TestModel;

            var model_find_schema = { 
                fields:{v1:"int",v2:"text",v3:"int",v4:"text",v5:"boolean",v6:'int'}, 
                key:[["v1","v2"],"v3", "v6"],
                indexes:["v4"] 
            };

            beforeEach(function(done) {
                this.timeout(15000);

                TestModel = ap.add_model("test_find", model_find_schema, {'mismatch_behaviour':'drop'});
                //TestModel._drop_table(done);
                
                TestModel.init(function(err,result){

                    if(err) return done(err);
                    var ins = new TestModel();
                    async.series([
                        function(callback){
                            ins.v1 = 1;
                            ins.v2 = 'two';
                            ins.v3 = 3;
                            ins.v4 = 'foo';
                            ins.v5 = true;
                            ins.v6 = 4;
                            ins.save(callback);
                        },
                        function(callback){
                            ins.v1 = 11;
                            ins.v2 = 'twelve';
                            ins.v3 = 13;
                            ins.v4 = 'baz';
                            ins.v5 = true;
                            ins.v6 = 14;
                            ins.save(callback);
                        },
                        function(callback){
                            ins.v1 = 21;
                            ins.v2 = 'twentytwo';
                            ins.v3 = 23;
                            ins.v4 = 'bar';
                            ins.v5 = false;
                            ins.v6 = 24;
                            ins.save(callback);
                        }

                    ],done);
                });
                
            });

            it('basic find', function(done){
                TestModel.find({'v1':1, 'v4':'foo', 'v5':true},function(err, results){
                    assert.lengthOf(results, 1);
                    var result = results[0];
                    assert.instanceOf(result, TestModel);
                    assert.deepEqual(result.v1, 1);
                    assert.deepEqual(result.v2, 'two');
                    assert.deepEqual(result.v3, 3);
                    assert.deepEqual(result.v4, 'foo');
                    assert.deepEqual(result.v5, true);
                    done();
                });
            });

            it('basic find with raw results', function(done){
                TestModel.find({'v1':1, 'v4':'foo', 'v5':true},{ raw: true },function(err, results){
                    assert.lengthOf(results, 1);
                    var result = results[0];
                    assert.notInstanceOf(result, TestModel);
                    assert.deepEqual(result.v1, 1);
                    assert.deepEqual(result.v2, 'two');
                    assert.deepEqual(result.v3, 3);
                    assert.deepEqual(result.v4, 'foo');
                    assert.deepEqual(result.v5, true);
                    done();
                });
            });

            it('using $in in last primary key', function(done){
                TestModel.find({'v1':11, 'v2':{'$in':['twelve','twentytwo']}, 'v3':13},function(err, results){
                    assert.lengthOf(results, 1);
                    var result = results[0];
                    assert.instanceOf(result, TestModel);
                    assert.deepEqual(result.v1, 11);
                    assert.deepEqual(result.v2, 'twelve');
                    assert.deepEqual(result.v3, 13);
                    assert.deepEqual(result.v4, 'baz');
                    assert.deepEqual(result.v5, true);
                    done();
                });
            });

            it('using >= ($gte) in clustering key', function(done){
                TestModel.find({'v3':{'$gte':1 } },function(err, results){
                    assert.lengthOf(results, 3);
                    done();
                });
            });

            it('providing no query', function(done){
                TestModel.find({},function(err, results){
                    assert.lengthOf(results, 3);
                    done();
                });
            });

            it('faulty find (unknown op)', function(done){
                TestModel.find({'v1':[{'$eq':1},{'$neq':2}]},function(err){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.find.invalidop');
                    done();
                });
            });
            it('faulty find (several ops)', function(done){
                TestModel.find({'v1':[{'$eq':1, 'foo':'bar'}]},function(err){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.find.multiop');
                    done();
                });
            });
            it('faulty find (unknown relation type)', function(done){
                TestModel.find({'v1': function(){}},function(err){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.find.invalidrelob');
                    done();
                });
            });
            it('faulty find (invalid limit type)', function(done){
                TestModel.find({'v1': 1, '$limit':'foo'},function(err){
                    assert.ok(err);
                    assert.propertyVal(err,'name','apollo.model.find.limittype');
                    done();
                });
            });

            it('faulty find (querying "$in" on non index property)', function(done){
                TestModel.find({'v1':{'$gt':1}, 'v4':{'$in':['foo','bar']}, 'v5':true},function(err){
                    assert.ok(err);
                    done();
                });
            });

            it('ordering by a clustering key ($orderby)', function(done){
                TestModel.find({'v1':11, 'v2':{'$in':['twelve','twentytwo']}, '$orderby':{'$desc' :'v3'} },function(err, results){
                    assert.notOk(err);
                    assert.lengthOf(results, 1);
                    done();
                });
            });

            it('ordering by a clustering key failing using unallowed order clause ($orderby)', function(done){
                TestModel.find({'v1':11, 'v2':{'$in':['twelve','twentytwo']}, '$orderby':{'DESC' :'v3'} },function(err, results){
                    assert.ok(err);
                    done();
                });
            });

            it('ordering by all clustering key ($orderby)', function(done){
                TestModel.find({'v1':11, 'v2':{'$in':['twelve','twentytwo']}, '$orderby':{'$asc': ['v3','v6']} },function(err, results){
                    assert.notOk(err);
                    assert.lengthOf(results, 1);
                    done();
                });
            });

            it('ordering by all clustering key fails if order is not definition order ($orderby)', function(done){
                TestModel.find({'v1':11, 'v2':{'$in':['twelve','twentytwo']}, '$orderby':{'$asc': ['v6','v3']} },function(err, results){
                    assert.ok(err);
                    done();
                });
            });

        });

        describe('Delete > ',function(){
            var TestModel;

            beforeEach(function(done) {
                TestModel = ap.add_model("test1", model_test1);
                TestModel.init(function(){
                    var ins = new TestModel({'v1': 500});
                    ins.save(done);
                });
            });

            it('successful static delete', function(done){
                TestModel.delete({'v1': 500}, function(err){
                    assert.notOk(err);
                    TestModel.find({'v1': 500}, function(err, results){
                        assert.notOk(err);
                        assert.lengthOf(results, 0);
                        done();
                    });
                });
            });

            it('successful instance delete', function(done){
                var ins = new TestModel({'v1': 500});
                ins.delete(function(err){
                    assert.notOk(err);
                    TestModel.find({'v1': 500}, function(err, results){
                        assert.notOk(err);
                        assert.lengthOf(results, 0);
                        done();
                    });
                });
            });

        });


        it.skip('pig update', function(done){
            ap.add_model("test1", model_test1, true, function(err,data){
                ap.pig_cql_update_connection("test1",true, function(err,data){
                    if(err) 
                        console.log('err: '+err);
                    else 
                        console.log(data);
                    done();
                });
            });
        });


    });

});

