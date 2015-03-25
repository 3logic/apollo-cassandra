var chai = require('chai');
var assert = chai.assert;
var async = require('async');
var Apollo = require(__dirname +'/../libs/apollo');


var connection;
switch(process.env.TRAVIS){
    case 'true':
        connection = {
            "contactPoints": [
                "127.0.0.1"
            ],
            "keyspace": "tests"
        };
        break;
    default:
        try{
            connection = require('./local_conf.json');
        }catch(e){
            throw "Missing local_conf.json in test directory";
        }
        break;
}

describe('Apollo > ', function(){

    this.timeout(10000);

    describe('Global library', function(){

        var apollo;

        describe('New Apollo > ', function(){

            it('is a valid instance', function(){
                apollo = new Apollo(connection);
                assert.instanceOf(apollo, Apollo, 'apollo is an instance of Apollo');
                assert.isFunction(apollo.connect, 'connect is a function of apollo');
            });

            it('connects to cassandra', function(done){
                apollo.connect(done);
                apollo.close();
            });
        });
    });


    describe('On Apollo instances > ',function(){

        var ap;

        beforeEach(function(done) {
            var on_old_client_closed = function(){
                ap = new Apollo(connection);
                // Setup
                ap.connect(function(err){
                    if(err) return done(err);
                    var BaseModel = ap.add_model("test1", model_test1);
                    BaseModel.drop_table(done);
                });
            };


            if(ap)
                ap.close(on_old_client_closed);
            else{
                on_old_client_closed();
            }

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

        var model_testvirtual1 = {
                fields:{
                    name:"text",
                    surname:"text",
                    'complete_name':{'type':'text', virtual:{'get': function(){return this.name+' '+this.surname} } }
                },
                key:[["name", "surname"]]
            };

        var model_testvirtual2 = {
                fields:{
                    name:"text",
                    surname:"text",
                    'complete_name':{'type':'text', virtual:{'set': function(v){var parts = v.split(' '); this.name = parts[0]; this.surname = parts[parts.length -1]; } } }
                },
                key:[["name", "surname"]]
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

        it('add faulty model (wrong default type)', function(){
            var schema = {
                fields:{
                    v1:{
                        type: "int",
                        "default": {'im':'a wrong object'}
                    }
                }
            };
            assert.throws(function(){
                var TestModel = ap.add_model("test1", schema );
            });
        });

        it('instance model', function(){
            var TestModel = ap.add_model("test1", model_test1);
            var ins = new TestModel({'v1': 500});

            assert.propertyVal(ins,'v1',500);
            assert.notProperty(ins,'v2');
            assert.property(ins,'save');
        });

        it('init works even if not already connected', function(done){
            var ap2 =  new Apollo(connection);
            var Model = ap2.add_model("test1", model_test1);
            Model.find( {'v1' : 1}, done);
        });

        it('find works if not already connected', function(done){
            var ap2 =  new Apollo(connection);
            var FindModel = ap2.add_model("test1", model_test1);
            FindModel.find( {'v1' : 1}, done);
        });

        it('virtual field getter', function(){
            var TestModel = ap.add_model("testvirtual1", model_testvirtual1);
            var ins = new TestModel({'name': 'foo', 'surname':'baz', complete_name:'bar'});

            assert.propertyVal(ins,'name','foo');
            assert.propertyVal(ins,'surname', 'baz');
            assert.propertyVal(ins,'complete_name', 'foo baz');
        });

        it('virtual field setter', function(){
            var TestModel = ap.add_model("testvirtual2", model_testvirtual2);
            var ins = new TestModel({'name': 'a', 'surname':'b', 'complete_name':'foo bar baz'});

            assert.propertyVal(ins,'name','foo');
            assert.propertyVal(ins,'surname', 'baz');
            assert.notOk(ins.complete_name);
        });

        describe('Validation >', function(){

            it('field validation', function(){
                var TestModel = ap.add_model("test1", model_test1);

                assert.throws(function(){
                    var ins = new TestModel({'v1' : 'a'});
                },'Invalid Value: "a" for Field: v1 (Type: int)');
            });


            it('field custom validator', function(){
                var custom_validation_schema = {
                    fields:{v1:"int",v2:"int",v3:{'type':"int",'rule': function(v){ return v > 10; } } },
                    key:["v1"],
                    indexes : ["v2"]
                };
                var TestModel = ap.add_model("testcustom1", custom_validation_schema);
                assert.throws(function(){
                    var ins = new TestModel({'v3' : 5});
                },'Invalid Value: "5" for Field: v3 (Type: int)');
            });

            it('field custom validator with custom message', function(){
                var custom_validation_schema = {
                    fields:{
                        v1:"int",
                        v2:"int",
                        v3:{
                            'type':"int",
                            "rule": {
                                validator: function(v){ return v > 10; },
                                message : 'V3 must be greater than 10'
                            }
                        }
                    },
                    key:["v1"],
                    indexes : ["v2"]
                };
                var TestModel = ap.add_model("testcustom2", custom_validation_schema);
                assert.throws(function(){
                    var ins = new TestModel({'v3' : 5});
                },'V3 must be greater than 10');
            });

            it('field custom validator with custom generated message', function(){
                var custom_validation_schema = {
                    fields:{
                        v1:"int",
                        v2:"int",
                        v3:{
                            'type':"int",
                            "rule": {
                                validator: function(v){ return v > 10; },
                                message : function(value){
                                    return 'v3 must be greater than 10, ' + value + ' is not';
                                }
                            }
                        }
                    },
                    key:["v1"],
                    indexes : ["v2"]
                };
                var TestModel = ap.add_model("testcustom", custom_validation_schema);
                var val = 5;
                assert.throws(function(){
                    var ins = new TestModel({'v3' : val});
                },'v3 must be greater than 10, ' + val + ' is not');
            });

            it('default validator is called after custom validator', function(){
                var custom_validation_schema = {
                    fields:{v1:"int",v2:"int",v3:{'type':"int",'rule': function(v){ return v != 10; } } },
                    key:["v1"],
                    indexes : ["v2"]
                };
                var TestModel = ap.add_model("testcustom", custom_validation_schema);
                assert.throws(function(){
                    var ins = new TestModel({'v3' : 'a'});
                },'Invalid Value: "a" for Field: v3 (Type: int)');
            });

        });


        describe('Schema operations > ',function(){

            beforeEach(function(done) {
                var BaseModel = ap.add_model("test1", model_test1, {mismatch_behaviour: 'drop'});
                BaseModel.drop_table(function(err){
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

            it('invalid mismatch_behaviour', function(){
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

            it('validator is ignored in schema comparison', function(done){

                var user_schema = {
                    fields:{
                        name: 'text',
                        email: {
                            type: 'text',
                            rule: {
                                validator: function(email){ return email.length>0; },
                                message: 'Email cannot be blank'
                            }
                        }
                    },
                    key: ["name"]
                };

                var TestModel = ap.add_model("newtest", user_schema);

                var ins, ins2;
                assert.doesNotThrow(function(){
                    ins = new TestModel({name:'testname', email:'a@b.c'});
                });

                ins.save(function(err){
                    TestModel.find({name:'testname'}, function(err,list){
                        //console.log(list.map(function(v){return v.name;}));
                        assert.doesNotThrow(function(){
                            ins2 = new TestModel({name:'testname2', email:'a@b.c'});
                        });
                        assert.notOk(err);
                        done();
                    });
                });

            });

        });


        describe('Save > ',function(){
            var TestModel;

            beforeEach(function(done) {
                TestModel = ap.add_model("test1", model_test1, {mismatch_behaviour:"drop"});
                TestModel.init(done);
            });

            it('successful basic save', function(done){
                var ins = new TestModel({'v1': 500});
                ins.save(function(err,result){
                    if(err){
                        console.log(err);
                    }
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


            it('successful save with default fields (value)', function(done){
                var model_test_def = {
                    fields:{v1:"int",v2:{type: "int", default: 42}, v3:"uuid"},
                    key:["v1"],
                    indexes : ["v3"]
                };

                var TestModelDef = ap.add_model("test_defaults", model_test_def);

                TestModelDef.init(function(err){
                    if(err) return done(err);
                    var ins = new TestModelDef({'v1': 500});
                    ins.save(function(err,result){
                        assert.notOk(err);
                        assert.propertyVal(ins,'v2',42);
                        done();
                    });
                });

            });

            it('successful save with default fields, value given', function(done){
                var model_test_def = {
                    fields:{v1:"int",v2:{type: "int", default: 42}, v3:"uuid"},
                    key:["v1"],
                    indexes : ["v3"]
                };

                var TestModelDef = ap.add_model("test_defaults", model_test_def);

                TestModelDef.init(function(){
                    var ins = new TestModelDef({'v1': 500, 'v2':40});
                    ins.save(function(err,result){
                        assert.notOk(err);
                        assert.propertyVal(ins,'v2', 40);
                        done();
                    });
                });

            });


            it('successful save with default fields, null value given', function(done){
                var model_test_def = {
                    fields:{v1:"int",v2:{type: "int", default: 42}, v3:"uuid"},
                    key:["v1"],
                    indexes : ["v3"]
                };

                var TestModelDef = ap.add_model("test_defaults", model_test_def);

                TestModelDef.init(function(){
                    var ins = new TestModelDef({'v1': 500, 'v2':null});
                    ins.save(function(err,result){
                        assert.notOk(err);
                        assert.isNull(ins.v2);
                        done();
                    });
                });

            });

            it('successful save with default fields (js function)', function(done){
                var model_test_def = {
                    fields:{v1:"int",v2:{type: "int", default: function(){return 43;}}, v3:"uuid"},
                    key:["v1"],
                    indexes : ["v3"]
                };

                var TestModelDef = ap.add_model("test_defaults", model_test_def);

                TestModelDef.init(function(){
                    var ins = new TestModelDef({'v1': 501});
                    ins.save(function(err,result){
                        assert.notOk(err);
                        assert.propertyVal(ins,'v2', 43);
                        done();
                    });
                });

            });

            it('successful save with default fields (db function)', function(done){
                var model_test_def = {
                    fields:{v1:"int",v2:"int", v3:{type:"uuid",default:{"$db_function":"uuid()"}} },
                    key:["v1"],
                    indexes : ["v3"]
                };

                var TestModelDef = ap.add_model("test_defaults", model_test_def,{'mismatch_behaviour':'drop'});

                TestModelDef.init(function(){
                    var ins = new TestModelDef({'v1': 502});
                    ins.save(function(err,result){
                        assert.notOk(err);
                        assert.ok(ins.v3);
                        done();
                    });
                });

            });


            it('successful save with default fields (js function on instance fields)', function(done){
                var model_test_def = {
                    fields:{v1:"int",v2:{type: "int", default: function(){return this.v1*2;} }, v3:"uuid"},
                    key:["v1"],
                    indexes : ["v3"]
                };

                var TestModelDef = ap.add_model("test_defaults", model_test_def);

                TestModelDef.init(function(){
                    var ins = new TestModelDef({'v1': 501});
                    ins.save(function(err,result){
                        assert.notOk(err);
                        TestModelDef.find({'v1': 501}, function(err, results){
                            assert.notOk(err);
                            assert.lengthOf(results, 1);
                            assert.propertyVal(results[0],'v2', 1002);
                            done();
                        });
                    });
                });

            });

            it('faulty save with default fields returning a wrong type', function(done){
                var model_test_def = {
                    fields:{v1:"int",v2:{type: "int", default: function(){return 'foo';}}, v3:"uuid"},
                    key:["v1"],
                    indexes : ["v3"]
                };

                var TestModelDef = ap.add_model("test_defaults", model_test_def);

                TestModelDef.init(function(){
                    var ins = new TestModelDef({'v1': 500});
                    ins.save(function(err,result){
                        assert.ok(err);
                        assert.propertyVal(err, 'name', 'apollo.model.save.invaliddefaultvalue');
                        done();
                    });
                });

            });

            describe('Validation of default values > ',function(){

                it('validator is called on default if provided by value', function(done){

                    var model_test_def = {
                        fields:{
                            v1:"int",
                            v3:{
                                type: "text",
                                rule: function(value){return value.length == 8;},
                                default: 'a'
                            }
                        },
                        key:["v1"],
                        indexes : ["v3"]
                    };

                    var TestModelDef = ap.add_model("test_ignore_default", model_test_def, {'mismatch_behaviour':'drop'});
                    var ins;

                    assert.doesNotThrow(function(){
                        ins = new TestModelDef({v1:50});
                    });
                    ins.save(function(err){
                        assert.ok(err);
                        assert.match(err.toString(), /apollo\.model\.save\.invaliddefaultvalue/i);
                        done();
                    });
                });

                it('validator is called on default if provided as a JS function', function(done){

                    var model_test_def = {
                        fields:{
                            v1:"int",
                            v3:{
                                type: "text",
                                rule: function(value){return value.length == 8;},
                                default: function(){return 'a';}
                            }
                        },
                        key:["v1"],
                        indexes : ["v3"]
                    };

                    var TestModelDef = ap.add_model("test_ignore_default", model_test_def, {'mismatch_behaviour':'drop'});
                    var ins;

                    assert.doesNotThrow(function(){
                        ins = new TestModelDef({v1:50});
                    });
                    ins.save(function(err){
                        assert.ok(err);
                        assert.match(err.toString(), /apollo\.model\.save\.invaliddefaultvalue/i);
                        done();
                    });
                });

                it('validator is not called on default if provided as a DB function', function(done){

                    var model_test_def = {
                        fields:{
                            v1:"int",
                            v3:{
                                type: "timeuuid",
                                rule: {
                                    validator:function(value){return value.length == 8;},
                                    message : 'invalid value, length must be 8'
                                },
                                default:{"$db_function":"now()"}
                            }
                        },
                        key:["v1"],
                        indexes : ["v3"]
                    };

                    var TestModelDef = ap.add_model("test_ignore_default", model_test_def, {'mismatch_behaviour':'drop'});
                    var ins;

                    assert.doesNotThrow(function(){
                        ins = new TestModelDef({v1:50});
                    });
                    ins.save(function(err){
                        assert.notOk(err);
                        done();
                    });
                });

                it('validator is not called on default if ignore_default is true', function(done){
                    var custom_validation_schema = {
                        fields:{
                            v1:"int",
                            v3:{
                                'type': "int",
                                'rule': {
                                    validator: function(v){ return v > 10; },
                                    ignore_default: true
                                },
                                'default': function(){ return 5; }
                            }
                        },
                        key:["v1"]
                    };
                    var TestModel = ap.add_model("test_ignore_default", custom_validation_schema, {'mismatch_behaviour':'drop'});
                    var ins;
                    assert.doesNotThrow(function(){
                        ins = new TestModel({v1:50});
                    });
                    ins.save(function(err){
                        assert.notOk(err);
                        done();
                    });
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
                // TestModel.drop_table(function(){

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

                // });

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
                TestModel = ap.add_model("test_delete", model_test1);
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

    });


    describe('Types tests >', function(){

        var apollo,
            model_types = {
                fields:{v1:"int",v2:"double",v3:"float"},
                key:["v1"]
            },
            Types;

        before(function(done) {
            apollo = new Apollo(connection);
            apollo.connect(function(err){
                if(err) return done(err);
                Types = apollo.add_model( 'types', model_types );
                Types.drop_table(function(err){
                    if(err) return done(err);
                    Types.init(function(err){
                        if(err) return done(err);
                        async.each(
                            [1,2,3,4,5,6,7,8,9,10],
                            function(i, cb){
                                var t = new Types({v1: i, v2: i * 1.0, v3: i * 1.0});
                                t.save(cb);
                            },
                            done
                        );
                    });
                });

            });
        });

        it('select correctly a row automatically casting type', function(done){
            Types.find({'v1': 1.0}, function(err, result){
                assert.instanceOf(result[0], Types);
                assert.deepEqual(result[0].v1, 1);
                done(err);
            });
        });

        it('select throw an exception for wrong type', function(done){
            Types.find({'v1': 10.1}, function(err, result){
                assert.ok(err);
                done();
            });
        });

        it('save correctly a row with generic types (int, double, float)', function(done){
            var t = new Types({v1: 12.0});
            t.save(done);
        });

        it('save and find uuid', function(done){
            var model_find_schema_uuid = {
                fields:{v1:"uuid",v2:"text"},
                key:["v1"]
            };
            TestModel = apollo.add_model("test_find_uuid", model_find_schema_uuid, {'mismatch_behaviour':'drop'});
            var uuid = apollo.uuid();
            var t = new TestModel({v1:uuid, v2: "hi" });
            t.save(function(err){
                if(err) return done(err);
                TestModel.find({v1:uuid},function(err,results){
                    assert.notOk(err);
                    assert.lengthOf(results, 1);
                    done();
                });
            });
        });

        it('save and find timeuuid', function(done){
            var model_find_schema_uuid = {
                fields:{v1:"timeuuid",v2:"text"},
                key:["v1"]
            };
            TestModel = apollo.add_model("test_find_timeuuid", model_find_schema_uuid, {'mismatch_behaviour':'drop'});
            var tuuid = apollo.timeuuid();
            var t = new TestModel({v1:tuuid, v2: "hi" });
            t.save(function(err){
                if(err) return done(err);
                TestModel.find({v1:tuuid},function(err,results){
                    assert.notOk(err);
                    assert.lengthOf(results, 1);
                    done();
                });
            });
        });

        it('correctly escape texts', function(done){
            var model_find_schema = {
                fields:{v1:"text"},
                key:["v1"]
            };
            TestModel = apollo.add_model("test_find_text", model_find_schema, {'mismatch_behaviour':'drop'});
            var t = new TestModel({v1: "Peter o'Toole" });
            t.save(function(err){
                if(err) return done(err);
                TestModel.find({v1:"Peter o'Toole"},function(err,results){
                    assert.notOk(err);
                    assert.lengthOf(results, 1);
                    done();
                });
            });
        });

    });

});

