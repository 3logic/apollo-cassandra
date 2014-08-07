var chai = require('chai');
var assert = chai.assert;
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
            ap.connect(done);
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


        var model_test5 = { 
            fields:{v1:"int",v2:"int",v3:"int",v4:"int",v5:"int"}, 
            key:[["v1","v2"],"v3"],
            indexes:["v5","v4"] 
        };


        it.only('create table query',function(done){
            var TestModel = ap.add_model("test5", model_test5);
            TestModel.init(done);
        });

        it('add model', function(){
            var TestModel = ap.get_model("test1", model_test1);
            assert.isFunction(TestModel);
            assert.property(TestModel,'find');
            assert.isFalse(TestModel.is_table_ready());
        });


        it('add faulty model (silly type)', function(){
            assert.throws(function(){
                var TestModel = ap.get_model("test1", faulty_model_test1);
            });
        });

        it('instance model', function(){
            var TestModel = ap.get_model("test1", model_test1);
            var ins = new TestModel({'v1': 500});

            assert.propertyVal(ins,'v1',500);
            assert.notProperty(ins,'v2');
            assert.property(ins,'save');
        });


        // describe('Schema conflicts on init',function(){
        //     beforeEach(function(done) {
        //         var TestModel = ap.get_model("test1", model_test1);
        //         TestModel.init(done);
        //     });

        //     it('same name, conflicting schema', function(done){
        //         var TestModel = ap.get_model("test1", model_test2);
        //         TestModel.init(function(err,result){
        //             assert.ok(err);
        //             assert.propertyVal(err,'name','apollo.model.tablecreation.schemamismatch');
        //             done();
        //         });
        //     });

        //     it('same name, same schema', function(done){
        //         var TestModel = ap.get_model("test1", model_test1);
        //         TestModel.init(function(err,result){
        //             assert.notOk(err);
        //             done();
        //         });
        //     });
        // });
         
        describe('Schema conflicts on init > ',function(){
            
            beforeEach(function(done) {
                var BaseModel = ap.get_model("test1", model_test1);
                BaseModel.init(done);
            });

            var conflict_model = model_test3;
            
            it('mismatch_behaviour:default(fail)', function(done){
                var TestModel = ap.get_model("test1", conflict_model);
                TestModel.init(function(err,result){
                    assert.Ok(err);
                    assert.propertyVal(err,'name','apollo.model.tablecreation.schemamismatch');                      
                    done();
                });
               
            });
            
            it('mismatch_behaviour:fail', function(done){
                var TestModel = ap.get_model("test1", conflict_model,{mismatch_behaviour:"fail"});
                TestModel.init(function(err,result){
                    assert.Ok(err);
                    assert.propertyVal(err,'name','apollo.model.tablecreation.schemamismatch');                    
                    done();
                });
               
            });
            
            it('mismatch_behaviour:drop', function(done){
                var TestModel = ap.get_model("test1", conflict_model,{mismatch_behaviour:"drop"});
                TestModel.init(function(err,result){
                    assert.notOk(err);                    
                    done();
                });
               
            });
            
            it('mismatch_behaviour invalid', function(done){
                assert.throw(
                    function(){ 
                        var TestModel = ap.get_model("test1", conflict_model,{mismatch_behaviour:"minchia"});
                } ,"invalid option" );
            });

            it('same name, same schema', function(done){
                var TestModel = ap.get_model("test1", model_test1);
                TestModel.init(function(err,result){
                    assert.notOk(err);
                    done();
                });
            });

        });


        
        describe('Save > ',function(){
            var TestModel;

            beforeEach(function(done) {
                TestModel = ap.get_model("test1", model_test1);
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

        });


        it.skip('pig update', function(done){
            ap.get_model("test1", model_test1, true, function(err,data){
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

