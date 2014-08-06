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

describe('Apollo', function(){
    
    describe('Global library', function(){

        var apollo;

        describe('New Apollo', function(){
            
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

    
    describe('On apollo instances',function(){

        var ap;

        beforeEach(function(done) {
            if(ap)
                ap.close();

            ap = new Apollo(connection);

            // Setup
            ap.connect(done);
        });


        var model_test1 = { 
            fields:{v1:"int",v2:"int"}, 
            key:["v1"] 
        };


        var model_test2 = { 
            fields:{v1:"int",v2:"text"}, 
            key:["v1"] 
        };

        var faulty_model_test1 = { 
            fields:{v1:"int",v2:"foo"}, 
            key:["v1"] 
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
            })
        });

        it('instance model', function(){
            var TestModel = ap.add_model("test1", model_test1);
            var ins = new TestModel({'v1': 500});

            assert.propertyVal(ins,'v1',500);
            assert.notProperty(ins,'v2');
            assert.property(ins,'save');
        });


        it('same name, conflicting schema', function(done){
            var TestModel = ap.add_model("test1", model_test2);
            TestModel.init(function(err,result){
                assert.ok(err);
                done();
            });
        });

        it('same name, same schema', function(done){
            var TestModel = ap.add_model("test1", model_test1);
            TestModel.init(function(err,result){
                assert.notOk(err);
                done();
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

