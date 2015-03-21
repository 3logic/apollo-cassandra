module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),

    mochacov: {
      test: {
        options: {
          reporter: 'spec'
        }
      },
      coverage: {
        options: {
          coveralls: {
            repoToken: '0aRlqM9q5jCD6L0Gnjx0sxmNHT5dI23aC'
          }
        }
      },
      local_coverage: {
        options: {
          reporter: 'html-cov',
          quiet: true,
          output: 'coverage/coverage.html'
        }
      },
      md: {
        options: {
          reporter: 'markdown',
          output: 'coverage/tests_desc.md'
        }
      },
      options: {
        files:{
          src: ['test/**/*.js']
        }
      }
    },
    
    jshint: {
      dev:{
        files: {
          src: ['Gruntfile.js', 'libs/**/*.js', 'test/**/*.js']
        },
        options: {
          force: true,
          // options here to override JSHint defaults
          globals: {}
        }
      }
    },
    jsdoc : {
        dist : {
            src: ['README.md', 'libs/**/*.js'], 
            options: {
                  private : false,
                  destination: 'docs/<%= pkg.version %>',
                  lenient: true,
                  template :  "node_modules/grunt-jsdoc/node_modules/ink-docstrap/template",
                  configure : "jsdoc.conf.json",
                  //tutorials : 'resources/tutorials',
                  verbose : true
                }
        }
    }

  });

 
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-jsdoc');
  grunt.loadNpmTasks('grunt-mocha-cov');


  grunt.registerTask('default', ['jshint', 'test', 'doc']);
  grunt.registerTask('doc', ['jsdoc']);
  
  if(process.env.TRAVIS){
    grunt.registerTask('test', ['mochacov:test', 'mochacov:coverage']);
  }else{
    grunt.registerTask('test', ['mochacov:test', 'mochacov:local_coverage']);
  }

};