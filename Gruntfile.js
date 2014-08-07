module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),

    mochaTest: {
      test: {
        options: {
          reporter: 'spec'
        },
        src: ['test/**/*.js']
      }
    },
    mochacov: {
      coverage: {
        options: {
          coveralls: true
        }
      },
      options: {
        files: 'test/**/*.js'
      }
    },
    
    jshint: {
      files: ['Gruntfile.js', 'libs/**/*.js', 'test/**/*.js'],
      options: {
        force: true,
        // options here to override JSHint defaults
        globals: {}
      }
    },
    jsdoc : {
        dist : {
            src: ['libs/**/*.js'], 
            options: {
                destination: 'docs'
            }
        }
    }

  });

 
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-mocha-test');
  grunt.loadNpmTasks('grunt-jsdoc');
  grunt.loadNpmTasks('grunt-mocha-cov');


  grunt.registerTask('test', ['mochaTest','mochacov']);
  grunt.registerTask('default', ['jshint', 'mochaTest', 'jsdoc']);

};