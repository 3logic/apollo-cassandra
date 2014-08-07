module.exports = function(grunt) {

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),

    mochacov: {
      test: {
        options: {
          reporter: 'spec'
        },
        src: ['test/**/*.js']
      },
      coverage: {
        options: {
          coveralls: {
            repoToken: '0aRlqM9q5jCD6L0Gnjx0sxmNHT5dI23aC'
          }
        },
        src: ['test/**/*.js']
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


  grunt.registerTask('test', ['mochacov']);
  grunt.registerTask('default', ['jshint', 'mochaTest', 'jsdoc']);

};