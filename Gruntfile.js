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
          output: 'coverage.html'
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
            src: ['libs/**/*.js'], 
            options: {
                destination: 'docs'
            }
        }
    }

  });

 
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-jsdoc');
  grunt.loadNpmTasks('grunt-mocha-cov');


  grunt.registerTask('default', ['jshint', 'test', 'doc']);
  grunt.registerTask('doc', ['jsdoc']);
  grunt.registerTask('test', ['mochacov']);

};