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

  grunt.registerTask('test', ['jshint', 'mochaTest']);
  grunt.registerTask('default', ['jshint', 'mochaTest', 'jsdoc']);

};