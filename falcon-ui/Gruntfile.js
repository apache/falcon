module.exports = function (grunt) {

  grunt.initConfig({
    copy: {
      resources: {
        cwd: 'app',
        src: ['html/**/*.html', 'index.html', 'config/*'],
        dest: 'dist/',
        expand: true
      },

      dependencies: {
        cwd: 'app/',
        src: ['css/fonts/*', 'css/img/*'],
        dest: 'dist/',
        expand: true
      },

      webapp : {
        cwd: 'dist',
        src: ['**/*.*'],
        dest: '../webapp/src/main/webapp/public/',
        expand: true
      },
      
      ambariview : {
        cwd: 'dist',
        src: ['**/*.*'],
        dest: '../falcon-ambari-view/src/main/resources/ui/',
        expand: true
      }
    },
    
    concat: {
      options: {
        separator: '\n\n',
        banner: '/**************************************************************/\n' + 
                '/*********Concatenated Vendor minified dependencies ***********/\n' + 
                '/**************************************************************/\n'
      },
      vendor: {
        src: [ 'app/js/lib/jquery-1.11.1.min.js', 
               'app/js/lib/angular.min.js',
               'app/js/lib/angular-cookies.min.js',
               'app/js/lib/uirouter.min.js',
               'app/js/lib/ui-bootstrap-tpls-0.11.0.min.js',
               'app/js/lib/d3.min.js',
               'app/js/lib/xml2json.min.js',
               'app/js/lib/angular-mocks.js',
               'app/js/lib/checklist-model.js',
               'app/js/lib/angular-animate.min.js',
               'app/js/lib/angular-messages.min.js'
        ],
        dest: 'dist/js/vendor.min.js'
      }
    },   
    
    uglify: {
      options: {
        beautify: true,
        mangle: true,
        compress: true,
        preserveComments: false,
        drop_console: false,
        sourceMap: true,
        banner: '/**** Apache Falcon UI ***/'
      },
      main: {
        files: {
          'dist/js/main.min.js': [
            'app/js/controllers/**/*-module.js',
            'app/js/controllers/**/*.js',
            'app/js/directives/*.js',
            'app/js/services/**/*.js',
            'app/js/services/services.js',
            'app/js/app.js'
          ]
        }
      }
    },

    jshint: {
      options: {
        eqeqeq: true,
        curly: true,
        undef: false,
        unused: true,
        force: true
      },
      target: {
        src: [
          'app/js/app.js',
          'app/js/controllers/**/*.js',
          'app/js/directives/*.js',
          'app/js/services/*.js'
        ]
      }
    },

    csslint: {
      strict: {
        src: ['dist/css/*.css']
      }
    },

    datauri: {
      'default': {
        options: {
          classPrefix: 'data-'
        },
        src: [
          'css/img/*.png',
          'css/img/*.gif',
          'css/img/*.jpg',
          'css/img/*.bmp'
        ],
        dest: [
          'tmp/base64Images.css'
        ]
      }
    },

    less: {
      development: {
        options: {
          compress: true,
          yuicompress: false,
          optimization: 2,
          cleancss: false,
          syncImport: false,
          strictUnits: false,
          strictMath: true,
          strictImports: true,
          ieCompat: false
        },
        files: {
          'dist/css/main.css': 'app/css/main.less'
        }
      }
    },

    watch: {
      less: {
        files: ['app/css/*.less', 'app/css/less/*.less', 'app/css/styles/*.less'],
        tasks: ['less'],
        options: {
          nospawn: true,
          livereload: true
        }
      },
      resources: {
        options: {
          livereload: true
        },
        files: ['app/html/**/*.html', 'app/index.html', 'app/css/fonts/*'],
        tasks: ['resources']
      },

      source: {
        options: {
          livereload: true
        },
        files: ['app/js/**/*.js', 'app/test/**/*Spec.js'],
        tasks: ['jshint', 'uglify', 'karma:unit:run' ]
      }
    },

    express: {
      server: {
        options: {
          script: 'server.js'
        }
      }
    },

    clean: ["dist"],

    scp: {
      options: {
        host: '127.0.0.1',
        username: 'root',
        password: 'hadoop',
        port: 2222
      },

      sandbox: {
        files: [
          {
            cwd: 'dist',
            src: '**',
            filter: 'isFile',
            // path on the server
            dest: '/usr/hdp/2.2.0.0-913/falcon/webapp/falcon/public'
          }
        ]
      }
    },

    karma: {
      unit: {
        configFile: 'karma.conf.js',       
        singleRun: true,
        autoWatch: false
      },
      continuous: {
        configFile: 'karma.conf.js',
        singleRun: false,
        autoWatch: false,
        background: true,
        browsers: ['PhantomJS']
      }
    }

  });

  grunt.registerTask('resources', ['copy:resources']);
  grunt.registerTask('dependencies', ['copy:dependencies']);
  grunt.registerTask('test', ['karma:continuous']);
  grunt.registerTask('build', ['clean', 'concat:vendor', 'uglify', 'less', 'resources', 'dependencies']);
  grunt.registerTask('w', ['build', 'karma:unit:start', 'watch']);
  grunt.registerTask('server', ['express', 'w']);
  grunt.registerTask('default', ['server']);
  grunt.registerTask('data64', ['datauri']);
  
  grunt.registerTask('dev', ['express', 'clean', 'concat:vendor', 'uglify', 'less', 'resources', 'dependencies','karma:unit:start', 'karma:continuous', 
                             'watch']);
  grunt.registerTask('deploy', ['clean', 'concat:vendor', 'uglify', 'less', 'resources', 'dependencies', 'karma:unit','scp']);
  
  
  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-less');
  grunt.loadNpmTasks('grunt-contrib-csslint');
  grunt.loadNpmTasks('grunt-contrib-copy');
  grunt.loadNpmTasks('grunt-contrib-clean');
  grunt.loadNpmTasks('grunt-scp');
  grunt.loadNpmTasks('grunt-express-server');
  grunt.loadNpmTasks('grunt-datauri');
  grunt.loadNpmTasks('grunt-karma');
  grunt.loadNpmTasks('grunt-contrib-concat');

};
