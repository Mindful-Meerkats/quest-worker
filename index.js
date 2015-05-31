// load rethinkdb
r = require('rethinkdb');

// throw errors -- noop handler
var throw_errors = function( err, blaat ){
    if( err ) throw err;
};

// ignore errors -- noop handler
var ignore_errors = function( err, blaat ){};

// debugger
var debug = function( msg, obj ){
    console.log( msg );
    if( obj ) console.log( obj );
};


var SECOND = 1000;
var MINUTE = 60*SECOND;
var HOUR   = 60*MINUTE;

// actual worker
function work( conn ){
    
    r.table('quests').sample(3).run( conn, function( err, cursor ){
        if( err ) throw err;
        
        cursor.toArray( function( err, quests ){
            if( err ) throw err;
            
            quests.forEach( function( quest ){
                
                r.table('meerkats').between(0,9, {index: "awaiting"}).run( conn, function( err, cursor ){
                    if( err ) throw err;
                    
                    cursor.toArray( function( err, meerkats ){
                        if( err ) throw err;
                        
                        meerkats.forEach( function( meerkat ){
                  
                            debug('adding quest ' + quest.id + ' to meerkat ' + meerkat.id );
                            r.table('meerkats').get( meerkat.id ).update( function(row){
                                return {
                                    quests: {
                                        awaiting: row('quests')('awaiting').append( quest )
                                    }
                                };
                            }).run( conn, throw_errors );
                            
                        });
                        
                    });
                    
                });
                
            });
            
        }); 
        
    });
    
    setTimeout( function(){
        work( conn );
    }, 5*MINUTE );
    
}

// open general connection for rethinkdb
r.connect({ host: 'localhost', port: 28015, db: 'development' }, function( err, conn ){
    if( err ) throw err;
    debug("rethink loaded");
    work( conn );
});    

