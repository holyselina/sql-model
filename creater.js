
var mySqlDao = require('./lib/db/mysql').default;
 

function firstLower(str){
    return str.substring(0,1).toLowerCase()+str.substring(1);
}

function getFields(tableName,cb){
    var sql = 'SHOW FULL COLUMNS  from '+tableName;
    mySqlDao.query(sql,function(err,rows){
          if(!err){
             cb(rows);
          }else{
              console.error(err);
          }
    });
}

function firstUpper(str){
    return str.substring(0,1).toUpperCase()+str.substring(1);
}

function getSelect(fields,tableName){
    var sql='';
    fields.forEach(function(ele,index){
        if(sql)sql+=',';
        sql += 'a.'+firstUpper(ele) + ' as ' +  firstLower(ele);
    });
    return  'select '+sql+' from ' + tableName + ' a where 1=1 ';
}

var tableName = 'userexamgather';

function getModelInit(fields){
    var code = 'exports.schema = {\n';
    fields.forEach(function(ele,idx){
        code +=  '    '+firstLower(ele) + ' : "'+ ele + '"';//' : { field : \'' +(ele) +'\' }';
        if(idx < fields.length-1){
            code+=',';
            code+='\n';
        }
    });
    code += '\n};';
    code += '\n';
    code += 'exports.tableName="'+tableName + '";';
    code += '\n';
    code += 'exports.db=db;';
    return code;
}

getFields(tableName,function(arr){
    var fs = arr.map(function(e){
        return (e.Field);
    });
    console.log(getModelInit(fs));
    process.exit();
});
/*
 console.log(getSelect(Uq));
 console.log(getModelInit(Uq));*/
/* var tableName = 'vipassistant';
*/
/*
getFieldsSqlite('GJZC_GK',tableName,function(fs){
    console.log(getSelect(fs,tableName));
    console.log(getModelInit(fs));
    console.log(JSON.stringify(fs))
});*/
