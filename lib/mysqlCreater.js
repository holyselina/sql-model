
function firstUpper(str){
    return str.substring(0,1).toUpperCase()+str.substring(1);
}
function firstLower(str){
    return str.substring(0,1).toLowerCase()+str.substring(1);
}

function getFields(tableName,dao,cb){
    var sql = 'SHOW FULL COLUMNS  from '+tableName;
    dao.query(sql,function(err,rows){
          if(!err){
              cb(null,rows);
          }else{
              cb(err);
          }
    });
}

function getSelect(fields,tableName){
    var sql='';
    fields.forEach(function(ele,index){
        if(sql)sql+=',';
        sql += 'a.'+firstUpper(ele) + ' as ' +  firstLower(ele);
    });
    return  'select '+sql+' from ' + tableName + ' a where 1=1 ';
}

function getModelInit(fields,tableName){
    var code ="var db = require('../lib/db/mysql').default;\n";
    code += "var sql = require('../lib/sql');\n";
    code += 'exports.schema = {\n';
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
    code += 'exports.db=db;\n';
    code += 'sql.extend(exports);';
    return code;
}


exports.getCode = function(tableName,dao,cb){
    dao = dao || global.dao;
    cb = cb || function(err,res){
        if(err){
            console.error(err);
        }else{
            console.log(res);
        }
    };
    getFields(tableName,dao,function(err,arr){
        if(err){
            return cb(err);
        }
        var fs = arr.map(function(e){
            return (e.Field);
        });
        cb(null,getModelInit(fs,tableName));
    });
};