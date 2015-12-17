
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

exports.getCode = function(tableName,dao,cb){
    dao = dao || global.dao || global.db;
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
        var code = 'exports.schema = {\n';
        arr.forEach(function(ele,idx){
            var field = ele.Field;
            code +=  '    '+ firstLower(field) + ' : "'+ field + '"';//' : { field : \'' +(ele) +'\' }';
            if(idx < arr.length-1){
                code+=',';
            }
            if(ele.Comment){
                code += ' \/\/'+ele.Comment;
            }
            if(idx < arr.length-1){
                code+='\n';
            }
        });
        code += '\n};';
        code += '\n';
        code += 'exports.tableName="'+tableName + '";';
        code += '\n';
        code += 'exports.db=db;\n';
        code += 'sqlModel.extend(exports);';
        cb(null,code);
    });
};