/**
 * sql语句的参数解析
 */
var _ = require('lodash');
var hanlders = [];
//参数正则表达式
var parameterReg = /#\{([^\{\}]+)\}/g;
//可选组正则表达式
var optionsGroupReg = /\[\[(((?!\]\]).)*)\]\]/g;
var nullValue = new Object();
var notNullValue = new Object();
var sqlString = require('./sqlString');
/**
 * 格式化字段的值
 * @param v
 * @returns {*}
 */
function escapeValue(v){
    if(v === nullValue) return 'NULL';
    return sqlString.escape(v).replace(/[\ud83c-\udfff]/g,'');
}

function replaceParams(inputSql,params){
    var ret = inputSql;
    if(params){
        ret = inputSql.replace(parameterReg, function (capture, group1) {
            var arr = group1.split(',');
            var propertyPath = arr[0];
            var vParams =arr.slice(1);
            var temp = {};
            if(vParams && vParams.length){
                vParams.forEach(function(ele){
                     var key = ele.split('=')[0];
                     temp[key] = ele.split('=')[1] || true;
                });
            }
            vParams=temp;
            var v = _.get(params,propertyPath);
            if(v == null){
                return capture;
            }
            var info = parseParamValue(v,vParams);
            return info.prefix ? (info.prefix + ' '+info.value) : info.value;
        });
    }
    return ret;
}

function hasParams(inputSql,params){
    var flag = false;
    if(params){
        inputSql.replace(parameterReg, function (capture, group1) {
            if(flag) return;
            var arr = group1.split(',');
            var propertyPath = arr[0];
            var v =  _.get(params,propertyPath);
            if(v != null && !flag){
                flag=true;
            }
        });
    }
    return flag;
}

function parseParamValue(paramValue,params){
    var info = {
        prefix : '=',
        value : paramValue,
        break:false
    };
    for(var i=0;i<hanlders.length;i++){
        var hd = hanlders[i];
        if(hd(info,params) === true){
            return info;
        }
        if(info.break){
            return info;
        }
    }
    return info;
}

function addHanlder(fn){
    if(!_.isFunction(fn)) throw new Error('fn must function');
    hanlders.push(fn);
}

function parseSQL(sql,params){
    if(!sql) return '';
    sql=sql.replace(optionsGroupReg, function (capture, group1) {
        if(hasParams(group1,params)){
            return group1;
        }else{
            return '';
        }
    });
    return replaceParams(sql,params);
}

function getParam(key,params){
    if(_.isPlainObject(params)){
        return params[key];
    }
}

addHanlder(function emy(info,params){
    if(getParam('empty',params)){
        info.prefix = '';
        info.value = '';
        return true;
    }
});

addHanlder(function obj(info,params){
    var v = info.value;
    if(_.isPlainObject(v)){
        for (var key in v){
            info.prefix = key;
            info.value = v[key];
            if(info.value != null){
                break;
            }
        }
    }
});

addHanlder(function noPrefix(info,params){
    if(getParam('noPrefix',params)){
        info.prefix = '';
    }
});

addHanlder(function nullVal(info,params){
    if(info.value === nullValue){
        info.break= true;
        info.prefix = 'IS';
        info.value = 'NULL';
    }else if(info.value === notNullValue){
        info.break= true;
        info.prefix = 'IS NOT';
        info.value = 'NULL';
    }
});

addHanlder(function fmt(info,params){
    var noFormat = getParam('noFormat',params);
    if(_.isArray(info.value)){
        var arr =[];
        if(info.prefix == '='){
            info.prefix = 'IN';
        }
        info.value.forEach(function (ele, index) {
              if(ele != null){
                  arr.push( noFormat ? ele : (escapeValue(ele)));
              }
        });
        if(arr.length>0){
            info.value = '('+arr.join(',')+')';
        }
    }else if(info.value != null){
        info.value = (noFormat ? info.value : (escapeValue(info.value)));
    }
});

var aliasMapping = {
    $notIn : 'NOT IN',
    $in : "IN",
    $like : "LIKE",
    $notLike : 'NOT LIKE',
    $gte : ">=",
    $lte: "<=",
    $gt : ">",
    $lt : "<",
    $ne: "!="
};

addHanlder(function (info,params){
    if(info.prefix){
        var alias = aliasMapping[info.prefix];
        if(alias){
            info.prefix = alias;
        }
    }
});


function joinCriteria(arr,join){
    if(!arr || arr.length === 0){
        return;
    }
    var mf = function(ele){
        if(_.isString(ele)){
            return ele;
        }else{
            return ele.value;
        }
    }
    if(arr.length === 1){
        return mf(arr[0]);
    }
    arr = _.chain(arr).sortBy('sort').map(mf).value();
    return '('+arr.join(' '+join+' ')+')';
}

function parseChild(key,obj,join,parseValue){
    var ret = [];
    if(_.isArray(obj)){
        obj.forEach(function(val){
            if(_.isPlainObject(val)){
                var v = parseParamsObject(val,parseValue);
                if(v != null){
                    ret.push(v);
                }
            }
        });
    }else if(_.isPlainObject(obj)){
        Object.keys(obj).forEach(function(key){
            var val = obj[key];
            var v = parse(key,val,parseValue);
            if(v != null){
                ret.push(v);
            }
        });
    }
    return joinCriteria(ret,join);
}

function parse(key,val,parseValue){
    if(key === '$and'){
        return parseChild(key,val,'AND',parseValue);
    }
    if(key === '$or'){
        return parseChild(key,val,'OR',parseValue);
    }
    if(_.isPlainObject(val) && _.includes(Object.keys(val),'$or')){
        var vs = val['$or'];
        var temp = [];
        if(_.isArray(vs)){
            vs.forEach(function(ele){
                var v = parseValue(key,ele);
                if(v != null){
                    temp.push(v);
                }
            });
        }
        return joinCriteria(temp,'OR');
    }
    return parseValue(key,val);
}

function parseParamsObject(obj,parseValue){
    return parse('$and',obj,parseValue)
}

exports.parseParamValue=parseParamValue;
exports.parseSQL=parseSQL;
exports.parseParamsObject=parseParamsObject;
exports.nullValue=nullValue;
exports.notNullValue=notNullValue;
