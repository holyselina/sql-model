/**
 * sql语句的参数解析
 */

var mysql = require('mysql');
var _ = require('lodash');
var hanlders = [];
//参数正则表达式
var parameterReg = /#\{([^\{\}]+)\}/g;
//可选组正则表达式
var optionsGroupReg = /\[\[(((?!\]\]).)*)\]\]/g;
var nullValue = new Object();
var notNullValue = new Object();
/**
 * 格式化字段的值
 * @param v
 * @returns {*}
 */
function escapeValue(v){
    if(v === nullValue) return 'NULL';
    return mysql.escape(v).replace(/[\ud83c-\udfff]/g,'');
}

function replaceParams(inputSql,params){
    var ret = inputSql;
    if(params){
        ret = inputSql.replace(parameterReg, function (capture, group1) {
            var arr = group1.split(',');
            var propertyPath = arr[0];
            var vParams =arr.slice(1);
            var v = _.get(params,propertyPath);
            if(v == null){
                return capture;
            }
            var info = resolveParamValue(v,vParams);
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

function resolveParamValue(paramValue,options){
    var info = {
        prefix : '=',
        value : paramValue,
        break:false
    };
    for(var i=0;i<hanlders.length;i++){
        var hd = hanlders[i];
        if(hd(info,options) === true){
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

function enabledParam(key,params){
    return params && params.indexOf(key) != -1
}

addHanlder(function emy(info,params){
    if(enabledParam('empty',params)){
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
    if(enabledParam('noPrefix',params)){
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
    var noFormat = enabledParam('noFormat',params);
    if(_.isArray(info.value)){
        var arr =[];
        if(info.prefix == '='){
            info.prefix = 'IN';
        }
        info.value.forEach(function (ele, index) {
            arr.push( noFormat ? ele : (escapeValue(ele)));
        });
        info.value = '('+arr.join(',')+')';
    }else{
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


exports.resolveParamValue=resolveParamValue;
exports.parseSQL=parseSQL;
exports.nullValue=nullValue;
exports.notNullValue=notNullValue;
