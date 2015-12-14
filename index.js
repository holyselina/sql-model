/**
 * 封装sql语句到模型方法
 */
var Promise = require("bluebird");
var _ = require('lodash');
var resolver = require('./lib/resolver');
var mysqlCreater = require('./lib/mysqlCreater');
//数据库操作对象的原型
var proto = {
    /*
     * 代表数据库中 NULL 值
     */
    nullValue:resolver.nullValue,
    /**
     * 代表数据库中 NOT NULL 值,主要用于查询
     */
    notNullValue:resolver.notNullValue,
    /*
     * 预处理数据库对象
     * @param params 查询的条件对象
     */
     hanldeDB : function(params){
        params = params || {};
        var ret = this.db;
        if(!ret && global.db){
            ret = global.db;
        }
        var v = getSpecialValue(params,'db');
        if(v){
            ret =  v;
        }
        if(_.isFunction(ret)){
            ret = ret.call(this,params);
        }
        return ret;
     },
    /*
     * 实际执行sql语句
     * @param db hanldeDB()返回的db对象
     * @param sql 实际的sql语句
     * @param callback 回调
     */
     execSQL:function(db,sql,callback){
        db.query(sql,callback);
     },
    /*
     * 根据条件生成查询的sql语句
     * @param params 查询参数对象
     * @param [select] 需要选择的数据列,多列空格分开,不传则查询所有列
     */
    getFindSQL : function(params,select){
        params = params || {};
        var jn,join=this.join,isJoin=getSpecialValue(params,'join');
        if(join && isJoin != null){
            if(isJoin === true){
                jn = join;
            }else if(Array.isArray(join)){
                if(!Array.isArray(isJoin)){
                    isJoin = [isJoin];
                }
                jn=_.filter(join,function(v,idx){
                      return _.includes(isJoin,v.tableName) || _.includes(isJoin, v.alias)
                });
            }
        }
        var limit=getSpecialValue(params,'limit'),
            sort= getSpecialValue(params,'sort'),
            groupBy=getSpecialValue(params,'groupBy'),
            noJoinSelect=getSpecialValue(params,'noJoinSelect');
        return this.getQuerySQL(params,
            {select:select,limit:limit,sort:sort,groupBy:groupBy,join:jn
            ,noJoinSelect:noJoinSelect});
    },
    /**
     * 根据查询条件查询结果集
     * @param params  查询参数对象
     * @param [select] 需要选择的数据列,多列空格分开,不传则查询所有列
     * @param callback 回调
     */
    find : function(params,select,callback){
        if(_.isFunction(select)){
            callback = select;
            if(_.isString(params)){
                select = params;
                params = null;
            }else{
                select = null;
            }
        }else if(_.isFunction(params)){
            callback = params;
            params = select = null;
        }
        var sql = this.getFindSQL(params,select);
        var db = this.hanldeDB(params);
        this.execSQL(db,sql,callback);
    },
    /**
     * 查询结果集的数量
     * @param params 查询参数对象
     * @param callback 回调
     */
    count:function(params,callback){
      if(_.isFunction(params)){
          callback=params;
          params=null;
      }
      params = params || {};
      setSpecialValue(params,'noJoinSelect',true);
      this.find(params,'#count(*) as totalCount',function(err,rows){
             getSpecialValue(params,'noJoinSelect',true);
             if(err){
                 return callback(err);
             }
             callback(null,rows[0].totalCount);
      });
    },
    /**
     * 按照分页查询结果集
     * @param params 查询参数对象
     * @param [select] 需要选择的数据列,多列空格分开,不传则查询所有列
     * @param callback 回调
     */
    findByPage : function(params,select,callback){
        if(_.isFunction(select)){
            callback = select;
            if(_.isString(params)){
                select = params;
                params = null;
            }else{
                select = null;
            }
        }else if(_.isFunction(params)){
            callback = params;
            params = select = null;
        }
        var limit;
        var sort;
        var self = this;
        if(params){
            limit =  getSpecialValue(params,'limit',true);
            sort =  getSpecialValue(params,'sort',true);
        }
        var result = {total:0,list:[]};
        self.count(params,function (err,count){
            if(err){
                return callback(err);
            }
            result.total = count;
            if(count > 0){
                if(params){
                    setSpecialValue(params,'limit',limit);
                    setSpecialValue(params,'sort',sort);
                }
                self.find(params,select,function list(err,rows){
                    if(err){
                        return callback(err);
                    }
                    result.list = rows;
                    callback(null,result);
                });
            }else{
                callback(null,result);
            }
        });
    },
    /**
     * 批量查询结果集
     * @param paramsArr 参数数组
     * @param [select] 需要选择的数据列,多列空格分开,不传则查询所有列
     * @param callback 回调
     */
    findBatch : function(paramsArr,select,callback){
        if(_.isFunction(select)){
            callback = select;
            select = null;
        }
        if(paramsArr.length == 0){
            return callback(null,[]);
        }
        var sqls = [],self=this;
        paramsArr.forEach(function(ele){
            var sql = self.getFindSQL(ele,ele.$select || select);
            sqls.push(sql);
        });
        var db = this.hanldeDB(paramsArr);
        this.execSQL(db,sqls.join(';'),function(err,rs){
            if(err){
                return callback(err);
            }
            if(sqls.length == 1){
                rs = [rs];
            }
            callback(null,rs);
        });
    },
    /**
     * 保存一条记录
     * @param params 参数对象
     * @param callback 回调
     */
    save : function(params,callback){
        params = params || {};
        var sql = this.getInsertSQL(params);
        var db = this.hanldeDB(params);
        this.execSQL(db,sql,callback);
    },
    /**
     * 批量保存记录
     * @param paramsArr 参数数组
     * @param callback 回调
     */
    saveBatch : function(paramsArr,callback){
        if(paramsArr.length == 0){
            return callback(null);
        }
        var sqls = [],self=this;
        paramsArr.forEach(function(ele){
            var sql = self.getInsertSQL(ele);
            sqls.push(sql);
        });
        var db = this.hanldeDB(paramsArr);
        this.execSQL(db,sqls.join(';'),callback);
    },
    /**
     * 删除记录
     * @param params 条件参数
     * @param callback 回调
     */
    remove : function(params,callback){
        if(_.isFunction(params)){
            callback = params;
            params = null;
        }
        var sql = this.getDeleteSQL(params,
           {limit:getSpecialValue(params,'limit')});
        var db = this.hanldeDB(params);
        this.execSQL(db,sql,callback);
    },
    /**
     * 批量删除记录
     * @param paramsArr 参数数组
     * @param callback 回调
     */
    removeBatch : function(paramsArr,callback){
        if(paramsArr.length == 0){
            return callback(null);
        }
        var sqls = [],self=this;
        paramsArr.forEach(function(ele){
            var sql = self.getDeleteSQL(ele,
                {limit:getSpecialValue(ele,'limit')});
            sqls.push(sql);
        });
        var db = this.hanldeDB(paramsArr);
        this.execSQL(db,sqls.join(';'),callback);
    },
    /**
     * 更新记录
     * @param updateObj 需要更新的对象
     * @param params 查询条件对象
     * @param callback 回调
     */
    update:function(updateObj,params,callback){
		if(_.isFunction(params)){
            callback = params;
			params = null;
        }
        var sql = this.getUpdateSQL(updateObj,params,
            {limit:getSpecialValue(params,'limit')});
        var db = this.hanldeDB(params);
        this.execSQL(db,sql,callback);
    },
    /**
     * 开始一个事务
     * @param callback 回调
     */
    beginTransaction:function(callback){
        var db=this.hanldeDB();
        if(db.beginTransaction){
            db.beginTransaction(callback);
        }else{
            callback(new Error('db not support beginTransaction'));
        }
    },
    /**
     * 连接的指定的事务上面
     * @param db beginTransaction 返回的事务对象
     */
    link:function(db){
        var obj = Object.create(this);
        if(db){
            obj.db = db;
        }
        obj.commit = function(cb){
            this.db.commit(cb);
        };
        obj.rollback = function(cb){
            this.db.rollback(cb);
        };
        Promise.promisifyAll(obj,{
            filter: function(name, func, target, passesDefaultFilter) {
                return ['commit','rollback'].indexOf(name) != -1;
            }
        });
        return obj;
    },
    eachSchema:function(fn){
       var schema = this.schema || {};
       var self = this;
       Object.keys(schema).forEach(function(key){
           fn(key,self.getSchemaValue(key));
       });
    },
    getSchemaValue:function(key){
        var schema = this.schema || {};
        var ret = schema[key];
        if(_.isString(ret)){
            ret = {
                field : ret
            };
        }
        return ret;
    },
    wrapField:function(field){
        if(field){
            return '`' + field + '`';
        }
        return field;
    },
    getSelect:function(select){
        var ret = [];
        var tar = [];
        var exclude = [];
        var self = this;
        var alias = this.getAlias();
        if(select){
            select = select.trim();
            if(select[0] == '#'){
                return select.substring(1);
            }
            if(select[0] == '!'){
                exclude = select.substring(1).split(/\s+/);
            }else{
                tar = select.split(/\s+/);
            }
        }
        this.eachSchema(function(key,val){
            if(exclude.length > 0 && _.includes(exclude,key)){
                return;
            }
            if(tar.length > 0 && !_.includes(tar,key)){
                return;
            }
            var field = val.field;
            if(field && key !== field){
                ret.push(alias+'.'+self.wrapField(field) + ' AS ' + self.wrapField(key));
            }else{
                ret.push(alias+'.'+self.wrapField(key));
            }
        });
        return ret.join(',');
    },
    getTableSQL:function(){
       var ret = this.wrapField(this.tableName);
       if(this.alias){
           ret += (' AS ' + this.wrapField(this.alias));
       }
       return ret;
    },
    getAlias:function(){
       return this.wrapField(this.alias || this.tableName);
    },
    getWhere:function(params,addWhere){
		var self = this;
        var alias = this.getAlias();
		var ret = parse('$and',params,function(key,val){
			  var type = self.getSchemaValue(key);
			  if(!type){
				  return;
			  }
			  var field = type.field || key;
			  var info = resolver.resolveParamValue(val);
              if(info.value == null)return;
			  return alias+'.'+self.wrapField(field) + (info.prefix ? (info.prefix + ' '+info.value) : info.value);
		});
        if(ret == null){
            ret = '';
        }
		if(addWhere){
            ret = ret + resolver.parseSQL(addWhere,params);
        }
        if(this.where){
            ret = ret + resolver.parseSQL(this.where,params);
        }
        return prefixWhere(ret);
    },
    parseJoin:parseJoin,
    getQuerySQL : function(params,options){
        options = options || {};
        var select = options.select;
        var selectSql = this.getSelect(select) || '';
        var tableAndJoin=this.getTableSQL();
        var joinWhere;
        var paresedJoin = this.parseJoin(options.join,{
            noJoinSelect:options.noJoinSelect
           ,params:params
        });
        if(paresedJoin){
            if(paresedJoin.select){
                if(selectSql) selectSql += ',';
                selectSql += paresedJoin.select;
            }
            tableAndJoin +=  paresedJoin.join;
            joinWhere = paresedJoin.where;
            tableAndJoin = resolver.parseSQL(tableAndJoin,params);
        }
		var where =  this.getWhere(params,joinWhere);
        var ret = 'SELECT ' + selectSql + ' FROM ' + tableAndJoin + where;
        var sort = options.sort;
        var limit = options.limit;
        var groupBy = options.groupBy;
        if(groupBy != null){
            ret += ' GROUP BY '+ groupBy.trim();
        }
        if(sort != null){
            ret += ' ORDER BY '+ sort.trim();
        }
        if(limit != null){
            ret += ' LIMIT ' + limit.toString();
        }
        return ret;
    },
    getInsertSQL : function(obj,options){
        var schema = this.schema;
        var tableName = this.tableName;
        var fieldArr = [];
        var valArr = [];
        var self = this;
        options = options || {};
        var hanlde = function(model){
            if(!_.isObject(model)){
                throw new Error('插入参数错误');
            }
            var output = [];
            self.eachSchema(function(key,def){
                var val = model[key];
                if (val == null){
                   val = def.default;
                }
                if (val == null){
                   return;
                }
                var fieldName = def.field || key;
                if(!_.includes(fieldArr,fieldName)){
                    fieldArr.push(fieldName);
                }
                output.push(resolver.resolveParamValue(val).value);
            });
            valArr.push('('+output.join(',')+')');
        };
        if(_.isArray(obj)){
            obj.forEach(function(ele){
                hanlde(ele);
            });
        }else{
            hanlde(obj);
        }
        return 'INSERT INTO '+tableName + '('+fieldArr.join(',')+') VALUES ' + valArr.join(',');
    },
    getUpdateSQL:function(updateObj,params,options){
        var retVal = 'UPDATE '+this.getTableSQL() + ' SET ';
        var arr = [];
        var keys = Object.keys(updateObj);
        var self = this;
        var alias = this.getAlias();
        keys.forEach(function(key){
            var type = self.getSchemaValue(key);
            if(!type) return;
            var fieldName = self.wrapField(type.field || key);
            var v = updateObj[key];
            if(v != null){
                if(_.isString(v) && v[0] == '#'){
                    arr.push(alias+'.'+fieldName+'='+v.substring(1));
                }else{
                    arr.push(alias+'.'+fieldName+'='+resolver.resolveParamValue(v).value);
                }
            }
        });
        retVal += arr.join(',');
		var where =  this.getWhere(params);
        if(where){
            retVal += where;
        }
        options = options || {};
        if(options.limit){
            retVal += ' limit ' + options.limit.toString();
        }
        return retVal;
    },
    getDeleteSQL:function(params,options){
        var tableName = this.getTableSQL();
        var where = this.getWhere(params);
        var retVal = 'delete from '+tableName;
        if(where){
            retVal += where;
        }
        options = options || {};
        if(options.limit){
            retVal += ' limit ' + options.limit.toString();
        }
        return retVal;
    }
};

Promise.promisifyAll(proto,{
    filter: function(name, func, target, passesDefaultFilter) {
        return ['find','count','findByPage','findBatch','save','saveBatch','removeBatch',
            'remove','update','beginTransaction'].indexOf(name) != -1;
    }
    ,multiArgs:false
});

exports.extend = function(exps){
    exps.__proto__ = Object.create(proto);
};

exports.define = function (schema,tableName,exps,db,join){
    exps.tableName=tableName;
    exps.schema=exps.modelSchema=schema;
    exps.db=db;
    exps.join=join;
    exports.extend(exps);
};

exports.mysqlCreater = mysqlCreater;

exports.resolver=resolver;

function prefixWhere(where){
    if(!where || !(where = where.trim())){
        return '';
    }
    var chk;
    if((chk=where.substring(0,5)) && chk.toLowerCase() == 'where'){
        return where;
    }else if((chk=where.substring(0,2)) && chk.toLowerCase() == 'or'){
        where = where.substring(2);
    }else if((chk=where.substring(0,3)) && chk.toLowerCase() == 'and'){
        where = where.substring(3);
    }
    return ' where '+ where.trim();
}

function joinCriteria(arr,join){
    if(!arr || arr.length == 0){
        return;
    }else if(arr.length == 1){
        return arr.join(' '+join+' ');
    }
    return '('+arr.join(' '+join+' ')+')';
}

function parseChild(key,obj,join,parseValue){
    var ret = [];
    if(_.isArray(obj)){
        obj.forEach(function(val){
            if(_.isObject(val)){
                var v = parse(key,val,parseValue);
                if(v != null){
                    ret.push(v);
                }
            }
        });
    }else if(_.isObject(obj)){
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
    if(key == '$and'){
        return parseChild(key,val,'AND',parseValue);
    }
    if(key == '$or'){
        return parseChild(key,val,'OR',parseValue);
    }
    if(_.isObject(val) && _.includes(Object.keys(val),'$or')){
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

function getSpecialValue(params,key,isDel){
    if(!params) {
        return;
    }
    var v = params['$'+key] || params['_'+key];
    if(key == 'limit' && v == null){
        var eachPageNum = params.eachPageNum;
        var curPage = params.curPage
        if(_.isNumber(eachPageNum) && _.isNumber(curPage)){
            if(isDel){
                delete params.eachPageNum;
                delete params.curPage;
            }
            return [curPage * eachPageNum,eachPageNum];
        }
    }
    if(v != null && isDel){
        delete params['$'+key];
        delete params['_'+key];
    }
    return v;
}

function setSpecialValue(params,key,value){
    params['$'+key] = value;
}

function parseJoin(joins,options){
    if(!joins){
        return null;
    }
    var self = this;
    var params = options.params;
    var joinWhere = '';
    var join = '';
    var joinSelect = '';
    var hande = function(joinObj){
        join += ' ';
        var sct = joinObj.select
            ,tbn = joinObj.tableName
            ,joinSql = joinObj.sql
            ,tbw = joinObj.where
            ,tbwSchema = joinObj.whereSchema
            ,ty = joinObj.type || 'INNER'
            ,alias = joinObj.alias;
        if(joinSql){
            if(!alias){
                throw new Error('join sql must set alias');
            }
            tbn = joinSql;
        }else{
            tbn = self.wrapField(tbn);
        }
        if(alias){
            alias = self.wrapField(alias);
        }
        join += (ty.toUpperCase() + ' JOIN '+ tbn +(alias ? ' AS '+alias  : '') + ' ON '+ joinObj.join);
        if(sct && !options.noJoinSelect){
            if(joinSelect){
                joinSelect+=',';
            }
            if(!_.isArray(sct)){
                sct = [sct];
            }
            sct.forEach(function(ele,idx){
                if(idx>0) joinSelect+=',';
                joinSelect += (ele.indexOf('.') == -1 ? (alias || tbn) +'.' : '') + ele;
            });
        }
        if(_.isString(tbw)){
            if(joinWhere) joinWhere += ' ';
            joinWhere += tbw;
        }else if(!tbwSchema && _.isPlainObject(tbw)){
            tbwSchema = tbw;
        }
        if(_.isPlainObject(tbwSchema)){
            var ret = parse('$and',params,function(key,val){
                var field = tbwSchema[key];
                if(!field){
                    return;
                }
                var info = resolver.resolveParamValue(val);
                if(info.value == null)return;
                return (alias || tbn)+'.'+self.wrapField(field)
                    +' '+ (info.prefix ? (info.prefix + ' '+info.value) : info.value);
            });
            if(ret){
                if(joinWhere){
                    joinWhere += ' AND ';
                }
                joinWhere += ret;
            }
        }
    };
    if(_.isArray(joins)){
        joins.forEach(function(ele){
            hande(ele);
        })
    }else if(_.isObject(joins)){
        hande(joins);
    }else{
        return null;
    }
    return {
        where:joinWhere,
        join:join,
        select:joinSelect
    }
}

