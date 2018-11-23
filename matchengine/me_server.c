/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_server.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_operlog.h"
# include "me_history.h"
# include "me_message.h"

/*---------------------------------------------------------------------------
VARIABLE: static rpc_svr *svr;

PURPOSE: 
    撮合服务结构，定义服务参数、回调函数等

REMARKS:
    config.json中配置
    "svr": {
        "bind": [
            "tcp@0.0.0.0:7316",
            "udp@0.0.0.0:7316"
        ],
        "buf_limit": 100,
        "max_pkg_size": 10240,
        "heartbeat_check": false
    }
---------------------------------------------------------------------------*/
static rpc_svr *svr;

/*---------------------------------------------------------------------------
VARIABLE: static dict_t *dict_cache;

PURPOSE: 
    深度缓存
    收到order.depth命令返回结果时，保存到缓存。下一次收到命令时，如果缓存未超时，返回缓存数据

REMARKS:
    config.json中未配置cache_timeout，默认值为0.45s    
---------------------------------------------------------------------------*/
static dict_t *dict_cache;

/*---------------------------------------------------------------------------
VARIABLE: static nw_timer cache_timer;

PURPOSE: 
    定时清空深度缓存dict_cache

REMARKS:
    60s清空缓存一次
---------------------------------------------------------------------------*/
static nw_timer cache_timer;

/*---------------------------------------------------------------------------
STURCT: struct cache_val

PURPOSE: 
    dict_cache深度缓存中，存储的数据

REMARKS:
---------------------------------------------------------------------------*/
struct cache_val {
    double      time;   // 微秒级时间戳
    json_t      *result;// 缓存字符串
};

/*---------------------------------------------------------------------------
FUNCTION: static int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)

PURPOSE: 
    向接收到的会话命令，发送响应数据

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]json - 响应数据
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
---------------------------------------------------------------------------*/
static int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_data;
    if (settings.debug) {
        message_data = json_dumps(json, JSON_INDENT(4));
    } else {
        message_data = json_dumps(json, 0);
    }
    if (message_data == NULL)
        return -__LINE__;
    log_trace("connection: %s send: %s", nw_sock_human_addr(&ses->peer_addr), message_data);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type = RPC_PKG_TYPE_REPLY;
    reply.body = message_data;
    reply.body_size = strlen(message_data);
    rpc_send(ses, &reply);
    free(message_data);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)

PURPOSE: 
    处理失败时，向接收到的会话命令，发送错误反馈

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]code - 错误代码
    [in]message - 错误内容
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    错误响应的格式为
    {
        "error":{"code":1,"message":"invalid argument"},
        "result":null,
        "id":123
    }
---------------------------------------------------------------------------*/
static int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static int reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 1, "invalid argument");
}

static int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 2, "internal error");
}

static int reply_error_service_unavailable(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 3, "service unavailable");
}

/*---------------------------------------------------------------------------
FUNCTION: static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result)

PURPOSE: 
    处理成功，向接收到的会话命令，发送处理结果

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]result - 处理结果
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    错误响应的格式为
    {
        "error":null,
        "result":{...},
        "id":123
    }
---------------------------------------------------------------------------*/
static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int reply_success(nw_ses *ses, rpc_pkg *pkg)

PURPOSE: 
    处理成功，向接收到的会话命令，发送成功通知

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    错误响应的格式为
    {
        "error":null,
        "result":{"status":"success"},
        "id":123
    }
---------------------------------------------------------------------------*/
static int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static bool process_cache(nw_ses *ses, rpc_pkg *pkg, sds *cache_key)

PURPOSE: 
    收到order.depth时，检查深度缓存是否仍有效，如果有效则以缓存数据发送响应

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [out]cache_key - order.depth命令生成的缓存dict_cache的key值
    
RETURN VALUE: 
    缓存有效，返回true，无效返回false

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    缓存超时时，删除对应key的缓存
---------------------------------------------------------------------------*/
static bool process_cache(nw_ses *ses, rpc_pkg *pkg, sds *cache_key)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "%u", pkg->command);
    key = sdscatlen(key, pkg->body, pkg->body_size);
    dict_entry *entry = dict_find(dict_cache, key);
    if (entry == NULL) {
        *cache_key = key;
        return false;
    }

    struct cache_val *cache = entry->val;
    double now = current_timestamp();
    if ((now - cache->time) > settings.cache_timeout) {
        dict_delete(dict_cache, key);
        *cache_key = key;
        return false;
    }

    reply_result(ses, pkg, cache->result);
    sdsfree(key);
    return true;
}

/*---------------------------------------------------------------------------
FUNCTION: static int add_cache(sds cache_key, json_t *result)

PURPOSE: 
    添加到深度缓存

PARAMETERS:
    [in]cache_key - order.depth命令生成的缓存dict_cache的key值
    [in]result - order.depth的处理结果，用于缓存的value
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    收到order.depth命令，并且生成新的结果时，放到缓存中
---------------------------------------------------------------------------*/
static int add_cache(sds cache_key, json_t *result)
{
    struct cache_val cache;
    cache.time = current_timestamp();
    cache.result = result;
    json_incref(result);
    dict_replace(dict_cache, cache_key, &cache);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_balance_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理balance.query命令

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    balance.query命令格式
    "params": [1, "BTC"]  // [user_id, asset] asset不存在则返回所有资产
    "result": {"BTC": {"available": "1.10000000","freeze": "9.90000000"}}
---------------------------------------------------------------------------*/
static int on_cmd_balance_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t request_size = json_array_size(params);
    if (request_size == 0)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id == 0)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    if (request_size == 1) {
        for (size_t i = 0; i < settings.asset_num; ++i) {
            const char *asset = settings.assets[i].name;
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
            if (available) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(available);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "available", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "available", available);
                }
            } else {
                json_object_set_new(unit, "available", json_string("0"));
            }

            mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
            if (freeze) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(freeze);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "freeze", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "freeze", freeze);
                }
            } else {
                json_object_set_new(unit, "freeze", json_string("0"));
            }

            json_object_set_new(result, asset, unit);
        }
    } else {
        for (size_t i = 1; i < request_size; ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (!asset || !asset_exist(asset)) {
                json_decref(result);
                return reply_error_invalid_argument(ses, pkg);
            }
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
            if (available) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(available);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "available", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "available", available);
                }
            } else {
                json_object_set_new(unit, "available", json_string("0"));
            }

            mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
            if (freeze) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(freeze);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "freeze", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "freeze", freeze);
                }
            } else {
                json_object_set_new(unit, "freeze", json_string("0"));
            }

            json_object_set_new(result, asset, unit);
        }
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_balance_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理balance.update命令

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    balance.update属于写操作，需要检查是否server接收写操作
    更新内存中的可用余额（增加或减少）
    写入balance_history
    推送kafka deals消息
    写入operlog

    balance.update命令格式
    "params": [user_id, asset, business, change, detail] 
    change为负数表示减少资产
    示例
    "params": [1, "BTC", "deposit", 100, "1.2345"]
    "result": "success"    
---------------------------------------------------------------------------*/
static int on_cmd_balance_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec_show(asset);
    if (prec < 0)
        return reply_error_invalid_argument(ses, pkg);

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 10, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("update_balance", params);
    return reply_success(ses, pkg);
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_asset_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理asset.list命令，返回所有支持的asset币种及精度

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
---------------------------------------------------------------------------*/
static int on_cmd_asset_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < settings.asset_num; ++i) {
        json_t *asset = json_object();
        json_object_set_new(asset, "name", json_string(settings.assets[i].name));
        json_object_set_new(asset, "prec", json_integer(settings.assets[i].prec_show));
        json_array_append_new(result, asset);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static json_t *get_asset_summary(const char *name)

PURPOSE: 
    获取币种的余额统计

PARAMETERS:
    [in]name - asset name

RETURN VALUE: 
    统计json结构

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
---------------------------------------------------------------------------*/
static json_t *get_asset_summary(const char *name)
{
    size_t available_count;
    size_t freeze_count;
    mpd_t *total = mpd_new(&mpd_ctx);
    mpd_t *available = mpd_new(&mpd_ctx);
    mpd_t *freeze = mpd_new(&mpd_ctx);
    balance_status(name, total, &available_count, available, &freeze_count, freeze);

    json_t *obj = json_object();
    json_object_set_new(obj, "name", json_string(name));
    json_object_set_new_mpd(obj, "total_balance", total);
    json_object_set_new(obj, "available_count", json_integer(available_count));
    json_object_set_new_mpd(obj, "available_balance", available);
    json_object_set_new(obj, "freeze_count", json_integer(freeze_count));
    json_object_set_new_mpd(obj, "freeze_balance", freeze);

    mpd_del(total);
    mpd_del(available);
    mpd_del(freeze);

    return obj;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_asset_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理asset.summary命令，返回币种余额统计

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    asset.summary命令格式
    "params": ["BTC"]  // asset不存在则返回所有资产
    "result": {
            "freeze_count": 0,
            "available_count": 0,
            "name": "BCH",
            "total_balance": "0",
            "freeze_balance": "0",
            "available_balance": "0"
        }
---------------------------------------------------------------------------*/
static int on_cmd_asset_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    if (json_array_size(params) == 0) {
        for (int i = 0; i < settings.asset_num; ++i) {
            json_array_append_new(result, get_asset_summary(settings.assets[i].name));
        }
    } else {
        for (int i = 0; i < json_array_size(params); ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (asset == NULL)
                goto invalid_argument;
            if (!asset_exist(asset))
                goto invalid_argument;
            json_array_append_new(result, get_asset_summary(asset));
        }
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    json_decref(result);
    return reply_error_invalid_argument(ses, pkg);
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_put_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理order.put_limit命令，返回币种余额统计

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    order.put_limit属于写操作，需要检查是否server接收写操作
    更新内存中的可用余额（增加或减少）
    执行撮合，未完全成交，则写入买卖队列，并冻结资产
    taker/maker完全成交，写入卖卖双方的order_history
    导致资产变动，写入卖卖双方的balance_history
    推送kafka orders/deals消息
    写入operlog

    order.put_limit命令格式
    parmams:[user_id,market,side,amount,price,taker_fee_rate,maker_fee_rate,source]
    示例
    {"method": "order.put_limit", "params": [1,"BTCBCH",1,"1","10000","0.002","0.001","api"], "id": 1516681174}
    {
        "error": null,
        "result": {
            "mtime": 1542272592.443491,
            "id": 1,
            "deal_fee": "0",
            "market": "BTCBCH",
            "taker_fee": "0.002",
            "source": "api",
            "maker_fee": "0.001",
            "type": 1,
            "side": 1,
            "user": 1,
            "ctime": 1542272592.443491,
            "left": "1",
            "price": "10000",
            "amount": "1",
            "deal_stock": "0",
            "deal_money": "0"
        },
        "id": 1516681174
    }
---------------------------------------------------------------------------*/
static int on_cmd_order_put_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 8)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount    = NULL;
    mpd_t *price     = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *maker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price 
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // maker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    maker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (maker_fee == NULL || mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    json_t *result = NULL;
    int ret = market_put_limit_order(true, &result, market, user_id, side, amount, price, taker_fee, maker_fee, source);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("limit_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (maker_fee)
        mpd_del(maker_fee);

    return reply_error_invalid_argument(ses, pkg);
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_put_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理order.put_market命令，返回币种余额统计

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    order.put_market属于写操作，需要检查是否server接收写操作
    更新内存中的可用余额（增加或减少）
    执行撮合，未完全成交，也会关闭委单，只成交能够成交的部分
    taker/maker完全成交，写入卖卖双方的order_history
    导致资产变动，写入卖卖双方的balance_history
    推送kafka orders/deals消息
    写入operlog

    order.put_market命令格式
    parmams:[user_id,market,side,amount,taker_fee_rate,source]
    示例
    {"method": "order.put_market", "params": [2,"BTCBCH",2,"100","0.002","test"], "id": 1516681174}
    {
        "error": null,
        "result": {
            "mtime": 1542275054.6989839,
            "id": 2,
            "deal_fee": "0.00002",
            "market": "BTCBCH",
            "taker_fee": "0.002",
            "source": "test",
            "maker_fee": "0",
            "type": 2,
            "side": 2,
            "user": 2,
            "ctime": 1542275054.698972,
            "left": "0e-16",
            "price": "0",
            "amount": "100",
            "deal_stock": "0.01",
            "deal_money": "100"
        },
        "id": 1516681174
    }
---------------------------------------------------------------------------*/
static int on_cmd_order_put_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount = NULL;
    mpd_t *taker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 4)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 5));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    json_t *result = NULL;
    int ret = market_put_market_order(true, &result, market, user_id, side, amount, taker_fee, source);

    mpd_del(amount);
    mpd_del(taker_fee);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "amount too small");
    } else if (ret == -3) {
        return reply_error(ses, pkg, 12, "no enough trader");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("market_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);

    return reply_error_invalid_argument(ses, pkg);
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理 order.pending 命令，返回用户的挂单

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    order.pending 命令格式
    parmams:[user_id,market,offset,limit]
    示例
    "params": [1, "BTCCNY", 0, 100]"
    "result": {
        "offset": 0,
        "limit": 100,
        "total": 1,
        "records": [
            {
                "id": 2,
                "ctime": 1492616173.355293,
                "mtime": 1492697636.238869,
                "market": "BTCCNY",
                "user": 2,
                "type": 1, // 1: limit order，2：market order
                "side": 2, // 1：sell，2：buy
                "amount": "1.0000".
                "price": "7000.00",
                "taker_fee": "0.0020",
                "maker_fee": "0.0010",
                "source": "web",
                "deal_money": "6300.0000000000",
                "deal_stock": "0.9000000000",
                "deal_fee": "0.0009000000"
            }
        ]
    }
---------------------------------------------------------------------------*/
static int on_cmd_order_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *orders = json_array();
    skiplist_t *order_list = market_get_order_list(market, user_id);
    if (order_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        json_object_set_new(result, "total", json_integer(order_list->len));
        if (offset < order_list->len) {
            skiplist_iter *iter = skiplist_get_iterator(order_list);
            skiplist_node *node;
            for (size_t i = 0; i < offset; i++) {
                if (skiplist_next(iter) == NULL)
                    break;
            }
            size_t index = 0;
            while ((node = skiplist_next(iter)) != NULL && index < limit) {
                index++;
                order_t *order = node->value;
                json_array_append_new(orders, get_order_info(order));
            }
            skiplist_release_iterator(iter);
        }
    }

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_cancel(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理 order.cancel 命令，返回用户的委单信息

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    order.cancel 属于写操作，需要检查是否server接收写操作
    关闭委单，解冻资产
    推送kafka orders消息
    写入operlog

    order.cancel 命令格式
    parmams:[user_id,market,order_id]
---------------------------------------------------------------------------*/
static int on_cmd_order_cancel(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }
    if (order->user_id != user_id) {
        return reply_error(ses, pkg, 11, "user not match");
    }

    json_t *result = NULL;
    int ret = market_cancel_order(true, &result, market, order);
    if (ret < 0) {
        log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("cancel_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理 order.book 命令，返回市场的订单信息

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    order.book 命令格式
    parmams:[market,side,offset,limit]
---------------------------------------------------------------------------*/
static int on_cmd_order_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 1));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));

    uint64_t total;
    skiplist_iter *iter;
    if (side == MARKET_ORDER_SIDE_ASK) {
        iter = skiplist_get_iterator(market->asks);
        total = market->asks->len;
        json_object_set_new(result, "total", json_integer(total));
    } else {
        iter = skiplist_get_iterator(market->bids);
        total = market->bids->len;
        json_object_set_new(result, "total", json_integer(total));
    }

    json_t *orders = json_array();
    if (offset < total) {
        for (size_t i = 0; i < offset; i++) {
            if (skiplist_next(iter) == NULL)
                break;
        }
        size_t index = 0;
        skiplist_node *node;
        while ((node = skiplist_next(iter)) != NULL && index < limit) {
            index++;
            order_t *order = node->value;
            json_array_append_new(orders, get_order_info(order));
        }
    }
    skiplist_release_iterator(iter);

    json_object_set_new(result, "orders", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static json_t *get_depth(market_t *market, size_t limit)

PURPOSE: 
    返回市场的深度信息

PARAMETERS:
    [in]market - 货币对市场
    [in]limit  - 接收到的数据报文
    
RETURN VALUE: 
    买卖队列的json结构

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
---------------------------------------------------------------------------*/
static json_t *get_depth(market_t *market, size_t limit)
{
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    size_t index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);

    return result;
}

/*---------------------------------------------------------------------------
FUNCTION: static json_t *get_depth_merge(market_t* market, size_t limit, mpd_t *interval)

PURPOSE: 
    返回合并精度后的市场的深度信息

PARAMETERS:
    [in]market - 货币对市场
    [in]limit  - 接收到的数据报文
    
RETURN VALUE: 
    买卖队列的json结构

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
---------------------------------------------------------------------------*/
static json_t *get_depth_merge(market_t* market, size_t limit, mpd_t *interval)
{
    mpd_t *q = mpd_new(&mpd_ctx);
    mpd_t *r = mpd_new(&mpd_ctx);
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    size_t index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        if (mpd_cmp(r, mpd_zero, &mpd_ctx) != 0) {
            mpd_add(price, price, interval, &mpd_ctx);
        }
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) >= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) <= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }

        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(q);
    mpd_del(r);
    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);

    return result;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_book_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理 order.depth 命令，返回市场的订单信息

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    order.depth 可以使用缓存查询，避免过多影响效率，缓存有效时间为0.45s
    order.depth 命令格式
    parmams:[market,limit,interval]
---------------------------------------------------------------------------*/
static int on_cmd_order_book_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // limit
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 1));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    // interval
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *interval = decimal(json_string_value(json_array_get(params, 2)), market->money_prec);
    if (!interval)
        return reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(interval);
        return reply_error_invalid_argument(ses, pkg);
    }

    sds cache_key = NULL;
    if (process_cache(ses, pkg, &cache_key)) {
        mpd_del(interval);
        return 0;
    }

    json_t *result = NULL;
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) == 0) {
        result = get_depth(market, limit);
    } else {
        result = get_depth_merge(market, limit, interval);
    }
    mpd_del(interval);

    if (result == NULL) {
        sdsfree(cache_key);
        return reply_error_internal_error(ses, pkg);
    }

    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理 order.pending_detail 命令，查询挂单，返回订单信息

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    如果订单已经关闭，返回空值
    order.pending_detail 命令格式
    parmams:[market,order_id]
---------------------------------------------------------------------------*/
static int on_cmd_order_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 1));

    order_t *order = market_get_order(market, order_id);
    json_t *result = NULL;
    if (order == NULL) {
        result = json_null();
    } else {
        result = get_order_info(order);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_order_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理 market.list 命令，返回所有货币对的基础信息

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    order.pending_detail 命令格式
    parmams:[]
---------------------------------------------------------------------------*/
static int on_cmd_market_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < settings.market_num; ++i) {
        json_t *market = json_object();
        json_object_set_new(market, "name", json_string(settings.markets[i].name));
        json_object_set_new(market, "stock", json_string(settings.markets[i].stock));
        json_object_set_new(market, "money", json_string(settings.markets[i].money));
        json_object_set_new(market, "fee_prec", json_integer(settings.markets[i].fee_prec));
        json_object_set_new(market, "stock_prec", json_integer(settings.markets[i].stock_prec));
        json_object_set_new(market, "money_prec", json_integer(settings.markets[i].money_prec));
        json_object_set_new_mpd(market, "min_amount", settings.markets[i].min_amount);
        json_array_append_new(result, market);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static json_t *get_market_summary(const char *name)

PURPOSE: 
    读取货币对的买卖统计

PARAMETERS:
    [in]name  - market name
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
---------------------------------------------------------------------------*/
static json_t *get_market_summary(const char *name)
{
    size_t ask_count;
    size_t bid_count;
    mpd_t *ask_amount = mpd_new(&mpd_ctx);
    mpd_t *bid_amount = mpd_new(&mpd_ctx);
    market_t *market = get_market(name);
    market_get_status(market, &ask_count, ask_amount, &bid_count, bid_amount);
    
    json_t *obj = json_object();
    json_object_set_new(obj, "name", json_string(name));
    json_object_set_new(obj, "ask_count", json_integer(ask_count));
    json_object_set_new_mpd(obj, "ask_amount", ask_amount);
    json_object_set_new(obj, "bid_count", json_integer(bid_count));
    json_object_set_new_mpd(obj, "bid_amount", bid_amount);

    mpd_del(ask_amount);
    mpd_del(bid_amount);

    return obj;
}

/*---------------------------------------------------------------------------
FUNCTION: static int on_cmd_market_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)

PURPOSE: 
    处理 market.summary 命令，返回货币对的统计信息

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    [in]params - 命令参数
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    market.summary 命令格式
    parmams:[market]  // 可以为空
---------------------------------------------------------------------------*/
static int on_cmd_market_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    if (json_array_size(params) == 0) {
        for (int i = 0; i < settings.market_num; ++i) {
            json_array_append_new(result, get_market_summary(settings.markets[i].name));
        }
    } else {
        for (int i = 0; i < json_array_size(params); ++i) {
            const char *market = json_string_value(json_array_get(params, i));
            if (market == NULL)
                goto invalid_argument;
            if (get_market(market) == NULL)
                goto invalid_argument;
            json_array_append_new(result, get_market_summary(market));
        }
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    json_decref(result);
    return reply_error_invalid_argument(ses, pkg);
}

/*---------------------------------------------------------------------------
FUNCTION: static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)

PURPOSE: 
    server的回调函数，负责接收数据并根据请求类型，分发执行

PARAMETERS:
    [in]ses  - 命令请求session
    [in]pkg  - 接收到的数据报文
    
RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
---------------------------------------------------------------------------*/
static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_BALANCE_QUERY:
        log_trace("from: %s cmd balance query, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_query %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_UPDATE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance update, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_update %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_LIST:
        log_trace("from: %s cmd asset list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_asset_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ASSET_SUMMARY:
        log_trace("from: %s cmd asset summary, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_asset_summary(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_asset_summary %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_LIMIT:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order put limit, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_MARKET:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order put market, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_market %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_QUERY:
        log_trace("from: %s cmd order query, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_query %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order cancel, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_cancel(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_BOOK:
        log_trace("from: %s cmd order book, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_book(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_BOOK_DEPTH:
        log_trace("from: %s cmd order book depth, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_book_depth(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book_depth %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_DETAIL:
        log_trace("from: %s cmd order detail, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_detail(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_detail %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_LIST:
        log_trace("from: %s cmd market list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_SUMMARY:
        log_trace("from: %s cmd market summary, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_summary(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_summary%s fail: %d", params_str, ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

cleanup:
    sdsfree(params_str);
    json_decref(params);
    return;

decode_error:
    if (params) {
        json_decref(params);
    }
    sds hex = hexdump(pkg->body, pkg->body_size);
    log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", \
            nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
    sdsfree(hex);
    rpc_svr_close_clt(svr, ses);

    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static uint32_t cache_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sdslen((sds)key));
}

static int cache_dict_key_compare(const void *key1, const void *key2)
{
    return sdscmp((sds)key1, (sds)key2);
}

static void *cache_dict_key_dup(const void *key)
{
    return sdsdup((const sds)key);
}

static void cache_dict_key_free(void *key)
{
    sdsfree(key);
}

static void *cache_dict_val_dup(const void *val)
{
    struct cache_val *obj = malloc(sizeof(struct cache_val));
    memcpy(obj, val, sizeof(struct cache_val));
    return obj;
}

static void cache_dict_val_free(void *val)
{
    struct cache_val *obj = val;
    json_decref(obj->result);
    free(val);
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_cache_timer(nw_timer *timer, void *privdata)

PURPOSE: 
    深度缓存清理定时器
    每60s执行一次，清空深度缓存

PARAMETERS:
    [in]timer -
    [in]privdata     - 

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    dict_clear操作与dict_find异步执行，不知道会不会存在冲突
---------------------------------------------------------------------------*/
static void on_cache_timer(nw_timer *timer, void *privdata)
{
    dict_clear(dict_cache);
}

/*---------------------------------------------------------------------------
FUNCTION: int init_server(void)

PURPOSE: 
    初始化并启动server
    初始化深度缓存，并启动缓存清理定时器

PARAMETERS:
    None

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    
---------------------------------------------------------------------------*/
int init_server(void)
{
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = cache_dict_hash_function;
    dt.key_compare    = cache_dict_key_compare;
    dt.key_dup        = cache_dict_key_dup;
    dt.key_destructor = cache_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;

    dict_cache = dict_create(&dt, 64);
    if (dict_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_timer, NULL);
    nw_timer_start(&cache_timer);

    return 0;
}

