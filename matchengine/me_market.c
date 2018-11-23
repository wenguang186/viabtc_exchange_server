/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/16, create
 */

# include "me_config.h"
# include "me_market.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"

/*---------------------------------------------------------------------------
VARIABLE: uint64_t order_id_start;

PURPOSE: 
    最后一次的委单id，新的委单使用++order_id_start作为委单id

REMARKS: 
    保存快照时，保存该值。恢复快照时，恢复该值。
---------------------------------------------------------------------------*/
uint64_t order_id_start;

/*---------------------------------------------------------------------------
VARIABLE: uint64_t deals_id_start;

PURPOSE: 
    最后一次的成交id，新的成交单使用++order_id_start作为id

REMARKS: 
    保存快照时，保存该值。恢复快照时，恢复该值。
---------------------------------------------------------------------------*/
uint64_t deals_id_start;

struct dict_user_key {
    uint32_t    user_id;
};

struct dict_order_key {
    uint64_t    order_id;
};

static uint32_t dict_user_hash_function(const void *key)
{
    const struct dict_user_key *obj = key;
    return obj->user_id;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    const struct dict_user_key *obj1 = key1;
    const struct dict_user_key *obj2 = key2;
    if (obj1->user_id == obj2->user_id) {
        return 0;
    }
    return 1;
}

static void *dict_user_key_dup(const void *key)
{
    struct dict_user_key *obj = malloc(sizeof(struct dict_user_key));
    memcpy(obj, key, sizeof(struct dict_user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void dict_user_val_free(void *key)
{
    skiplist_release(key);
}

static uint32_t dict_order_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_order_key));
}

static int dict_order_key_compare(const void *key1, const void *key2)
{
    const struct dict_order_key *obj1 = key1;
    const struct dict_order_key *obj2 = key2;
    if (obj1->order_id == obj2->order_id) {
        return 0;
    }
    return 1;
}

static void *dict_order_key_dup(const void *key)
{
    struct dict_order_key *obj = malloc(sizeof(struct dict_order_key));
    memcpy(obj, key, sizeof(struct dict_order_key));
    return obj;
}

static void dict_order_key_free(void *key)
{
    free(key);
}

/*---------------------------------------------------------------------------
FUNCTION: static int order_match_compare(const void *value1, const void *value2)

PURPOSE: 
    比较两个同一买卖方向委单的优先次序

PARAMETERS:
    value1 - 委单1的对象指针
    value2 - 委单2的对象指针

RETURN VALUE: 
    >0，value1比value2靠后，撮合优先级低，在队列中要往后排
    <0，value1比value2靠前，撮合优先级高，在队列中要往前排
    =0，value1与value2是同一委单

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static int order_match_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }
    if (order1->type != order2->type) {
        return 1;
    }

    int cmp;
    if (order1->side == MARKET_ORDER_SIDE_ASK) {
        cmp = mpd_cmp(order1->price, order2->price, &mpd_ctx);
    } else {
        cmp = mpd_cmp(order2->price, order1->price, &mpd_ctx);
    }
    if (cmp != 0) {
        return cmp;
    }

    return order1->id > order2->id ? 1 : -1;
}

static int order_id_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;
    if (order1->id == order2->id) {
        return 0;
    }

    return order1->id > order2->id ? -1 : 1;
}

static void order_free(order_t *order)
{
    mpd_del(order->price);
    mpd_del(order->amount);
    mpd_del(order->taker_fee);
    mpd_del(order->maker_fee);
    mpd_del(order->left);
    mpd_del(order->freeze);
    mpd_del(order->deal_stock);
    mpd_del(order->deal_money);
    mpd_del(order->deal_fee);
    free(order->market);
    free(order->source);
    free(order);
}

/*---------------------------------------------------------------------------
FUNCTION: json_t *get_order_info(order_t *order)

PURPOSE: 
    将order_t转换为json_t

PARAMETERS:
    <Parameter name> –<Parameter description>

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
json_t *get_order_info(order_t *order)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "market", json_string(order->market));
    json_object_set_new(info, "source", json_string(order->source));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "side", json_integer(order->side));
    json_object_set_new(info, "user", json_integer(order->user_id));
    json_object_set_new(info, "ctime", json_real(order->create_time));
    json_object_set_new(info, "mtime", json_real(order->update_time));

    json_object_set_new_mpd(info, "price", order->price);
    json_object_set_new_mpd(info, "amount", order->amount);
    json_object_set_new_mpd(info, "taker_fee", order->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", order->maker_fee);
    json_object_set_new_mpd(info, "left", order->left);
    json_object_set_new_mpd(info, "deal_stock", order->deal_stock);
    json_object_set_new_mpd(info, "deal_money", order->deal_money);
    json_object_set_new_mpd(info, "deal_fee", order->deal_fee);

    return info;
}

/*---------------------------------------------------------------------------
FUNCTION: static int order_put(market_t *m, order_t *order)

PURPOSE: 
    把委单插入market的买卖队列，并冻结未成交部分的资产额度
PARAMETERS:
    m - 
    order - 

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    限价单不能完全成交时，写入market的深度
---------------------------------------------------------------------------*/
static int order_put(market_t *m, order_t *order)
{
    if (order->type != MARKET_ORDER_TYPE_LIMIT)
        return -__LINE__;

    struct dict_order_key order_key = { .order_id = order->id };
    if (dict_add(m->orders, &order_key, order) == NULL)
        return -__LINE__;

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(m->users, &user_key, order_list) == NULL)
            return -__LINE__;
    }

    if (order->side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_insert(m->asks, order) == NULL)
            return -__LINE__;
        mpd_copy(order->freeze, order->left, &mpd_ctx);
        if (balance_freeze(order->user_id, m->stock, order->left) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->bids, order) == NULL)
            return -__LINE__;
        mpd_t *result = mpd_new(&mpd_ctx);
        mpd_mul(result, order->price, order->left, &mpd_ctx);
        mpd_copy(order->freeze, result, &mpd_ctx);
        if (balance_freeze(order->user_id, m->money, result) == NULL) {
            mpd_del(result);
            return -__LINE__;
        }
        mpd_del(result);
    }

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int order_finish(bool real, market_t *m, order_t *order)

PURPOSE: 
    关闭委单（完全成交、撤单），从买卖队列中删除，解冻资产，并添加到数据库委单历史

PARAMETERS:
    real -
    m -
    order -

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static int order_finish(bool real, market_t *m, order_t *order)
{
    if (order->side == MARKET_ORDER_SIDE_ASK) {
        skiplist_node *node = skiplist_find(m->asks, order);
        if (node) {
            skiplist_delete(m->asks, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, m->stock, order->freeze) == NULL) {
                return -__LINE__;
            }
        }
    } else {
        skiplist_node *node = skiplist_find(m->bids, order);
        if (node) {
            skiplist_delete(m->bids, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, m->money, order->freeze) == NULL) {
                return -__LINE__;
            }
        }
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->orders, &order_key);

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
    }

    if (real) {
        if (mpd_cmp(order->deal_stock, mpd_zero, &mpd_ctx) > 0) {
            int ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
        }
    }

    order_free(order);
    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: market_t *market_create(struct market *conf)

PURPOSE: 
    根据配置创建market变量
    
PARAMETERS:
    conf - 从config.json中读取的maret配置
    
RETURN VALUE: 
    None

EXCEPTION: 
    Return market_t pointer， if success，else NULL

EXAMPLE CALL:
    for (size_t i = 0; i < settings.market_num; ++i) {
        market_t *m = market_create(&settings.markets[i]);
        if (m == NULL) {
            return -__LINE__;
        }

        dict_add(dict_market, settings.markets[i].name, m);
    }

REMARKS: 
    config.json中的货币对的定义
    {
        "name": "BTCBCH",
        "stock": {
            "name": "BTC",
            "prec": 8
        },
        "money": {
            "name": "BCH",
            "prec": 8
        },
        "min_amount": "0.001"
    }
---------------------------------------------------------------------------*/
market_t *market_create(struct market *conf)
{
    if (!asset_exist(conf->stock) || !asset_exist(conf->money))
        return NULL;
    if (conf->stock_prec + conf->money_prec > asset_prec(conf->money))
        return NULL;
    if (conf->stock_prec + conf->fee_prec > asset_prec(conf->stock))
        return NULL;
    if (conf->money_prec + conf->fee_prec > asset_prec(conf->money))
        return NULL;

    market_t *m = malloc(sizeof(market_t));
    memset(m, 0, sizeof(market_t));
    m->name             = strdup(conf->name);
    m->stock            = strdup(conf->stock);
    m->money            = strdup(conf->money);
    m->stock_prec       = conf->stock_prec;
    m->money_prec       = conf->money_prec;
    m->fee_prec         = conf->fee_prec;
    m->min_amount       = mpd_qncopy(conf->min_amount);

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_user_hash_function;
    dt.key_compare      = dict_user_key_compare;
    dt.key_dup          = dict_user_key_dup;
    dt.key_destructor   = dict_user_key_free;
    dt.val_destructor   = dict_user_val_free;

    m->users = dict_create(&dt, 1024);
    if (m->users == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_order_hash_function;
    dt.key_compare      = dict_order_key_compare;
    dt.key_dup          = dict_order_key_dup;
    dt.key_destructor   = dict_order_key_free;

    m->orders = dict_create(&dt, 1024);
    if (m->orders == NULL)
        return NULL;

    skiplist_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare          = order_match_compare;

    m->asks = skiplist_create(&lt);
    m->bids = skiplist_create(&lt);
    if (m->asks == NULL || m->bids == NULL)
        return NULL;

    return m;
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_balance_trade_add(order_t *order, const char *asset, 
                mpd_t *change, mpd_t *price, mpd_t *amount)

PURPOSE: 
    将委单成交导致的余额增加，添加到balance_history_${user_id}
    
PARAMETERS:
    order  - 委单变量指针
    asset  - 增加资产币种
    change - 增加数量
    price  - 成交价格，用于添加detail
    amount - 成交数量，用于添加detail
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    撮合成交时，增加的资产调用该函数保存进历史数据库（异步）
    成交信息保存在detail    
---------------------------------------------------------------------------*/
static int append_balance_trade_add(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", change, detail_str);
    free(detail_str);
    json_decref(detail);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_balance_trade_sub(order_t *order, const char *asset, 
            mpd_t *change, mpd_t *price, mpd_t *amount)

PURPOSE: 
    将委单成交导致的余额减少，添加到balance_history_${user_id}
    
PARAMETERS:
    order  - 委单变量指针
    asset  - 减少资产币种
    change - 减少数量
    price  - 成交价格，用于添加detail
    amount - 成交数量，用于添加detail
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    撮合成交时，减少的资产调用该函数保存进历史数据库（异步）
    成交信息保存在detail    
---------------------------------------------------------------------------*/
static int append_balance_trade_sub(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_balance_trade_fee(order_t *order, const char *asset, 
                mpd_t *change, mpd_t *price, mpd_t *amount, mpd_t *fee_rate)

PURPOSE: 
    将委单成交的手续费，导致的余额减少，添加到balance_history_${user_id}
    
PARAMETERS:
    order  - 委单变量指针
    asset  - 减少资产币种
    change - 减少数量，即手续费
    price  - 成交价格，用于添加detail
    amount - 成交数量，用于添加detail
    fee_rate - 手续费比例，用于添加detail
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    撮合成交时，手续费导致的减少的资产，调用该函数保存进历史数据库（异步）
    成交信息保存在detail    
---------------------------------------------------------------------------*/
static int append_balance_trade_fee(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount, mpd_t *fee_rate)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    json_object_set_new_mpd(detail, "f", fee_rate);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int execute_limit_ask_order(bool real, market_t *m, order_t *taker)

PURPOSE: 
    执行买单撮合，查找bids卖方队列，撮合可以成交的对手单。
    撮合成功，则计算成交价格/数量/费用，调整买卖双方委单状态、资产余额，
    如果完全成交，会调用接口，添加order/balance/deal历史记录，并向kafka发送orders/deals消息
    如果部分成交，添加到买方队列，添加balance/deal历史记录，并向kafka发送orders/deals消息
    如果未成交，添加到asks卖方队列，并向kafka发送orders/deals消息

PARAMETERS:
    real  - 是否真实操作，默认为true
    m     - market变量指针
    taker - 吃单变量（限价卖单）
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_limit_ask_order(real, m, order);
    } else {
        ret = execute_limit_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

REMARKS: 
    收到order.putlimit命令之后，调用该函数在对方队列进行撮合
---------------------------------------------------------------------------*/
static int execute_limit_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) > 0) {
            break;
        }

        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, taker, maker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, m->money, ask_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, m->stock, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int execute_limit_bid_order(bool real, market_t *m, order_t *taker)

PURPOSE: 
    执行买单撮合，查找asks卖方队列，撮合可以成交的对手单。
    撮合成功，则计算成交价格/数量/费用，调整买卖双方委单状态、资产余额，
    如果完全成交，会调用接口，添加order/balance/deal历史记录，并向kafka发送orders/deals消息
    如果部分成交，添加到买方队列，添加balance/deal历史记录，并向kafka发送orders/deals消息
    如果未成交，添加到bids买方队列，并向kafka发送orders/deals消息

PARAMETERS:
    real  - 是否真实操作，默认为true
    m     - market变量指针
    taker - 吃单变量（限价买单）
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_limit_ask_order(real, m, order);
    } else {
        ret = execute_limit_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

REMARKS: 
    收到order.putlimit命令之后，调用该函数在对方队列进行撮合
---------------------------------------------------------------------------*/
static int execute_limit_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) < 0) {
            break;
        }

        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, maker, taker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, m->stock, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, m->money, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int market_put_limit_order(bool real, json_t **result, market_t *m, 
    uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price, 
    mpd_t *taker_fee, mpd_t *maker_fee, const char *source)

PURPOSE: 
    根据传参生成限价单，并执行撮合
    如果完全成交，保存order/balance/deal历史，并发送orders到kafka
    如果未完全成交，添加到买卖队列，保存balance/deal历史，并发送orders/deals到kafka

PARAMETERS:
    real      - 是否真实操作，默认为true
    result    - 处理完成的order的json结构
    m         - 货币对market
    user_id   - 
    side      - 买卖方向
    amount    - 委单金额
    price     - 委单价格
    taker_fee - 吃单手续费
    maker_fee - 做市商手续费
    source    - 来源字符串
    
RETURN VALUE: 
    >=0，成功下达委单
    =-1，可用余额不足
    =-2，下单数量太少
    <-2，发生错误的行号

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    json_t *result = NULL;
    int ret = market_put_limit_order(true, &result, market, user_id, side, amount, price, taker_fee, maker_fee, source);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

REMARKS: 
    收到order.putlimit命令之后，调用该函数执行委单
---------------------------------------------------------------------------*/
int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price, mpd_t *taker_fee, mpd_t *maker_fee, const char *source)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, amount, price, &mpd_ctx);
        if (!balance || mpd_cmp(balance, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -1;
        }
        mpd_del(require);
    }

    if (mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
        return -2;
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, maker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_limit_ask_order(real, m, order);
    } else {
        ret = execute_limit_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (mpd_cmp(order->left, mpd_zero, &mpd_ctx) == 0) {
        if (real) {
            ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
            push_order_message(ORDER_EVENT_FINISH, order, m);
            *result = get_order_info(order);
        }
        order_free(order);
    } else {
        if (real) {
            push_order_message(ORDER_EVENT_PUT, order, m);
            *result = get_order_info(order);
        }
        ret = order_put(m, order);
        if (ret < 0) {
            log_fatal("order_put fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int execute_market_ask_order(bool real, market_t *m, order_t *taker)

PURPOSE: 
    执行市价卖单撮合
    查找bids买方队列，撮合可以成交的买单，计算成交价格/数量/费用，
    调整买卖双方委单状态、资产余额，添加balance/deal/order历史记录，
    并向kafka发送orders/deals消息

PARAMETERS:
    real  - 是否真实操作，默认为true
    m     - market变量指针
    taker - 吃单变量（市价卖单）
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
    } else {
        ret = execute_market_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

REMARKS: 
    收到order.putmarket命令之后，调用该函数在对方队列进行撮合
    如果吃光对手盘，那么市价单也不会添加到买卖队列，而是以成交数量关闭委单
---------------------------------------------------------------------------*/
static int execute_market_ask_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, taker, maker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, m->money, ask_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, m->stock, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int execute_market_bid_order(bool real, market_t *m, order_t *taker)

PURPOSE: 
    执行市价买单撮合
    查找asks卖方队列，撮合可以成交的对手单，并计算成交价格/数量/费用，
    调整买卖双方委单状态、资产余额，添加balance/deal/order历史记录，
    并向kafka发送orders/deals消息

PARAMETERS:
    real  - 是否真实操作，默认为true
    m     - market变量指针
    taker - 吃单变量（市价买单）
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
    } else {
        ret = execute_market_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

REMARKS: 
    收到order.putmarket命令之后，调用该函数在对方队列进行撮合
    如果吃光对手盘，那么市价单也不会添加到买卖队列，而是以成交数量关闭委单
---------------------------------------------------------------------------*/
static int execute_market_bid_order(bool real, market_t *m, order_t *taker)
{
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);

        mpd_div(amount, taker->left, price, &mpd_ctx);
        mpd_rescale(amount, amount, -m->stock_prec, &mpd_ctx);
        while (true) {
            mpd_mul(result, amount, price, &mpd_ctx);
            if (mpd_cmp(result, taker->left, &mpd_ctx) > 0) {
                mpd_set_i32(result, -m->stock_prec, &mpd_ctx);
                mpd_pow(result, mpd_ten, result, &mpd_ctx);
                mpd_sub(amount, amount, result, &mpd_ctx);
            } else {
                break;
            }
        }

        if (mpd_cmp(amount, maker->left, &mpd_ctx) > 0) {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }
        if (mpd_cmp(amount, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, maker, taker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, deal, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, m->stock, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, m->money, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int market_put_market_order(bool real, json_t **result, market_t *m, 
                uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *taker_fee,
                const char *source)


PURPOSE: 
    根据传参生成市价单，并执行撮合，保存委单/余额/成交历史，并发送orders/deals到kafka

PARAMETERS:
    real      - 是否真实操作，默认为true
    result    - 处理完成的order的json结构
    m         - 货币对market
    user_id   - 
    side      - 买卖方向
    amount    - 委单金额
    taker_fee - 吃单手续费
    source    - 来源字符串
    
RETURN VALUE: 
    >=0，成功下达委单
    =-1，可用余额不足
    =-2，下单数量太少
    =-3，没有对手盘
    <-3，发生错误的行号

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    json_t *result = NULL;
    int ret = market_put_market_order(true, &result, market, user_id, side, amount, taker_fee, source);
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

REMARKS: 
    收到order.putmarket命令之后，调用该函数执行委单
    如果吃光对手盘，那么市价单也不会添加到买卖队列，而是以成交数量关闭委单
---------------------------------------------------------------------------*/
int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *taker_fee, const char *source)
{
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        skiplist_iter *iter = skiplist_get_iterator(m->bids);
        skiplist_node *node = skiplist_next(iter);
        if (node == NULL) {
            skiplist_release_iterator(iter);
            return -3;
        }
        skiplist_release_iterator(iter);

        if (mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
            return -2;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        skiplist_iter *iter = skiplist_get_iterator(m->asks);
        skiplist_node *node = skiplist_next(iter);
        if (node == NULL) {
            skiplist_release_iterator(iter);
            return -3;
        }
        skiplist_release_iterator(iter);

        order_t *order = node->value;
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, order->price, m->min_amount, &mpd_ctx);
        if (mpd_cmp(amount, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -2;
        }
        mpd_del(require);
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_MARKET;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->price, mpd_zero, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
    } else {
        ret = execute_market_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (real) {
        int ret = append_order_history(order);
        if (ret < 0) {
            log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info(order);
    }

    order_free(order);
    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)

PURPOSE: 
    撤销委单
    调用接口，发送orders消息到kafka
    调用接口，从买卖队列中删除委单，解冻资产，写入委单历史
    
PARAMETERS:
    real   - 是否执行
    result - 委单转换的json
    m      - 货币对
    order  - 委单
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    收到order.cancel命令时调用
---------------------------------------------------------------------------*/
int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)
{
    if (real) {
        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info(order);
    }
    order_finish(real, m, order);
    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int market_put_order(market_t *m, order_t *order)

PURPOSE: 
    把委单插入market的买卖队列，并冻结未成交部分的资产额度

PARAMETERS:
    m - 
    order - 

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    启动时，从slice_order_{time}恢复时调用该函数。
    order_put()中有冻结资产的操作，这个应该是失败的，
    调用market_put_order()的load_orders()没有处理返回值
---------------------------------------------------------------------------*/
int market_put_order(market_t *m, order_t *order)
{
    return order_put(m, order);
}

/*---------------------------------------------------------------------------
FUNCTION: order_t *market_get_order(market_t *m, uint64_t order_id)

PURPOSE: 
    从market的委单表中查找委单
    
PARAMETERS:
    m - 货币对
    order_id - 查找的委单id
    
RETURN VALUE: 
    如果查找成功，返回委单对象指针，否则返回 NULL

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }

REMARKS: 
    从market_t->orders中查找，该结构存储交易中的委单
---------------------------------------------------------------------------*/
order_t *market_get_order(market_t *m, uint64_t order_id)
{
    struct dict_order_key key = { .order_id = order_id };
    dict_entry *entry = dict_find(m->orders, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

/*---------------------------------------------------------------------------
FUNCTION: skiplist_t *market_get_order_list(market_t *m, uint32_t user_id)

PURPOSE: 
    从market的委单表中查找该用户的所有委单
    
PARAMETERS:
    m - 货币对市场
    user_id - 查找的user id
    
RETURN VALUE: 
    如果查找成功，返回委单列表指针，否则返回 NULL

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }

REMARKS: 
    收到order.query命令后调用
    从market_t->users中查找，该结构存储每个账户的交易中的委单列表
---------------------------------------------------------------------------*/
skiplist_t *market_get_order_list(market_t *m, uint32_t user_id)
{
    struct dict_user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(m->users, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

/*---------------------------------------------------------------------------
FUNCTION: int market_get_status(market_t *m, size_t *ask_count, mpd_t *ask_amount, 
            size_t *bid_count, mpd_t *bid_amount)

PURPOSE: 
    统计货币对交易中的单据数量、交易量
    
PARAMETERS:
    m -
    ask_count  - 卖单单据数量
    ask_amount - 卖单交易数量
    bid_count  - 买单单据数量
    bid_amount - 买单交易数量
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    收到market.summary时调用
---------------------------------------------------------------------------*/
int market_get_status(market_t *m, size_t *ask_count, mpd_t *ask_amount, size_t *bid_count, mpd_t *bid_amount)
{
    *ask_count = m->asks->len;
    *bid_count = m->bids->len;
    mpd_copy(ask_amount, mpd_zero, &mpd_ctx);
    mpd_copy(bid_amount, mpd_zero, &mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(ask_amount, ask_amount, order->left, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(bid_amount, bid_amount, order->left, &mpd_ctx);
    }

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: sds market_status(sds reply)

PURPOSE: 
    查询最近提交的委单id、最近的成交单id
    
PARAMETERS:
    reply - 查询结果附加到该字符串尾部
    
RETURN VALUE: 
    拼接之后的字符串

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    cli 收到命令 status 时调用      
---------------------------------------------------------------------------*/
sds market_status(sds reply)
{
    reply = sdscatprintf(reply, "order last ID: %"PRIu64"\n", order_id_start);
    reply = sdscatprintf(reply, "deals last ID: %"PRIu64"\n", deals_id_start);
    return reply;
}

