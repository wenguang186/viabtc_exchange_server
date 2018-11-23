/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/08, create
 */

# include "me_config.h"
# include "me_message.h"

# include <librdkafka/rdkafka.h>

/*---------------------------------------------------------------------------
VARIABLE: static rd_kafka_t *rk;

PURPOSE: 
    kafka生产者变量

REMARKS: 
    注册kafka生产者，生成消息发送给kafka框架
---------------------------------------------------------------------------*/
static rd_kafka_t *rk;

/*---------------------------------------------------------------------------
VARIABLE: static rd_kafka_topic_t *rkt_deals;

PURPOSE: 
    kafka 消息类型 deals

REMARKS: 
    rkt_orders 对应 orders， rkt_balances 对应 balances
---------------------------------------------------------------------------*/
static rd_kafka_topic_t *rkt_deals;
static rd_kafka_topic_t *rkt_orders;
static rd_kafka_topic_t *rkt_balances;

/*---------------------------------------------------------------------------
VARIABLE: static list_t *list_deals;

PURPOSE: 
    将要发送给 kafka 的 deals 消息缓存列表

REMARKS: 
    list_orders 对应 orders 缓存， list_balances 对应 balances 缓存
---------------------------------------------------------------------------*/
static list_t *list_deals;
static list_t *list_orders;
static list_t *list_balances;

/*---------------------------------------------------------------------------
VARIABLE: static nw_timer timer;

PURPOSE: 
    缓存执行定时器
    负责定时把list_deals/orders/balances 发送到 rkt_deals/orders/balances
    并调用kafka处理消息

REMARKS: 
    0.1s执行一次
---------------------------------------------------------------------------*/
static nw_timer timer;

/*---------------------------------------------------------------------------
FUNCTION: static void on_delivery(rd_kafka_t *rk, 
                const rd_kafka_message_t *rkmessage, void *opaque)

PURPOSE: 
    kafka 回调函数，发送消息时调用
    
PARAMETERS:
    rk - kafka 生产者
    rkmessage - 发送的消息状态
    opaque - 
    
RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    输出执行日志，用于检查kafka执行是否正常
---------------------------------------------------------------------------*/
static void on_delivery(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    if (rkmessage->err) {
        log_fatal("Message delivery failed: %s", rd_kafka_err2str(rkmessage->err));
    } else {
        log_trace("Message delivered (topic: %s, %zd bytes, partition %"PRId32")",
                rd_kafka_topic_name(rkmessage->rkt), rkmessage->len, rkmessage->partition);
    }
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_logger(const rd_kafka_t *rk, int level,
             const char *fac, const char *buf)


PURPOSE: 
    kafka 回调函数，kafka生成日志时调用，供生产者跟踪执行过程
    
PARAMETERS:
    rk - kafka 生产者
    level - 发送的消息状态
    fac - 
    buf - 日志内容
    
RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    在本地输出kafka执行日志，以便于检查问题
---------------------------------------------------------------------------*/
static void on_logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
    log_error("RDKAFKA-%i-%s: %s: %s\n", level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

/*---------------------------------------------------------------------------
FUNCTION: static void produce_list(list_t *list, rd_kafka_topic_t *topic)

PURPOSE: 
    把消息列表中的数据，发送到kafka
    
PARAMETERS:
    list - 消息缓存列表
    topic - kafka消息类型
    
RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    if (list_balances->len) {
        produce_list(list_balances, rkt_balances);
    }

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static void produce_list(list_t *list, rd_kafka_topic_t *topic)
{
    list_node *node;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        int ret = rd_kafka_produce(topic, 0, RD_KAFKA_MSG_F_COPY, node->value, strlen(node->value), NULL, 0, NULL);
        if (ret == -1) {
            log_fatal("Failed to produce: %s to topic %s: %s\n", (char *)node->value,
                    rd_kafka_topic_name(rkt_deals), rd_kafka_err2str(rd_kafka_last_error()));
            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                break;
            }
        }
        list_del(list, node);
    }
    list_release_iterator(iter);
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_timer(nw_timer *t, void *privdata)

PURPOSE: 
    定时器回调函数
    定时把消息缓存，写到kafka
    
PARAMETERS:
    t - 
    privdata - 
    
RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static void on_timer(nw_timer *t, void *privdata)
{
    if (list_balances->len) {
        produce_list(list_balances, rkt_balances);
    }
    if (list_orders->len) {
        produce_list(list_orders, rkt_orders);
    }
    if (list_deals->len) {
        produce_list(list_deals, rkt_deals);
    }

    rd_kafka_poll(rk, 0);
}

static void on_list_free(void *value)
{
    free(value);
}

/*---------------------------------------------------------------------------
FUNCTION: int init_message(void)

PURPOSE: 
    初始化消息处理机制
    初始化kafka框架及消息结构，启动消息处理定时器
    
PARAMETERS:
    None
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
int init_message(void)
{
    char errstr[1024];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", settings.brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        log_stderr("Set kafka brokers: %s fail: %s", settings.brokers, errstr);
        return -__LINE__;
    }
    if (rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        log_stderr("Set kafka buffering: %s fail: %s", settings.brokers, errstr);
        return -__LINE__;
    }
    rd_kafka_conf_set_log_cb(conf, on_logger);
    rd_kafka_conf_set_dr_msg_cb(conf, on_delivery);

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (rk == NULL) {
        log_stderr("Failed to create new producer: %s", errstr);
        return -__LINE__;
    }

    rkt_balances = rd_kafka_topic_new(rk, "balances", NULL);
    if (rkt_balances == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_orders = rd_kafka_topic_new(rk, "orders", NULL);
    if (rkt_orders == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }
    rkt_deals = rd_kafka_topic_new(rk, "deals", NULL);
    if (rkt_deals == NULL) {
        log_stderr("Failed to create topic object: %s", rd_kafka_err2str(rd_kafka_last_error()));
        return -__LINE__;
    }

    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.free = on_list_free;

    list_deals = list_create(&lt);
    if (list_deals == NULL)
        return -__LINE__;
    list_orders = list_create(&lt);
    if (list_orders == NULL)
        return -__LINE__;
    list_balances = list_create(&lt);
    if (list_balances == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int fini_message(void)
{
    on_timer(NULL, NULL);

    rd_kafka_flush(rk, 1000);
    rd_kafka_topic_destroy(rkt_balances);
    rd_kafka_topic_destroy(rkt_orders);
    rd_kafka_topic_destroy(rkt_deals);
    rd_kafka_destroy(rk);

    return 0;
}

static json_t *json_array_append_mpd(json_t *message, mpd_t *val)
{
    char *str = mpd_to_sci(val, 0);
    json_array_append_new(message, json_string(str));
    free(str);
    return message;
}

/*---------------------------------------------------------------------------
FUNCTION: static int push_message(char *message, rd_kafka_topic_t *topic, list_t *list)

PURPOSE: 
    推送消息到kafka
    
PARAMETERS:
    message - 消息内容 
    topic - 消息类型
    list - 消息缓存列表
    
RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    如果缓存中有排队消息，放到缓存末尾。
    如果没有消息，直接发送到kafka
---------------------------------------------------------------------------*/
static int push_message(char *message, rd_kafka_topic_t *topic, list_t *list)
{
    log_trace("push %s message: %s", rd_kafka_topic_name(topic), message);

    if (list->len) {
        list_add_node_tail(list, message);
        return 0;
    }

    int ret = rd_kafka_produce(topic, 0, RD_KAFKA_MSG_F_COPY, message, strlen(message), NULL, 0, NULL);
    if (ret == -1) {
        log_fatal("Failed to produce: %s to topic %s: %s\n", message, rd_kafka_topic_name(rkt_deals), rd_kafka_err2str(rd_kafka_last_error()));
        if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            list_add_node_tail(list, message);
            return 0;
        }
        free(message);
        return -__LINE__;
    }
    free(message);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int push_balance_message(double t, uint32_t user_id, const char *asset,
            const char *business, mpd_t *change)

PURPOSE: 
    发送balances消息到kafka
    
PARAMETERS:    
    t        - 时间
    user_id  - 账户
    asset    - 币种
    business - 事务类型(trade/deposit/withdraw)
    change   - 变动数量

RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    收到balance.update时调用
    trade类型变动不发送消息
---------------------------------------------------------------------------*/
int push_balance_message(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change)
{
    json_t *message = json_array();
    json_array_append_new(message, json_real(t));
    json_array_append_new(message, json_integer(user_id));
    json_array_append_new(message, json_string(asset));
    json_array_append_new(message, json_string(business));
    json_array_append_mpd(message, change);

    push_message(json_dumps(message, 0), rkt_balances, list_balances);
    json_decref(message);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int push_order_message(uint32_t event, order_t *order, market_t *market)

PURPOSE: 
    推送委单消息orders到kafka
    
PARAMETERS:
    event  - 委单状态类型(ORDER_EVENT_PUT/ORDER_EVENT_UPDATE/ORDER_EVENT_FINISH)
    order  - 委单结构
    market - 货币对
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    收到委单命令后，会生成委单消息
    order.put_limit/order.put_market/order.cancel
---------------------------------------------------------------------------*/
int push_order_message(uint32_t event, order_t *order, market_t *market)
{
    json_t *message = json_object();
    json_object_set_new(message, "event", json_integer(event));
    json_object_set_new(message, "order", get_order_info(order));
    json_object_set_new(message, "stock", json_string(market->stock));
    json_object_set_new(message, "money", json_string(market->money));

    push_message(json_dumps(message, 0), rkt_orders, list_orders);
    json_decref(message);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int push_deal_message(double t, const char *market, order_t *ask, order_t *bid, mpd_t *price, mpd_t *amount,
        mpd_t *ask_fee, mpd_t *bid_fee, int side, uint64_t id, const char *stock, const char *money)

PURPOSE: 
    撮合成交时，发送成交单deals到kafka

PARAMETERS:
    t       - 时间
    market  - 货币对
    ask     - 卖方委单
    bid     - 买方委单
    price   - 成交价格
    amount  - 成交数量
    ask_fee - 卖方手续费
    bid_fee - 买方手续费
    side    - 成交方向
    id      - 成交单id
    stock   - 基础币种
    money   - 计价币种
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
int push_deal_message(double t, const char *market, order_t *ask, order_t *bid, mpd_t *price, mpd_t *amount,
        mpd_t *ask_fee, mpd_t *bid_fee, int side, uint64_t id, const char *stock, const char *money)
{
    json_t *message = json_array();
    json_array_append_new(message, json_real(t));
    json_array_append_new(message, json_string(market));
    json_array_append_new(message, json_integer(ask->id));
    json_array_append_new(message, json_integer(bid->id));
    json_array_append_new(message, json_integer(ask->user_id));
    json_array_append_new(message, json_integer(bid->user_id));
    json_array_append_mpd(message, price);
    json_array_append_mpd(message, amount);
    json_array_append_mpd(message, ask_fee);
    json_array_append_mpd(message, bid_fee);
    json_array_append_new(message, json_integer(side));
    json_array_append_new(message, json_integer(id));
    json_array_append_new(message, json_string(stock));
    json_array_append_new(message, json_string(money));

    push_message(json_dumps(message, 0), rkt_deals, list_deals);
    json_decref(message);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: bool is_message_block(void)

PURPOSE: 
    检查是否kafka缓存消息过多
    
PARAMETERS:
    None
    
RETURN VALUE: 
    消息超过最大数量，返回true，没超过返回false

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    如果消息过多，那么matchengine将不再执行产生消息的命令
    balance.update order.put_limit order.put_market order.cancel
---------------------------------------------------------------------------*/
bool is_message_block(void)
{
    if (list_deals->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_orders->len >= MAX_PENDING_MESSAGE)
        return true;
    if (list_balances->len >= MAX_PENDING_MESSAGE)
        return true;

    return false;
}

/*---------------------------------------------------------------------------
FUNCTION: sds message_status(sds reply)

PURPOSE: 
    查询缓存的消息数量
    
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
sds message_status(sds reply)
{
    reply = sdscatprintf(reply, "message deals pending: %lu\n", list_deals->len);
    reply = sdscatprintf(reply, "message orders pending: %lu\n", list_orders->len);
    reply = sdscatprintf(reply, "message balances pending: %lu\n", list_balances->len);
    return reply;
}

