/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/06, create
 */

# include "me_config.h"
# include "me_history.h"
# include "me_balance.h"

/*---------------------------------------------------------------------------
VARIABLE: static MYSQL *mysql_conn;

PURPOSE: 
    MySQL数据链接对象，链接数据库trade_history

REMARKS: 
    init_hsitory()时初始化，执行sql时调用
---------------------------------------------------------------------------*/
static MYSQL *mysql_conn;

/*---------------------------------------------------------------------------
VARIABLE: static nw_job *job;

PURPOSE: 
    执行sql的工作队列

REMARKS: 
    由定时器将dict_sql写入job队列执行
---------------------------------------------------------------------------*/
static nw_job *job;

/*---------------------------------------------------------------------------
VARIABLE: static dict_t *dict_sql;

PURPOSE: 
    sql缓存结构

REMARKS: 
    需要执行的sql缓存到此处，由定时器将dict_sql写入job队列执行
    trade_history.order_history_$i
    trade_history.order_detail_$i
    trade_history.deal_history_$i
    trade_history.user_deal_history_$i
    trade_history.balance_history_$i
---------------------------------------------------------------------------*/
static dict_t *dict_sql;

/*---------------------------------------------------------------------------
VARIABLE: static nw_timer timer;

PURPOSE: 
    定时器，定时检查dict_sql中是否有未处理sql，添加到job中执行

REMARKS: 
    0.1s执行一次
---------------------------------------------------------------------------*/
static nw_timer timer;

enum {
    HISTORY_USER_BALANCE,
    HISTORY_USER_ORDER,
    HISTORY_USER_DEAL,
    HISTORY_ORDER_DETAIL,
    HISTORY_ORDER_DEAL,
};

struct dict_sql_key {
    uint32_t type;
    uint32_t hash;
};

/*---------------------------------------------------------------------------
FUNCTION: static uint32_t dict_sql_hash_function(const void *key)

PURPOSE: 
    自定义生成dict_sql的哈希值

PARAMETERS:
    key - dict_sql_key结构变量指针

RETURN VALUE: 
    hash value

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static uint32_t dict_sql_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_sql_key));
}

/*---------------------------------------------------------------------------
FUNCTION: static void *dict_sql_key_dup(const void *key)

PURPOSE: 
    复制dict_sql的key

PARAMETERS:
    key - dict_sql_key结构变量指针

RETURN VALUE: 
    复制key的新的变量指针

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    malloc得到的指针，使用完之后需要free，或使用dict_sql_key_free
---------------------------------------------------------------------------*/
static void *dict_sql_key_dup(const void *key)
{
    struct dict_sql_key *obj = malloc(sizeof(struct dict_sql_key));
    memcpy(obj, key, sizeof(struct dict_sql_key));
    return obj;
}

/*---------------------------------------------------------------------------
FUNCTION: static int dict_sql_key_compare(const void *key1, const void *key2)

PURPOSE: 
    比较两个key值大小

PARAMETERS:
    key1 - dict_sql_key结构变量指针
    key2 - dict_sql_key结构变量指针

RETURN VALUE: 
    <0, if key1 < key2
    =0, if key1 = key2
    >0, if key1 > key2

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    逐一比较key1、key2内存中的每个字节，
---------------------------------------------------------------------------*/
static int dict_sql_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct dict_sql_key));
}

/*---------------------------------------------------------------------------
FUNCTION: static void dict_sql_key_free(void *key)

PURPOSE: 
    释放key的堆空间

PARAMETERS:
    key - dict_sql_key结构变量指针

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
---------------------------------------------------------------------------*/
static void dict_sql_key_free(void *key)
{
    free(key);
}

/*---------------------------------------------------------------------------
FUNCTION: static void *on_job_init(void)

PURPOSE: 
    初始化job的数据库链接

PARAMETERS:
    key - dict_sql_key结构变量指针

RETURN VALUE: 
    MYSQL*链接对象指针

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    job线程结束时，在on_job_release()中自动释放
---------------------------------------------------------------------------*/
static void *on_job_init(void)
{
    return mysql_connect(&settings.db_history);
}


/*---------------------------------------------------------------------------
FUNCTION: static void on_job(nw_job_entry *entry, void *privdata)

PURPOSE: 
    job处理函数
    执行收到的sql

PARAMETERS:
    entry - 待处理数据
    privdata - MYSQL链接对象指针

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    如果执行错误，会通过log_fatal向alertcenter发送错误报告，并休眠1s后重复执行
    如果遇到id重复错误，会停止执行并跳出
    因为job有数量限制，如果出现多条sql fail，那么会导致job占满，无法执行新sql
---------------------------------------------------------------------------*/
static void on_job(nw_job_entry *entry, void *privdata)
{
    MYSQL *conn = privdata;
    sds sql = entry->request;
    log_trace("exec sql: %s", sql);
    while (true) {
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0 && mysql_errno(conn) != 1062) {
            log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            usleep(1000 * 1000);
            continue;
        }
        break;
    }
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_job_cleanup(nw_job_entry *entry)

PURPOSE: 
    job完成时，释放传入的堆变量

PARAMETERS:
    entry – job接收到的数据指针

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static void on_job_cleanup(nw_job_entry *entry)
{
    sdsfree(entry->request);
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_job_release(void *privdata)

PURPOSE: 
    job线程退出时，释放数据库连接

PARAMETERS:
    privdata – MYSQL指针

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_timer(nw_timer *t, void *privdata)

PURPOSE: 
    写入历史数据库的定时器函数
    把dict_sql中缓存的sql，逐一添加到job队列

PARAMETERS:
    t – 定时器对象指针
    privdata - MYSQL连接指针

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static void on_timer(nw_timer *t, void *privdata)
{
    size_t count = 0;
    dict_iterator *iter = dict_get_iterator(dict_sql);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        nw_job_add(job, 0, entry->val);
        dict_delete(dict_sql, entry->key);
        count++;
    }
    dict_release_iterator(iter);

    if (count) {
        log_debug("flush history count: %zu", count);
    }
}

/*---------------------------------------------------------------------------
FUNCTION: int init_history(void)

PURPOSE: 
    初始化历史数据存储模块
    创建数据库连接
    初始化sql缓存dict_sql
    初始化job队列
    启动sql检查定时器

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
int init_history(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_history.charset) != 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = dict_sql_hash_function;
    dt.key_compare    = dict_sql_key_compare;
    dt.key_dup        = dict_sql_key_dup;
    dt.key_destructor = dict_sql_key_free;

    dict_sql = dict_create(&dt, 1024);
    if (dict_sql == 0) {
        return -__LINE__;
    }

    nw_job_type jt;
    memset(&jt, 0, sizeof(jt));
    jt.on_init    = on_job_init;
    jt.on_job     = on_job;
    jt.on_cleanup = on_job_cleanup;
    jt.on_release = on_job_release;

    job = nw_job_create(&jt, settings.history_thread);
    if (job == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int fini_history(void)

PURPOSE: 
    程序结束时，关闭timer，释放job队列

PARAMETERS:
    None

RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
int fini_history(void)
{
    on_timer(NULL, NULL);

    usleep(100 * 1000);
    nw_job_release(job);

    return 0;
}

static sds sql_append_mpd(sds sql, mpd_t *val, bool comma)
{
    char *str = mpd_to_sci(val, 0);
    sql = sdscatprintf(sql, "'%s'", str);
    if (comma) {
        sql = sdscatprintf(sql, ", ");
    }
    free(str);
    return sql;
}

/*---------------------------------------------------------------------------
FUNCTION: static sds get_sql(struct dict_sql_key *key)

PURPOSE: 
    从dict_sql取出指定键值的sql字符串，如果没有添加空字符串并返回

PARAMETERS:
    key - dict_sql键值

RETURN VALUE: 
    sql字符串

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    struct dict_sql_key key;
    key.hash = order->user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_ORDER;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
    } else {
    }

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static sds get_sql(struct dict_sql_key *key)
{
    dict_entry *entry = dict_find(dict_sql, key);
    if (!entry) {
        sds val = sdsempty();
        entry = dict_add(dict_sql, key, val);
        if (entry == NULL) {
            sdsfree(val);
            return NULL;
        }
    }
    return entry->val;
}

/*---------------------------------------------------------------------------
FUNCTION: static void set_sql(struct dict_sql_key *key, sds sql)

PURPOSE: 
    向dict_sql中写入待执行sql

PARAMETERS:
    key - 键值
    sql - 待执行sql

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    struct dict_sql_key key;
    key.hash = order->user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_ORDER;
    set_sql(&key, sql);

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static void set_sql(struct dict_sql_key *key, sds sql)
{
    dict_entry *entry = dict_find(dict_sql, key);
    if (entry) {
        entry->val = sql;
    }
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_user_order(order_t *order)

PURPOSE: 
    生成委单历史sql，调用接口，添加到dict_sql，等待定时器调用写入order_history_$i

PARAMETERS:
    order – 委单结构指针

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    与append_order_detail()一起被调用
    只有委单关闭（成交或撤销）时，才会写入
---------------------------------------------------------------------------*/
static int append_user_order(order_t *order)
{
    struct dict_sql_key key;
    key.hash = order->user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_ORDER;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `order_history_%u` (`id`, `create_time`, `finish_time`, `user_id`, "
                "`market`, `source`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, `deal_stock`, `deal_money`, `deal_fee`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, '%s', '%s', %u, %u, ", order->id,
        order->create_time, order->update_time, order->user_id, order->market, order->source, order->type, order->side);
    sql = sql_append_mpd(sql, order->price, true);
    sql = sql_append_mpd(sql, order->amount, true);
    sql = sql_append_mpd(sql, order->taker_fee, true);
    sql = sql_append_mpd(sql, order->maker_fee, true);
    sql = sql_append_mpd(sql, order->deal_stock, true);
    sql = sql_append_mpd(sql, order->deal_money, true);
    sql = sql_append_mpd(sql, order->deal_fee, false);
    sql = sdscatprintf(sql, ")");

    set_sql(&key, sql);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_order_detail(order_t *order)

PURPOSE: 
    生成委单sql，调用接口，添加到dict_sql，等待定时器调用写入order_detail_$i

PARAMETERS:
    order – 委单结构指针

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    与append_user_order()一起被调用
    只有委单关闭（成交或撤销）时，才会写入
---------------------------------------------------------------------------*/
static int append_order_detail(order_t *order)
{
    struct dict_sql_key key;
    key.hash = order->id % HISTORY_HASH_NUM;
    key.type = HISTORY_ORDER_DETAIL;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `order_detail_%u` (`id`, `create_time`, `finish_time`, `user_id`, "
                "`market`, `source`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, `deal_stock`, `deal_money`, `deal_fee`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(%"PRIu64", %f, %f, %u, '%s', '%s', %u, %u, ", order->id,
        order->create_time, order->update_time, order->user_id, order->market, order->source, order->type, order->side);
    sql = sql_append_mpd(sql, order->price, true);
    sql = sql_append_mpd(sql, order->amount, true);
    sql = sql_append_mpd(sql, order->taker_fee, true);
    sql = sql_append_mpd(sql, order->maker_fee, true);
    sql = sql_append_mpd(sql, order->deal_stock, true);
    sql = sql_append_mpd(sql, order->deal_money, true);
    sql = sql_append_mpd(sql, order->deal_fee, false);
    sql = sdscatprintf(sql, ")");

    set_sql(&key, sql);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_order_deal(double t, uint32_t user_id, uint64_t deal_id, 
                uint64_t order_id, uint64_t deal_order_id, int role, 
                mpd_t *price, mpd_t *amount, mpd_t *deal, mpd_t *fee, mpd_t *deal_fee)

PURPOSE: 
    生成成交单sql，调用接口，添加到dict_sql，等待定时器调用写入deal_history_$i

PARAMETERS:
    t – 时间
    user_id - 委单账户id
    deal_id - 成交单id
    order_id - 成交单所属委单id
    deal_order_id - 对手的委单id
    role    - MARKET_ROLE_TAKER/MARKET_ROLE_MAKER
    price   - 成交价格
    amount  - 成交数量
    deal    - 成交总额
    fee     - 己方支付的手续费
    deal_fee - 对手支付的手续费

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    与append_user_deal()一起被调用
    发送成交之后写入，需要为asks/bids同时生成记录
    注意买卖手续费计量单位不一样，均是以所得计量
---------------------------------------------------------------------------*/
static int append_order_deal(double t, uint32_t user_id, uint64_t deal_id, uint64_t order_id, uint64_t deal_order_id, int role, mpd_t *price, mpd_t *amount, mpd_t *deal, mpd_t *fee, mpd_t *deal_fee)
{
    struct dict_sql_key key;
    key.hash = order_id % HISTORY_HASH_NUM;
    key.type = HISTORY_ORDER_DEAL;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `deal_history_%u` (`id`, `time`, `user_id`, `deal_id`, `order_id`, `deal_order_id`, `role`, `price`, `amount`, `deal`, `fee`, `deal_fee`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(NULL, %f, %u, %"PRIu64", %"PRIu64", %"PRIu64", %d, ", t, user_id, deal_id, order_id, deal_order_id, role);
    sql = sql_append_mpd(sql, price, true);
    sql = sql_append_mpd(sql, amount, true);
    sql = sql_append_mpd(sql, deal, true);
    sql = sql_append_mpd(sql, fee, true);
    sql = sql_append_mpd(sql, deal_fee, false);
    sql = sdscatprintf(sql, ")");

    set_sql(&key, sql);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_user_deal(double t, uint32_t user_id, uint64_t deal_id, 
                uint64_t order_id, uint64_t deal_order_id, int role, 
                mpd_t *price, mpd_t *amount, mpd_t *deal, mpd_t *fee, mpd_t *deal_fee)

PURPOSE: 
    生成成交单sql，调用接口，添加到dict_sql，等待定时器调用写入user_deal_history_$i

PARAMETERS:
    t – 时间
    user_id - 委单账户id
    deal_id - 成交单id
    order_id - 成交单所属委单id
    deal_order_id - 对手的委单id
    role    - MARKET_ROLE_TAKER/MARKET_ROLE_MAKER
    price   - 成交价格
    amount  - 成交数量
    deal    - 成交总额
    fee     - 己方支付的手续费
    deal_fee - 对手支付的手续费

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    与append_order_deal()一起被调用
    发生成交之后调用，需要为asks/bids同时生成记录
    注意买卖手续费计量单位不一样，均是以所得计量
---------------------------------------------------------------------------*/
static int append_user_deal(double t, uint32_t user_id, const char *market, uint64_t deal_id, uint64_t order_id, uint64_t deal_order_id, int side, int role, mpd_t *price, mpd_t *amount, mpd_t *deal, mpd_t *fee, mpd_t *deal_fee)
{
    struct dict_sql_key key;
    key.hash = user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_DEAL;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `user_deal_history_%u` (`id`, `time`, `user_id`, `market`, `deal_id`, `order_id`, `deal_order_id`, `side`, `role`, `price`, `amount`, `deal`, `fee`, `deal_fee`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    sql = sdscatprintf(sql, "(NULL, %f, %u, '%s', %"PRIu64", %"PRIu64", %"PRIu64", %d, %d, ", t, user_id, market, deal_id, order_id, deal_order_id, side, role);
    sql = sql_append_mpd(sql, price, true);
    sql = sql_append_mpd(sql, amount, true);
    sql = sql_append_mpd(sql, deal, true);
    sql = sql_append_mpd(sql, fee, true);
    sql = sql_append_mpd(sql, deal_fee, false);
    sql = sdscatprintf(sql, ")");

    set_sql(&key, sql);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int append_user_balance(double t, uint32_t user_id, const char *asset,
                const char *business, mpd_t *change, mpd_t *balance, const char *detail)

PURPOSE: 
    生成余额变动sql，调用接口，添加到dict_sql，等待定时器调用写入balance_history_$i

PARAMETERS:
    t - 时间
    user_id - 帐户id
    asset   - 币种
    business - 事务类型，成交固定为trade，充提币建议为deposit/withdraw，可以为任意值，由命令参数决定
    change  - 变动金额
    balance - 剩余余额
    detail  - 传入参数中的detail，成交单为固定格式

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static int append_user_balance(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change, mpd_t *balance, const char *detail)
{
    struct dict_sql_key key;
    key.hash = user_id % HISTORY_HASH_NUM;
    key.type = HISTORY_USER_BALANCE;
    sds sql = get_sql(&key);
    if (sql == NULL)
        return -__LINE__;

    if (sdslen(sql) == 0) {
        sql = sdscatprintf(sql, "INSERT INTO `balance_history_%u` (`id`, `time`, `user_id`, `asset`, `business`, `change`, `balance`, `detail`) VALUES ", key.hash);
    } else {
        sql = sdscatprintf(sql, ", ");
    }

    char buf[10 * 1024];
    sql = sdscatprintf(sql, "(NULL, %f, %u, '%s', '%s', ", t, user_id, asset, business);
    sql = sql_append_mpd(sql, change, true);
    sql = sql_append_mpd(sql, balance, true);
    mysql_real_escape_string(mysql_conn, buf, detail, strlen(detail));
    sql = sdscatprintf(sql, "'%s')", buf);

    set_sql(&key, sql);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int append_order_history(order_t *order)

PURPOSE: 
    调用接口保存委单到数据库

PARAMETERS:
    order – 委单结构指针

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    最终由定时器/job线程池异步写入数据库
    只有委单关闭（成交或撤销）时，才会调用
    同时写入order_history_$(user_id) 与 order_detail_$(order_id)
---------------------------------------------------------------------------*/
int append_order_history(order_t *order)
{
    append_user_order(order);
    append_order_detail(order);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int append_order_deal_history(double t, uint64_t deal_id, order_t *ask, 
        int ask_role, order_t *bid, int bid_role, mpd_t *price, mpd_t *amount, 
        mpd_t *deal, mpd_t *ask_fee, mpd_t *bid_fee)

PURPOSE: 
    撮合成功时，保存成交单到数据库

PARAMETERS:
    t        - 时间
    deal_id  - 成交单id
    ask      - 卖方委单对象指针
    ask_role - 卖方角色 MARKET_ROLE_TAKER/MARKET_ROLE_MAKER
    bid      - 买方委单对象指针
    bid_role - 买方角色 MARKET_ROLE_TAKER/MARKET_ROLE_MAKER
    price    - 成交价格
    amount   - 成交数量
    deal     - 成交总额
    ask_fee  - 卖方手续费
    bid_fee  - 买方手续费

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    需要同时为买卖双方添加成交记录
    最终由定时器/job线程池异步写入数据库
    同时写入 deal_history_$(order_id) 与 user_deal_history_$(user_id)
---------------------------------------------------------------------------*/
int append_order_deal_history(double t, uint64_t deal_id, order_t *ask, int ask_role, order_t *bid, int bid_role, mpd_t *price, mpd_t *amount, mpd_t *deal, mpd_t *ask_fee, mpd_t *bid_fee)
{
    append_order_deal(t, ask->user_id, deal_id, ask->id, bid->id, ask_role, price, amount, deal, ask_fee, bid_fee);
    append_order_deal(t, bid->user_id, deal_id, bid->id, ask->id, bid_role, price, amount, deal, bid_fee, ask_fee);

    append_user_deal(t, ask->user_id, ask->market, deal_id, ask->id, bid->id, ask->side, ask_role, price, amount, deal, ask_fee, bid_fee);
    append_user_deal(t, bid->user_id, ask->market, deal_id, bid->id, ask->id, bid->side, bid_role, price, amount, deal, bid_fee, ask_fee);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int append_user_balance_history(double t, uint32_t user_id, const char *asset, 
                const char *business, mpd_t *change, const char *detail)


PURPOSE: 
    生成余额变动sql，调用接口，添加到dict_sql，等待定时器调用写入balance_history_$i

PARAMETERS:
    t - 时间
    user_id - 帐户id
    asset   - 币种
    business - 事务类型，成交固定为trade，充提币建议为deposit/withdraw，可以为任意值，由命令参数决定
    change  - 变动金额
    detail  - 传入参数中的detail，成交单为固定格式

RETURN VALUE: 
    Zero

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    撮合成交，或收到balance.update命令时调用
    撮合成交会产生两条历史记录，一条交易变动，一条扣费变动
    疑问：因为写入sql是异步的，所以balance_total()应该和change不是同步产生，可能存在不一致问题
---------------------------------------------------------------------------*/
int append_user_balance_history(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change, const char *detail)
{
    mpd_t *balance = balance_total(user_id, asset);
    append_user_balance(t, user_id, asset, business, change, balance, detail);
    mpd_del(balance);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: bool is_history_block(void)

PURPOSE: 
    检查写数据库的job是否堵塞了

PARAMETERS:
    <Parameter name> –<Parameter description>

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    如果挂起的数据库写操作超过1000，暂时不接受委单命令，并发送到alertcenter
---------------------------------------------------------------------------*/
bool is_history_block(void)
{
    if (job->request_count >= MAX_PENDING_HISTORY) {
        return true;
    }
    return false;
}

/*---------------------------------------------------------------------------
FUNCTION: sds history_status(sds reply)

PURPOSE: 
    获取等待执行写历史数据库的sql数量

PARAMETERS:
    <Parameter name> –<Parameter description>

RETURN VALUE: 
    <Description of function return value>

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    cli status命令调用
---------------------------------------------------------------------------*/
sds history_status(sds reply)
{
    return sdscatprintf(reply, "history pending %d\n", job->request_count);
}

