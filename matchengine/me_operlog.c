/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/01, create
 */

# include "me_config.h"
# include "me_operlog.h"

/*---------------------------------------------------------------------------
VARIABLE: uint64_t operlog_id_start;

PURPOSE: 
    最后一次的操作日志id，新的操作日志使用++operlog_id_start作为id

REMARKS: 
    只有写操作会产生操作日志
    balance.update
    order.put_limit
    order.put_market
    order.cancel
---------------------------------------------------------------------------*/
uint64_t operlog_id_start;

/*---------------------------------------------------------------------------
VARIABLE: static MYSQL *mysql_conn;

PURPOSE: 
    mysql.operlog_{time}数据连接
    
REMARKS:     
---------------------------------------------------------------------------*/
static MYSQL *mysql_conn;

/*---------------------------------------------------------------------------
VARIABLE: static nw_job *job;

PURPOSE: 
    写operlog_{day}线程
    
REMARKS: 
    本模块只创建了一个工作线程，这样能够保证sql的执行顺序
    将sql写入数据库，只有重复id错误或写入成功，才会结束本次job，否则会一直尝试写入
---------------------------------------------------------------------------*/
static nw_job *job;

/*---------------------------------------------------------------------------
VARIABLE: static list_t *list;

PURPOSE: 
    sql缓存列表
    
REMARKS: 
    等待写入的日志以struct operlog结构暂时放入该list，等待timer检查到之后，
    转换为sql，并发送到job执行    
---------------------------------------------------------------------------*/
static list_t *list;

/*---------------------------------------------------------------------------
VARIABLE: static nw_timer timer;

PURPOSE: 
    operlog写入定时器，定时检查list缓存，并发送到job执行
    
REMARKS:
    0.1s执行一次
---------------------------------------------------------------------------*/
static nw_timer timer;

/*---------------------------------------------------------------------------
STRUCT: struct operlog

PURPOSE: 
    缓存结构

REMARKS:
    struct变量，通过append_operlog()存储在list中，并在flush_log()中封装为sql
---------------------------------------------------------------------------*/
struct operlog {
    uint64_t id;        // 数据库id
    double create_time; // 时间戳，整数部分为秒，小数部分精确到微秒，小数点后6位
    char *detail;       // 日志内容，格式如{"method":"","params":{}}
};

static void *on_job_init(void)
{
    return mysql_connect(&settings.db_log);
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_job(nw_job_entry *entry, void *privdata)

PURPOSE: 
    job线程回调函数，负责收到数据后，进行执行。
    执行trade_log.operlog_{day}的写入sql。

PARAMETERS:
    entry - job数据
    privdata - mysql链接

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    写入成功，或者发现从夫id错误，否则一直挂起执行
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

static void on_job_cleanup(nw_job_entry *entry)
{
    sdsfree(entry->request);
}

static void on_job_release(void *privdata)
{
    mysql_close(privdata);
}

static void on_list_free(void *value)
{
    struct operlog *log = value;
    free(log->detail);
    free(log);
}

/*---------------------------------------------------------------------------
FUNCTION: static void flush_log(void)

PURPOSE: 
    将list中的数据转换为sql，并发送到job线程执行。

PARAMETERS:
    None

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    如果没有数据表trade_log.operlog_{day}存在，需要创建
    将缓存中的数据整理成一条sql，以便高效执行
    函数中有一处使用buf[10240]，用于暂存传入的操作参数(struct operlog->detail)，
    一般操作命令都比较短，不会超过缓存
---------------------------------------------------------------------------*/
static void flush_log(void)
{
    static sds table_last;
    if (table_last == NULL) {
        table_last = sdsempty();
    }

    time_t now = time(NULL);
    struct tm *tm = localtime(&now);
    sds table = sdsempty();
    table = sdscatprintf(table, "operlog_%04d%02d%02d", 1900 + tm->tm_year, 1 + tm->tm_mon, tm->tm_mday);

    if (sdscmp(table_last, table) != 0) {
        sds create_table_sql = sdsempty();
        create_table_sql = sdscatprintf(create_table_sql, "CREATE TABLE IF NOT EXISTS `%s` like `operlog_example`", table);
        nw_job_add(job, 0, create_table_sql);
        table_last = sdscpy(table_last, table);
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `time`, `detail`) VALUES ", table);
    sdsfree(table);

    size_t count;
    char buf[10240];
    list_node *node;
    list_iter *iter = list_get_iterator(list, LIST_START_HEAD);
    while ((node = list_next(iter)) != NULL) {
        struct operlog *log = node->value;
        size_t detail_len = strlen(log->detail);
        mysql_real_escape_string(mysql_conn, buf, log->detail, detail_len);
        sql = sdscatprintf(sql, "(%"PRIu64", %f, '%s')", log->id, log->create_time, buf);
        if (list_len(list) > 1) {
            sql = sdscatprintf(sql, ", ");
        }
        list_del(list, node);
        count++;
    }
    list_release_iterator(iter);
    nw_job_add(job, 0, sql);
    log_debug("flush oper log count: %zu", count);
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_timer(nw_timer *t, void *privdata)

PURPOSE: 
    写operlog_{day}定时器函数，负责调用接口，将list缓存写入job线程

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
    0.1秒执行一次，保证及时写入操作
---------------------------------------------------------------------------*/
static void on_timer(nw_timer *t, void *privdata)
{
    if (list->len > 0) {
        flush_log();
    }
}

/*---------------------------------------------------------------------------
FUNCTION: int init_operlog(void)

PURPOSE: 
    初始化operlog_{day}存储功能
    创建mysql链接，初始化缓存结构与job线程池，并启动定时器检查操作日志

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
int init_operlog(void)
{
    mysql_conn = mysql_init(NULL);
    if (mysql_conn == NULL)
        return -__LINE__;
    if (mysql_options(mysql_conn, MYSQL_SET_CHARSET_NAME, settings.db_log.charset) != 0)
        return -__LINE__;

    nw_job_type type;
    memset(&type, 0, sizeof(type));
    type.on_init    = on_job_init;
    type.on_job     = on_job;
    type.on_cleanup = on_job_cleanup;
    type.on_release = on_job_release;

    job = nw_job_create(&type, 1);
    if (job == NULL)
        return -__LINE__;

    list_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.free = on_list_free;
    list = list_create(&lt);
    if (list == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

int fini_operlog(void)
{
    on_timer(NULL, NULL);

    usleep(100 * 1000);
    nw_job_release(job);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int append_operlog(const char *method, json_t *params)

PURPOSE: 
    操作日志写到缓存

PARAMETERS:
    method - 操作类型，如update_balance/limit_order/market_order/cancel_order
    params - 操作参数

RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:     
---------------------------------------------------------------------------*/
int append_operlog(const char *method, json_t *params)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "method", json_string(method));
    json_object_set(detail, "params", params);
    struct operlog *log = malloc(sizeof(struct operlog));
    log->id = ++operlog_id_start;
    log->create_time = current_timestamp();
    log->detail = json_dumps(detail, JSON_SORT_KEYS);
    json_decref(detail);
    list_add_node_tail(list, log);
    log_debug("add log: %s", log->detail);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: bool is_operlog_block(void)

PURPOSE: 
    检查操作日志job线程是否任务过多
    如果任务过多会暂停日志命令的处理

PARAMETERS:
    None

RETURN VALUE: 
    任务超出最大指标，返回true，否则返回false

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    MAX_PENDING_OPERLOG 默认 100
---------------------------------------------------------------------------*/
bool is_operlog_block(void)
{
    if (job->request_count >= MAX_PENDING_OPERLOG)
        return true;
    return false;
}

/*---------------------------------------------------------------------------
FUNCTION: sds operlog_status(sds reply)

PURPOSE: 
    读取操作日志的最近id，以及挂起job数量

PARAMETERS:
    reply - 要附加到的原始字符串

RETURN VALUE: 
    附加日志信息后的字符串

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    cli命令status调用
---------------------------------------------------------------------------*/
sds operlog_status(sds reply)
{
    reply = sdscatprintf(reply, "operlog last ID: %"PRIu64"\n", operlog_id_start);
    reply = sdscatprintf(reply, "operlog pending: %d\n", job->request_count);
    return reply;
}

