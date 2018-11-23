/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include "me_config.h"
# include "me_persist.h"
# include "me_operlog.h"
# include "me_market.h"
# include "me_load.h"
# include "me_dump.h"

/*---------------------------------------------------------------------------
VARIABLE: static time_t last_slice_time;

PURPOSE: 
    最后一次快照时间

REMARKS:     
---------------------------------------------------------------------------*/
static time_t last_slice_time;

/*---------------------------------------------------------------------------
VARIABLE: static nw_timer timer;

PURPOSE: 
    快照定时器，定时执行快照操作

REMARKS:

---------------------------------------------------------------------------*/
static nw_timer timer;

/*---------------------------------------------------------------------------
FUNCTION: static time_t get_today_start(void)

PURPOSE: 
    生成今天0点的时间戳

PARAMETERS:
    None

RETURN VALUE: 
    时间戳

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static time_t get_today_start(void)
{
    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    struct tm t;
    memset(&t, 0, sizeof(t));
    t.tm_year = lt->tm_year;
    t.tm_mon  = lt->tm_mon;
    t.tm_mday = lt->tm_mday;
    return mktime(&t);
}

/*---------------------------------------------------------------------------
FUNCTION: static int get_last_slice(MYSQL *conn, time_t *timestamp, 
            uint64_t *last_oper_id, uint64_t *last_order_id, uint64_t *last_deals_id)

PURPOSE: 
    读取最近的一次快照记录

PARAMETERS:
    [in]conn - mysql.trade_log数据连接
    [out]timestamp     - 最近一次快照时，创建表名的时间戳
    [out]last_oper_id  - 最近一次快照时，最后执行的operlog操作日志id
    [out]last_order_id - 最近一次快照时，最后执行的下单id（不是保存到history的）
    [out]last_deals_id - 最近一次快照时，最后撮合的成交单id（不是保存到history的）

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    系统启动时，从数据库恢复快照时调用
---------------------------------------------------------------------------*/
static int get_last_slice(MYSQL *conn, time_t *timestamp, uint64_t *last_oper_id, uint64_t *last_order_id, uint64_t *last_deals_id)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `time`, `end_oper_id`, `end_order_id`, `end_deals_id` from `slice_history` ORDER BY `id` DESC LIMIT 1");
    log_stderr("get last slice time");
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        log_stderr("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    if (num_rows != 1) {
        mysql_free_result(result);
        return 0;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    *timestamp = strtol(row[0], NULL, 0);
    *last_oper_id  = strtoull(row[1], NULL, 0);
    *last_order_id = strtoull(row[2], NULL, 0);
    *last_deals_id = strtoull(row[3], NULL, 0);
    mysql_free_result(result);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int load_slice_from_db(MYSQL *conn, time_t timestamp)

PURPOSE: 
    恢复最近一次快照的挂单、余额到内存数据结构

PARAMETERS:
    [in]conn - mysql.trade_log数据连接
    [in]timestamp - 最近一次快照时，创建表名的时间戳

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    系统启动时，从数据库恢复快照时调用
---------------------------------------------------------------------------*/
static int load_slice_from_db(MYSQL *conn, time_t timestamp)
{
    sds table = sdsempty();

    table = sdscatprintf(table, "slice_order_%ld", timestamp);
    log_stderr("load orders from: %s", table);
    int ret = load_orders(conn, table);
    if (ret < 0) {
        log_error("load_orders from %s fail: %d", table, ret);
        log_stderr("load_orders from %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

    sdsclear(table);
    table = sdscatprintf(table, "slice_balance_%ld", timestamp);
    log_stderr("load balance from: %s", table);
    ret = load_balance(conn, table);
    if (ret < 0) {
        log_error("load_balance from %s fail: %d", table, ret);
        log_stderr("load_balance from %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

    sdsfree(table);
    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int load_operlog_from_db(MYSQL *conn, time_t date, uint64_t *start_id)

PURPOSE: 
    读取operlog_$(day)中的记录，并以此恢复上次快照之后的操作

PARAMETERS:
    conn  - mysql.trade_log数据链接
    date -  读取日期
    start_id - 读取>=start_id的记录

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static int load_operlog_from_db(MYSQL *conn, time_t date, uint64_t *start_id)
{
    struct tm *t = localtime(&date);
    sds table = sdsempty();
    table = sdscatprintf(table, "operlog_%04d%02d%02d", 1900 + t->tm_year, 1 + t->tm_mon, t->tm_mday);
    log_stderr("load oper log from: %s", table);
    if (!is_table_exists(conn, table)) {
        log_error("table %s not exist", table);
        log_stderr("table %s not exist", table);
        sdsfree(table);
        return 0;
    }

    int ret = load_operlog(conn, table, start_id);
    if (ret < 0) {
        log_error("load_operlog from %s fail: %d", table, ret);
        log_stderr("load_operlog from %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

    sdsfree(table);
    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int init_from_db(void)

PURPOSE: 
    初始化用户余额、深度盘面、操作日志
    读取最近一次余额、深度快照，装载到内存，恢复数据后，准备开始撮合服务

PARAMETERS:
    None

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    使用最近一次slice，意味着makeslice之后的委单、成交将要被放弃。
    最近的委单、成交，被曝存在order_id_start/deals_id_start，trade_history不会删除，但是新单到达时，会进行覆盖
---------------------------------------------------------------------------*/
int init_from_db(void)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    time_t now = time(NULL);
    uint64_t last_oper_id  = 0;
    uint64_t last_order_id = 0;
    uint64_t last_deals_id = 0;
    int ret = get_last_slice(conn, &last_slice_time, &last_oper_id, &last_order_id, &last_deals_id);
    if (ret < 0) {
        return ret;
    }

    log_info("last_slice_time: %ld, last_oper_id: %"PRIu64", last_order_id: %"PRIu64", last_deals_id: %"PRIu64,
            last_slice_time, last_oper_id, last_order_id, last_deals_id);
    log_stderr("last_slice_time: %ld, last_oper_id: %"PRIu64", last_order_id: %"PRIu64", last_deals_id: %"PRIu64,
            last_slice_time, last_oper_id, last_order_id, last_deals_id);

    order_id_start = last_order_id;
    deals_id_start = last_deals_id;

    if (last_slice_time == 0) {
        ret = load_operlog_from_db(conn, now, &last_oper_id);
        if (ret < 0)
            goto cleanup;
    } else {
        ret = load_slice_from_db(conn, last_slice_time);
        if (ret < 0) {
            goto cleanup;
        }

        time_t begin = last_slice_time;
        time_t end = get_today_start() + 86400;
        while (begin < end) {
            ret = load_operlog_from_db(conn, begin, &last_oper_id);
            if (ret < 0) {
                goto cleanup;
            }
            begin += 86400;
        }
    }

    operlog_id_start = last_oper_id;

    mysql_close(conn);
    log_stderr("load success");

    return 0;

cleanup:
    mysql_close(conn);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int dump_order_to_db(MYSQL *conn, time_t end)

PURPOSE: 
    输出挂单队列到数据库trade_log.slice_order_{time}

PARAMETERS:
    [in]conn - mysql.trade_log数据链接
    [in]end  - 快照时间戳，用于创建表名 

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static int dump_order_to_db(MYSQL *conn, time_t end)
{
    sds table = sdsempty();
    table = sdscatprintf(table, "slice_order_%ld", end);
    log_info("dump order to: %s", table);
    int ret = dump_orders(conn, table);
    if (ret < 0) {
        log_error("dump_orders to %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }
    sdsfree(table);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int dump_balance_to_db(MYSQL *conn, time_t end)

PURPOSE: 
    输出余额到数据库trade_log.slice_balance_{time}

PARAMETERS:
    [in]conn - mysql.trade_log数据链接
    [in]end  - 快照时间戳，用于创建表名 

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
static int dump_balance_to_db(MYSQL *conn, time_t end)
{
    sds table = sdsempty();
    table = sdscatprintf(table, "slice_balance_%ld", end);
    log_info("dump balance to: %s", table);
    int ret = dump_balance(conn, table);
    if (ret < 0) {
        log_error("dump_balance to %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }
    sdsfree(table);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int update_slice_history(MYSQL *conn, time_t end)

PURPOSE: 
    保存快照信息到trade_log.slice_history

PARAMETERS:
    [in]conn - mysql.trade_log数据链接
    [in]end  - 快照时间戳 

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
int update_slice_history(MYSQL *conn, time_t end)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "INSERT INTO `slice_history` (`id`, `time`, `end_oper_id`, `end_order_id`, `end_deals_id`) VALUES (NULL, %ld, %"PRIu64", %"PRIu64", %"PRIu64")",
            end, operlog_id_start, order_id_start, deals_id_start);
    log_info("update slice history to: %ld", end);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret < 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int dump_to_db(time_t timestamp)

PURPOSE: 
    调用接口，输出挂单、余额到数据库，并更新快照记录

PARAMETERS:
    [in]timestamp - 快照时间戳

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    定时或手动快照时调用
---------------------------------------------------------------------------*/
int dump_to_db(time_t timestamp)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    log_info("start dump slice, timestamp: %ld", timestamp);

    int ret;
    ret = dump_order_to_db(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }

    ret = dump_balance_to_db(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }

    ret = update_slice_history(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }

    log_info("dump success");
    mysql_close(conn);
    return 0;

cleanup:
    log_info("dump fail");
    mysql_close(conn);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: static int slice_count(MYSQL *conn, time_t timestamp)

PURPOSE: 
    读取在keeptime内的快照的数量

PARAMETERS:
    [in]conn - mysql.trade_log数据链接
    [in]timestamp - 快照时间戳

RETURN VALUE: 
    >=0, count of slices. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    删除过期快照时，检查是否会留存快照，如果没有有效内的快照，则要保留超出有效期的快照
---------------------------------------------------------------------------*/
static int slice_count(MYSQL *conn, time_t timestamp)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT COUNT(*) FROM `slice_history` WHERE `time` >= %ld", timestamp - settings.slice_keeptime);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    if (num_rows != 1) {
        mysql_free_result(result);
        return -__LINE__;
    }

    MYSQL_ROW row = mysql_fetch_row(result);
    int count = atoi(row[0]);
    mysql_free_result(result);

    return count;
}

/*---------------------------------------------------------------------------
FUNCTION: static int delete_slice(MYSQL *conn, uint64_t id, time_t timestamp)

PURPOSE: 
    删除快照

PARAMETERS:
    [in]conn - mysql.trade_log数据链接
    [in]id   - slice_history中的记录id
    [in]timestamp - 快照时间戳

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
---------------------------------------------------------------------------*/
static int delete_slice(MYSQL *conn, uint64_t id, time_t timestamp)
{
    log_info("delete slice id: %"PRIu64", time: %ld start", id, timestamp);

    int ret;
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE `slice_order_%ld`", timestamp);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "DROP TABLE `slice_balance_%ld`", timestamp);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "DELETE FROM `slice_history` WHERE `id` = %"PRIu64"", id);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        return -__LINE__;
    }
    sdsfree(sql);

    log_info("delete slice id: %"PRIu64", time: %ld success", id, timestamp);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int clear_slice(time_t timestamp)

PURPOSE: 
    删除过期快照

PARAMETERS:
    [in]timestamp - 当前时间戳

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    如果有效期内没有快照，那么不删除过期快照
    过期时间在config.json中配置，以秒计数，259200=3*24*3600
    "slice_keeptime": 259200 
---------------------------------------------------------------------------*/
int clear_slice(time_t timestamp)
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql fail");
        return -__LINE__;
    }

    int ret = slice_count(conn, timestamp);
    if (ret < 0) {
        goto cleanup;
    }
    if (ret == 0) {
        log_error("0 slice in last %d seconds", settings.slice_keeptime);
        goto cleanup;
    }

    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `id`, `time` FROM `slice_history` WHERE `time` < %ld", timestamp - settings.slice_keeptime);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        ret = -__LINE__;
        goto cleanup;
    }
    sdsfree(sql);

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        uint64_t id = strtoull(row[0], NULL, 0);
        time_t slice_time = strtol(row[1], NULL, 0);
        ret = delete_slice(conn, id, slice_time);
        if (ret < 0) {
            mysql_free_result(result);
            goto cleanup;
        }

    }
    mysql_free_result(result);

    mysql_close(conn);
    return 0;

cleanup:
    mysql_close(conn);
    return ret;
}

/*---------------------------------------------------------------------------
FUNCTION: int make_slice(time_t timestamp)

PURPOSE: 
    执行快照，并清理过期快照

PARAMETERS:
    [in]timestamp - 当前时间戳

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    通过fork()创建子进程执行，这样子进程会继承父进程数据
    可能存在的问题：
    1.继承服务，监听服务端口继续运行，会继续处理数据，导致快照时时间不一致
    2.异步定时器、工作线程仍然存在，会继续尝试数据库、日志等操作
---------------------------------------------------------------------------*/
int make_slice(time_t timestamp)
{
    int pid = fork();
    if (pid < 0) {
        log_fatal("fork fail: %d", pid);
        return -__LINE__;
    } else if (pid > 0) {
        return 0;
    }

    int ret;
    ret = dump_to_db(timestamp);
    if (ret < 0) {
        log_fatal("dump_to_db fail: %d", ret);
    }

    ret = clear_slice(timestamp);
    if (ret < 0) {
        log_fatal("clear_slice fail: %d", ret);
    }

    exit(0);
    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static void on_timer(nw_timer *timer, void *privdata)

PURPOSE: 
    快照定时器，检查是否到快照时间，并调用快照接口

PARAMETERS:
    [in]timer - 
    [in]privdata - 

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    快照间隔在config.json中定义 "slice_interval": 3600
    定时器检查时间间隔在3600 =< interval <= 3605之间，
    这样意味着，假设：1.timer可能遇到不准时调用；2.快照执行时间超过5s
---------------------------------------------------------------------------*/
static void on_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    if ((now - last_slice_time) >= settings.slice_interval && (now % settings.slice_interval) <= 5) {
        make_slice(now);
        last_slice_time = now;
    }
}

/*---------------------------------------------------------------------------
FUNCTION: int init_persist(void)

PURPOSE: 
    启动快照定时器

PARAMETERS:
    None
    
RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
---------------------------------------------------------------------------*/
int init_persist(void)
{
    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

