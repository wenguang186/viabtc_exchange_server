/*
 * Description: 
 *     History: yang@haipo.me, 2017/04/04, create
 */

# include "ut_mysql.h"
# include "me_trade.h"
# include "me_market.h"
# include "me_balance.h"

/*---------------------------------------------------------------------------
FUNCTION: static sds sql_append_mpd(sds sql, mpd_t *val, bool comma)

PURPOSE: 
    把mpd_t类型转为字符串，附加到sql字符串尾部

PARAMETERS:
    sql   - 字符串，将val添加到其尾部
    val   - 将要添加的数值
    comma - 是否追加逗号

RETURN VALUE: 
    拼接之后的字符串

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
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
FUNCTION: static int dump_orders_list(MYSQL *conn, const char *table, skiplist_t *list)

PURPOSE: 
    输出深度委单到mysql.trade_log.slice_order_${time}

PARAMETERS:
    conn – MySQL数据链接
    table - 表名，slice_order_$timestamp 后缀时间戳参数
    list - 委单列表，当前货币对的asks/bids列表

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret;
    ret = dump_orders_list(conn, table, market->asks);
    if (ret < 0) {
        log_error("dump market: %s asks orders list fail: %d", market->name, ret);
        return -__LINE__;
    }

REMARKS: 
    把未成交的深度委单列表，写入数据库做持久化存储。
    注意，这里并没有与撮合过程做互斥，list也没有做保护，存在冲突问题
---------------------------------------------------------------------------*/
static int dump_orders_list(MYSQL *conn, const char *table, skiplist_t *list)
{
    sds sql = sdsempty();

    size_t insert_limit = 1000;
    size_t index = 0;
    skiplist_iter *iter = skiplist_get_iterator(list);
    skiplist_node *node;
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `t`, `side`, `create_time`, `update_time`, `user_id`, `market`, "
                    "`price`, `amount`, `taker_fee`, `maker_fee`, `left`, `freeze`, `deal_stock`, `deal_money`, `deal_fee`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(%"PRIu64", %u, %u, %f, %f, %u, '%s' , ",
                order->id, order->type, order->side, order->create_time, order->update_time, order->user_id, order->market);
        sql = sql_append_mpd(sql, order->price, true);
        sql = sql_append_mpd(sql, order->amount, true);
        sql = sql_append_mpd(sql, order->taker_fee, true);
        sql = sql_append_mpd(sql, order->maker_fee, true);
        sql = sql_append_mpd(sql, order->left, true);
        sql = sql_append_mpd(sql, order->freeze, true);
        sql = sql_append_mpd(sql, order->deal_stock, true);
        sql = sql_append_mpd(sql, order->deal_money, true);
        sql = sql_append_mpd(sql, order->deal_fee, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret < 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                skiplist_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    skiplist_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret < 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int dump_orders(MYSQL *conn, const char *table)

PURPOSE: 
    执行深度委单快照
    创建快照数据表mysql.trade_log.slice_order_${time}，并调用输出深度委单接口

PARAMETERS:
    conn – MySQL数据链接
    table - 表名，slice_order_$timestamp 后缀时间戳参数

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret = dump_orders(conn, table);
    if (ret < 0) {
        log_error("dump_orders to %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

REMARKS: 
    把未成交的深度委单列表，写入数据库做持久化存储。
    注意，这里并没有与撮合过程做互斥，get_market()得到的market也没有做保护，存在冲突问题
---------------------------------------------------------------------------*/
int dump_orders(MYSQL *conn, const char *table)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE IF EXISTS `%s`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_order_example`", table);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    for (int i = 0; i < settings.market_num; ++i) {
        market_t *market = get_market(settings.markets[i].name);
        if (market == NULL) {
            return -__LINE__;
        }
        int ret;
        ret = dump_orders_list(conn, table, market->asks);
        if (ret < 0) {
            log_error("dump market: %s asks orders list fail: %d", market->name, ret);
            return -__LINE__;
        }
        ret = dump_orders_list(conn, table, market->bids);
        if (ret < 0) {
            log_error("dump market: %s bids orders list fail: %d", market->name, ret);
            return -__LINE__;
        }
    }

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: static int dump_balance_dict(MYSQL *conn, const char *table, dict_t *dict)

PURPOSE: 
    用户资产余额输出接口，写余额结构到mysql.trade_log.slice_balance_$timestamp

PARAMETERS:
    conn  – MySQL数据链接
    table - 表名，slice_order_$timestamp 后缀时间戳参数
    dict  - 资产字典

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret = dump_balance_dict(conn, table, dict_balance);
    if (ret < 0) {
        log_error("dump_balance_dict fail: %d", ret);
        return -__LINE__;
    }

REMARKS: 
    注意，这里并没有与撮合过程做互斥
---------------------------------------------------------------------------*/
static int dump_balance_dict(MYSQL *conn, const char *table, dict_t *dict)
{
    sds sql = sdsempty();

    size_t insert_limit = 1000;
    size_t index = 0;
    dict_iterator *iter = dict_get_iterator(dict);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct balance_key *key = entry->key;
        mpd_t *balance = entry->val;
        if (index == 0) {
            sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `user_id`, `asset`, `t`, `balance`) VALUES ", table);
        } else {
            sql = sdscatprintf(sql, ", ");
        }

        sql = sdscatprintf(sql, "(NULL, %u, '%s', %u, ", key->user_id, key->asset, key->type);
        sql = sql_append_mpd(sql, balance, false);
        sql = sdscatprintf(sql, ")");

        index += 1;
        if (index == insert_limit) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret < 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                dict_release_iterator(iter);
                sdsfree(sql);
                return -__LINE__;
            }
            sdsclear(sql);
            index = 0;
        }
    }
    dict_release_iterator(iter);

    if (index > 0) {
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret < 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
    }

    sdsfree(sql);
    return 0;
}


/*---------------------------------------------------------------------------
FUNCTION: int dump_balance(MYSQL *conn, const char *table)

PURPOSE: 
    执行用户余额快照
    创建dict_balance快照，并调用输出接口保存到数据表mysql.trade_log.slice_balance_$timestamp

PARAMETERS:
    conn – MySQL数据链接
    table - 表名，slice_order_$timestamp 后缀时间戳参数

RETURN VALUE: 
    Zero, if success. <0, the error line number.

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    int ret = dump_balance(conn, table);
    if (ret < 0) {
        log_error("dump_balance to %s fail: %d", table, ret);
        sdsfree(table);
        return -__LINE__;
    }

REMARKS: 
    注意，这里并没有与撮合过程做互斥，dict_balance没有做保护，存在冲突问题

    反向调用顺序（退栈）
    dump_order_to_db()
    dump_to_db()
    make_slice()
    on_timer()
---------------------------------------------------------------------------*/
int dump_balance(MYSQL *conn, const char *table)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "DROP TABLE IF EXISTS `%s`", table);
    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsclear(sql);

    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` LIKE `slice_balance_example`", table);
    log_trace("exec sql: %s", sql);
    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        return -__LINE__;
    }
    sdsfree(sql);

    ret = dump_balance_dict(conn, table, dict_balance);
    if (ret < 0) {
        log_error("dump_balance_dict fail: %d", ret);
        return -__LINE__;
    }

    return 0;
}

