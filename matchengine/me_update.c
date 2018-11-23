/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/18, create
 */

# include "me_config.h"
# include "me_update.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"

/*---------------------------------------------------------------------------
VARIABLE: static dict_t *dict_update;

PURPOSE: 
    余额更新命令缓存，存储balance.update命令（server收到的或从operlog装载的）

REMARKS:
    用于检查余额更新命令是否被执行过，如果被执行过，则避免重复执行
    缓存有效期为24h
---------------------------------------------------------------------------*/
static dict_t *dict_update;

/*---------------------------------------------------------------------------
VARIABLE: static nw_timer timer;

PURPOSE: 
    余额更新命令缓存清理定时器
    60s执行一次，删除过期（24h）的缓存

REMARKS:
---------------------------------------------------------------------------*/
static nw_timer timer;

struct update_key {
    uint32_t    user_id;
    char        asset[ASSET_NAME_MAX_LEN + 1];
    char        business[BUSINESS_NAME_MAX_LEN + 1];
    uint64_t    business_id;
};

struct update_val {
    double      create_time;
};

static uint32_t update_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct update_key));
}

static int update_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct update_key));
}

static void *update_dict_key_dup(const void *key)
{
    struct update_key *obj = malloc(sizeof(struct update_key));
    memcpy(obj, key, sizeof(struct update_key));
    return obj;
}

static void update_dict_key_free(void *key)
{
    free(key);
}

static void *update_dict_val_dup(const void *val)
{
    struct update_val*obj = malloc(sizeof(struct update_val));
    memcpy(obj, val, sizeof(struct update_val));
    return obj;
}

static void update_dict_val_free(void *val)
{
    free(val);
}


/*---------------------------------------------------------------------------
FUNCTION: static void on_timer(nw_timer *timer, void *privdata)

PURPOSE: 
    余额更新命令缓存清理定时器
    60s执行一次，删除过期（24h）的缓存

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

---------------------------------------------------------------------------*/
static void on_timer(nw_timer *t, void *privdata)
{
    double now = current_timestamp();
    dict_iterator *iter = dict_get_iterator(dict_update);
    dict_entry *entry;
    while ((entry = dict_next(iter)) != NULL) {
        struct update_val *val = entry->val;
        if (val->create_time < (now - 86400)) {
            dict_delete(dict_update, entry->key);
        }
    }
    dict_release_iterator(iter);
}

/*---------------------------------------------------------------------------
FUNCTION: int init_update(void)

PURPOSE: 
    初始化余额更新缓存dict_update，启动清理定时器

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
int init_update(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = update_dict_hash_function;
    type.key_compare    = update_dict_key_compare;
    type.key_dup        = update_dict_key_dup;
    type.key_destructor = update_dict_key_free;
    type.val_dup        = update_dict_val_dup;
    type.val_destructor = update_dict_val_free;

    dict_update = dict_create(&type, 64);
    if (dict_update == NULL)
        return -__LINE__;

    nw_timer_set(&timer, 60, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int update_user_balance(bool real, uint32_t user_id, const char *asset,
             const char *business, uint64_t business_id, mpd_t *change, json_t *detail)

PURPOSE: 
    更新用户可用余额

PARAMETERS:
    real - 是否写入history数据库，并发送balances消息。装载operlog时为false
    user_id - 
    asset - 币种
    business - 事务类型
    business_id - 事务id
    change - 变动金额，可正可负
    detail - 备注明细

RETURN VALUE: 
    0，执行成功
    -1，命令已经执行过
    -2，可用余额不足

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:
    收到或启动装载 balance.update  时调用
    如果检查到命令已经被执行过，不再执行
    如果是收到命令real=true，那么写入balance.history，推送并balances消息到kafka
---------------------------------------------------------------------------*/
int update_user_balance(bool real, uint32_t user_id, const char *asset, const char *business, uint64_t business_id, mpd_t *change, json_t *detail)
{
    struct update_key key;
    key.user_id = user_id;
    strncpy(key.asset, asset, sizeof(key.asset));
    strncpy(key.business, business, sizeof(key.business));
    key.business_id = business_id;

    dict_entry *entry = dict_find(dict_update, &key);
    if (entry) {
        return -1;
    }

    mpd_t *result;
    mpd_t *abs_change = mpd_new(&mpd_ctx);
    mpd_abs(abs_change, change, &mpd_ctx);
    if (mpd_cmp(change, mpd_zero, &mpd_ctx) >= 0) {
        result = balance_add(user_id, BALANCE_TYPE_AVAILABLE, asset, abs_change);
    } else {
        result = balance_sub(user_id, BALANCE_TYPE_AVAILABLE, asset, abs_change);
    }
    mpd_del(abs_change);
    if (result == NULL)
        return -2;

    struct update_val val = { .create_time = current_timestamp() };
    dict_add(dict_update, &key, &val);

    if (real) {
        double now = current_timestamp();
        json_object_set_new(detail, "id", json_integer(business_id));
        char *detail_str = json_dumps(detail, 0);
        append_user_balance_history(now, user_id, asset, business, change, detail_str);
        free(detail_str);
        push_balance_message(now, user_id, asset, business, change);
    }

    return 0;
}

