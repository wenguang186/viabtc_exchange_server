/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/15, create
 */

# include "me_config.h"
# include "me_balance.h"

/*---------------------------------------------------------------------------
VARIABLE: dict_t *dict_balance;

PURPOSE: 
    用户余额结构
    以struct balance_key为键值(user_id/BALANCE_TYPE_AVAILABLE,user_id/BALANCE_TYPE_FREEZE)

REMARKS: 
    有一系列的访问函数get/set/del/add/sub/status，可以访问dict_balance
    me_cli.c查询资产 与me_dump.c 输出资产导数据库时，也有用到
---------------------------------------------------------------------------*/
dict_t *dict_balance;

/*---------------------------------------------------------------------------
VARIABLE: static dict_t *dict_asset;

PURPOSE: 
    存储资产的管理配置

REMARKS: 
    主要用于资产格式化asset_prec/asset_prec_show 与 重复检查 asset_exist
---------------------------------------------------------------------------*/
static dict_t *dict_asset;

struct asset_type {
    int prec_save;
    int prec_show;
};

static uint32_t asset_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static void *asset_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void *asset_dict_val_dup(const void *val)
{
    struct asset_type *obj = malloc(sizeof(struct asset_type));
    if (obj == NULL)
        return NULL;
    memcpy(obj, val, sizeof(struct asset_type));
    return obj;
}

static int asset_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void asset_dict_key_free(void *key)
{
    free(key);
}

static void asset_dict_val_free(void *val)
{
    free(val);
}

static uint32_t balance_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct balance_key));
}

static void *balance_dict_key_dup(const void *key)
{
    struct balance_key *obj = malloc(sizeof(struct balance_key));
    if (obj == NULL)
        return NULL;
    memcpy(obj, key, sizeof(struct balance_key));
    return obj;
}

static void *balance_dict_val_dup(const void *val)
{
    return mpd_qncopy(val);
}

static int balance_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct balance_key));
}

static void balance_dict_key_free(void *key)
{
    free(key);
}

static void balance_dict_val_free(void *val)
{
    mpd_del(val);
}

static int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = asset_dict_hash_function;
    type.key_compare    = asset_dict_key_compare;
    type.key_dup        = asset_dict_key_dup;
    type.key_destructor = asset_dict_key_free;
    type.val_dup        = asset_dict_val_dup;
    type.val_destructor = asset_dict_val_free;

    dict_asset = dict_create(&type, 64);
    if (dict_asset == NULL)
        return -__LINE__;

    memset(&type, 0, sizeof(type));
    type.hash_function  = balance_dict_hash_function;
    type.key_compare    = balance_dict_key_compare;
    type.key_dup        = balance_dict_key_dup;
    type.key_destructor = balance_dict_key_free;
    type.val_dup        = balance_dict_val_dup;
    type.val_destructor = balance_dict_val_free;

    dict_balance = dict_create(&type, 64);
    if (dict_balance == NULL)
        return -__LINE__;

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: int init_balance()

PURPOSE: 
    Init dict_asset.

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
int init_balance()
{
    ERR_RET(init_dict());

    for (size_t i = 0; i < settings.asset_num; ++i) {
        struct asset_type type;
        type.prec_save = settings.assets[i].prec_save;
        type.prec_show = settings.assets[i].prec_show;
        if (dict_add(dict_asset, settings.assets[i].name, &type) == NULL)
            return -__LINE__;
    }

    return 0;
}

static struct asset_type *get_asset_type(const char *asset)
{
    dict_entry *entry = dict_find(dict_asset, asset);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

bool asset_exist(const char *asset)
{
    struct asset_type *at = get_asset_type(asset);
    return at ? true : false;
}

int asset_prec(const char *asset)
{
    struct asset_type *at = get_asset_type(asset);
    return at ? at->prec_save : -1;
}

int asset_prec_show(const char *asset)
{
    struct asset_type *at = get_asset_type(asset);
    return at ? at->prec_show: -1;
}

/*---------------------------------------------------------------------------
FUNCTION: mpd_t *balance_get(uint32_t user_id, uint32_t type, const char *asset)

PURPOSE: 
    读取用户资产余额

PARAMETERS:
    user_id - 
    type    - BALANCE_TYPE_AVAILABLE/BALANCE_TYPE_FREEZE
    asset   - coin name

RETURN VALUE: 
    User's balance, if success. NULL, if failed. 

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
mpd_t *balance_get(uint32_t user_id, uint32_t type, const char *asset)
{
    struct balance_key key;
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));

    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        return entry->val;
    }

    return NULL;
}

/*---------------------------------------------------------------------------
FUNCTION: void balance_del(uint32_t user_id, uint32_t type, const char *asset)

PURPOSE: 
    清空用户资产余额

PARAMETERS:
    user_id - 
    type    - BALANCE_TYPE_AVAILABLE/BALANCE_TYPE_FREEZE
    asset   - coin name

RETURN VALUE: 
    None

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
void balance_del(uint32_t user_id, uint32_t type, const char *asset)
{
    struct balance_key key;
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));
    dict_delete(dict_balance, &key);
}

/*---------------------------------------------------------------------------
FUNCTION: mpd_t *balance_set(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)

PURPOSE: 
    重置用户资产余额

PARAMETERS:
    user_id - 
    type    - BALANCE_TYPE_AVAILABLE/BALANCE_TYPE_FREEZE
    asset   - coin name
    amount  - =0删除余额，>0设置余额，不允许<0

RETURN VALUE: 
    New balance, if success. NULL, if failed. 

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
mpd_t *balance_set(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    int ret = mpd_cmp(amount, mpd_zero, &mpd_ctx);
    if (ret < 0) {
        return NULL;
    } else if (ret == 0) {
        balance_del(user_id, type, asset);
        return mpd_zero;
    }

    struct balance_key key;
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));

    mpd_t *result;
    dict_entry *entry;
    entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        mpd_rescale(result, amount, -at->prec_save, &mpd_ctx);
        return result;
    }

    entry = dict_add(dict_balance, &key, amount);
    if (entry == NULL)
        return NULL;
    result = entry->val;
    mpd_rescale(result, amount, -at->prec_save, &mpd_ctx);

    return result;
}

/*---------------------------------------------------------------------------
FUNCTION: mpd_t *balance_add(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)

PURPOSE: 
    增加用户资产余额

PARAMETERS:
    user_id - 
    type    - BALANCE_TYPE_AVAILABLE/BALANCE_TYPE_FREEZE
    asset   - coin name
    amount  - >=0要增加的余额，不允许<0

RETURN VALUE: 
    New balance, if success. NULL, if failed. 

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
mpd_t *balance_add(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    struct balance_key key;
    key.user_id = user_id;
    key.type = type;
    strncpy(key.asset, asset, sizeof(key.asset));

    mpd_t *result;
    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        mpd_add(result, result, amount, &mpd_ctx);
        mpd_rescale(result, result, -at->prec_save, &mpd_ctx);
        return result;
    }

    return balance_set(user_id, type, asset, amount);
}

/*---------------------------------------------------------------------------
FUNCTION: mpd_t *balance_sub(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)

PURPOSE: 
    减少用户资产余额

PARAMETERS:
    user_id - 
    type    - BALANCE_TYPE_AVAILABLE/BALANCE_TYPE_FREEZE
    asset   - coin name
    amount  - >=0要减少的余额，不允许<0

RETURN VALUE: 
    New balance, if success. NULL, if failed. 

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    <Additional remarks of the function>
---------------------------------------------------------------------------*/
mpd_t *balance_sub(uint32_t user_id, uint32_t type, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    mpd_t *result = balance_get(user_id, type, asset);
    if (result == NULL)
        return NULL;
    if (mpd_cmp(result, amount, &mpd_ctx) < 0)
        return NULL;

    mpd_sub(result, result, amount, &mpd_ctx);
    if (mpd_cmp(result, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, type, asset);
        return mpd_zero;
    }
    mpd_rescale(result, result, -at->prec_save, &mpd_ctx);

    return result;
}

/*---------------------------------------------------------------------------
FUNCTION: mpd_t *balance_freeze(uint32_t user_id, const char *asset, mpd_t *amount)

PURPOSE: 
    冻结用户资产

PARAMETERS:
    user_id - 
    asset   - coin name
    amount  - >=0要增加冻结的余额，不允许<0

RETURN VALUE: 
    New available balance, if success. NULL, if failed. 

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    冻结成功的前提：
    1.asset合法；2.amount >= mpd_zero；3. cur available >= amount

    问题：没有做互斥，如果单线程执行撮合，可能还是可以的，但是和dump也还是有冲突
---------------------------------------------------------------------------*/
mpd_t *balance_freeze(uint32_t user_id, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
    if (available == NULL)
        return NULL;
    if (mpd_cmp(available, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, BALANCE_TYPE_FREEZE, asset, amount) == 0)
        return NULL;
    mpd_sub(available, available, amount, &mpd_ctx);
    if (mpd_cmp(available, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, BALANCE_TYPE_AVAILABLE, asset);
        return mpd_zero;
    }
    mpd_rescale(available, available, -at->prec_save, &mpd_ctx);

    return available;
}

/*---------------------------------------------------------------------------
FUNCTION: mpd_t *balance_unfreeze(uint32_t user_id, const char *asset, mpd_t *amount)

PURPOSE: 
    解冻用户资产

PARAMETERS:
    user_id - 
    asset   - coin name
    amount  - >=0要解冻的余额，不允许<0

RETURN VALUE: 
    New freezed balance, if success. NULL, if failed. 

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
    解冻成功的前提：
    1.asset合法；2.amount >= mpd_zero；3. cur freeze >= amount

    问题：没有做互斥，如果单线程执行撮合，可能还是可以的，但是和dump也还是有冲突
---------------------------------------------------------------------------*/
mpd_t *balance_unfreeze(uint32_t user_id, const char *asset, mpd_t *amount)
{
    struct asset_type *at = get_asset_type(asset);
    if (at == NULL)
        return NULL;

    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
    if (freeze == NULL)
        return NULL;
    if (mpd_cmp(freeze, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, BALANCE_TYPE_AVAILABLE, asset, amount) == 0)
        return NULL;
    mpd_sub(freeze, freeze, amount, &mpd_ctx);
    if (mpd_cmp(freeze, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, BALANCE_TYPE_FREEZE, asset);
        return mpd_zero;
    }
    mpd_rescale(freeze, freeze, -at->prec_save, &mpd_ctx);

    return freeze;
}

/*---------------------------------------------------------------------------
FUNCTION: mpd_t *balance_total(uint32_t user_id, const char *asset)

PURPOSE: 
    读取用户资产总额

PARAMETERS:
    user_id - 
    asset   - coin name

RETURN VALUE: 
    User's total balance 

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
---------------------------------------------------------------------------*/
mpd_t *balance_total(uint32_t user_id, const char *asset)
{
    mpd_t *balance = mpd_new(&mpd_ctx);
    mpd_copy(balance, mpd_zero, &mpd_ctx);
    mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
    if (available) {
        mpd_add(balance, balance, available, &mpd_ctx);
    }
    mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
    if (freeze) {
        mpd_add(balance, balance, freeze, &mpd_ctx);
    }

    return balance;
}

/*---------------------------------------------------------------------------
FUNCTION: int balance_status(const char *asset, mpd_t *total, size_t *available_count, mpd_t *available, size_t *freeze_count, mpd_t *freeze)

PURPOSE: 
    读取所有用户资产总额

PARAMETERS:
    asset   - coin name
    total   - All user's total balance
    available_count - The Count of user who has available balance
    available - All user's available balance
    freeze_count  - The Count of user who has freeze balance
    freeze  - All user's freeze balance

RETURN VALUE: 
    0

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS: 
---------------------------------------------------------------------------*/
int balance_status(const char *asset, mpd_t *total, size_t *available_count, mpd_t *available, size_t *freeze_count, mpd_t *freeze)
{
    *freeze_count = 0;
    *available_count = 0;
    mpd_copy(total, mpd_zero, &mpd_ctx);
    mpd_copy(freeze, mpd_zero, &mpd_ctx);
    mpd_copy(available, mpd_zero, &mpd_ctx);

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_balance);
    while ((entry = dict_next(iter)) != NULL) {
        struct balance_key *key = entry->key;
        if (strcmp(key->asset, asset) != 0)
            continue;
        mpd_add(total, total, entry->val, &mpd_ctx);
        if (key->type == BALANCE_TYPE_AVAILABLE) {
            *available_count += 1;
            mpd_add(available, available, entry->val, &mpd_ctx);
        } else {
            *freeze_count += 1;
            mpd_add(freeze, freeze, entry->val, &mpd_ctx);
        }
    }
    dict_release_iterator(iter);

    return 0;
}

