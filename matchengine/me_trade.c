/*
 * Description: 
 *     History: yang@haipo.me, 2017/03/29, create
 */

# include "me_config.h"
# include "me_trade.h"

/*---------------------------------------------------------------------------
VARIABLE: static dict_t *dict_market;

PURPOSE: 
    市场容器

REMARKS:
    以market_name为key，以market_t*为value
---------------------------------------------------------------------------*/
static dict_t *dict_market;

static uint32_t market_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int market_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *market_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void market_dict_key_free(void *key)
{
    free(key);
}

/*---------------------------------------------------------------------------
FUNCTION: int init_trade(void)

PURPOSE: 
    初始化货币对dict_market

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
int init_trade(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = market_dict_hash_function;
    type.key_compare = market_dict_key_compare;
    type.key_dup = market_dict_key_dup;
    type.key_destructor = market_dict_key_free;

    dict_market = dict_create(&type, 64);
    if (dict_market == NULL)
        return -__LINE__;

    for (size_t i = 0; i < settings.market_num; ++i) {
        market_t *m = market_create(&settings.markets[i]);
        if (m == NULL) {
            return -__LINE__;
        }

        dict_add(dict_market, settings.markets[i].name, m);
    }

    return 0;
}

/*---------------------------------------------------------------------------
FUNCTION: market_t *get_market(const char *name)

PURPOSE: 
    从dict_market去的市场指针

PARAMETERS:
    name - 市场名称

RETURN VALUE: 
    如果找到market，返回market指针，否则返回 NULL

EXCEPTION: 
    <Exception that may be thrown by the function>

EXAMPLE CALL:
    <Example call of the function>

REMARKS:     
---------------------------------------------------------------------------*/
market_t *get_market(const char *name)
{
    dict_entry *entry = dict_find(dict_market, name);
    if (entry)
        return entry->val;
    return NULL;
}

