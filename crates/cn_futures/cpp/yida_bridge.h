#pragma once
#include <stddef.h>
#include <stdint.h>
// Owned POD copies only. No vendor pointer crosses this boundary.
struct CnYidaEvent {
    int32_t kind, error, day, reference, direction, offset, hedge;
    int32_t status, lots, traded, date, position_direction, frozen, minimum, maximum;
    int32_t replay, product_class, expiry, right, login_count, flag, native_time;
    int32_t order_group, trade_flag, no_order, legacy_system_id, legacy_trade_id;
    int32_t order_type, order_attribute, market_minimum, market_maximum;
    int32_t selection_type, connection_id, real_connection_id, connection_status;
    int32_t connection_count, public_connection, dedicated_connection;
    int32_t options_type, contract_multiple;
    int64_t system_id, trade_id;
    double price, tick, available, commission, strike_price, upper_limit_price;
    char symbol[64], exchange[12], original_order_id[64], original_trade_id[64];
    char connection_info[28], insert_flow[32], cancel_flow[32];
    char underlying[64], underlying_exchange[12];
};
struct CnYidaOrderRequest {
    char exchange[12], symbol[64];
    int32_t reference, direction, offset, tif, market, lots, selection, connection;
    double price, option_market_buy_ceiling;
};
struct CnYidaCancelRequest {
    char exchange[12];
    int32_t group, reference, selection, connection;
    int64_t system_id;
};
using CnYidaCallback = void (*)(void*, const CnYidaEvent*);
extern "C" {
size_t cn_yida_event_size();
size_t cn_yida_order_request_size();
size_t cn_yida_cancel_request_size();
int cn_yida_version(const char*, char*, size_t);
void* cn_yida_create(const char*, const char*, int, int, CnYidaCallback, void*, char*, size_t);
int cn_yida_destroy(void*);
int cn_yida_start(void*);
int cn_yida_allow_instrument(void*, const char*, const char*);
int cn_yida_login(void*, const char*, const char*, const char*, const char*);
int cn_yida_reference(void*);
int cn_yida_insert(void*, const CnYidaOrderRequest*);
int cn_yida_cancel(void*, const CnYidaCancelRequest*);
int cn_yida_insert_batch(void*, const CnYidaOrderRequest*, int, int*);
int cn_yida_cancel_batch(void*, const CnYidaCancelRequest*, int, int*);
int cn_yida_transport_ready(void*);
int cn_yida_public_simulation_target(void*);
void cn_yida_disconnect(void*);
}
