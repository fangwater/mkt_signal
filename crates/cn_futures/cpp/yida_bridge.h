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
    int64_t system_id, trade_id;
    double price, tick, available, commission;
    char symbol[32], exchange[12], original_order_id[64], original_trade_id[64];
};
using CnYidaCallback = void (*)(void*, const CnYidaEvent*);
extern "C" {
size_t cn_yida_event_size();
int cn_yida_version(const char*, char*, size_t);
void* cn_yida_create(const char*, const char*, int, CnYidaCallback, void*, char*, size_t);
int cn_yida_destroy(void*);
int cn_yida_start(void*);
int cn_yida_login(void*, const char*, const char*, const char*, const char*);
int cn_yida_reference(void*);
int cn_yida_insert(void*, const char*, const char*, int, int, int, int, int, int, double);
int cn_yida_cancel(void*, const char*, int64_t);
int cn_yida_transport_ready(void*);
int cn_yida_public_simulation_target(void*);
void cn_yida_disconnect(void*);
}
