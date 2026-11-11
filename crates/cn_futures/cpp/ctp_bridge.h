#pragma once
#include <cstddef>
#include <cstdint>

// One private bridge contract, shared with Rust's repr(C) types. No SDK structs cross FFI.
struct NfEvent {
    int32_t kind, request, error, last, present;
    int32_t front, session, volume, traded, remaining, today, yesterday;
    int32_t status, submit_status, direction, offset, hedge, position_direction, position_date;
    int32_t active, product_class, minimum, maximum;
    int32_t time_condition, volume_condition;
    int32_t contract_multiple, expiry, market_minimum, market_maximum, relative, investor_range;
    double price, price_tick, available;
    double commission_rates[6], margin_rates[4], account_values[44];
    char symbol[81], exchange[9], order_ref[13], system_id[21], trade_id[21];
    char product[81];
    char trading_day[9], investor[13], max_order_ref[13], message[256];
};
using NfCallback = void (*)(void*, const NfEvent*);
extern "C" {
size_t nf_ctp_event_size();
int nf_ctp_version(const char*, char*, size_t);
void* nf_ctp_create(const char*, const char*, bool, NfCallback, void*, char*, size_t);
void nf_ctp_destroy(void*);
int nf_ctp_start(void*, const char*);
int nf_ctp_authenticate(void*, int, const char*, const char*, const char*, const char*);
int nf_ctp_login(void*, int, const char*, const char*, const char*);
int nf_ctp_settlement(void*, int, const char*, const char*);
int nf_ctp_query(void*, int, int, const char*, const char*, const char*, const char*);
int nf_ctp_insert(void*, int, const char*, const char*, const char*, const char*, const char*, int, int, int, int, double);
int nf_ctp_cancel(void*, int, const char*, const char*, const char*, const char*, const char*, const char*, int, int);
}
