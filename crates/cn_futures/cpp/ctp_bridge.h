#pragma once
#include <cstddef>
#include <cstdint>

// One private bridge contract, shared with Rust's repr(C) types. No SDK structs cross FFI.
struct NfHotEvent {
    int32_t kind;
    int32_t request;
    int32_t error;
    int32_t last;
    int32_t present;
    int32_t front;
    int32_t session;
    int32_t volume;
    int32_t traded;
    int32_t remaining;
    int32_t status;
    int32_t submit_status;
    int32_t direction;
    int32_t offset;
    int32_t hedge;
    int32_t time_condition;
    int32_t volume_condition;
    double price;
    char symbol[81];
    char exchange[9];
    char order_ref[13];
    char system_id[21];
    char trade_id[21];
    char trading_day[9];
    char investor[13];
    char message[81];
};
struct NfEvent {
    NfHotEvent hot;
    int32_t today;
    int32_t yesterday;
    int32_t position_direction;
    int32_t position_date;
    int32_t active;
    int32_t product_class;
    int32_t minimum;
    int32_t maximum;
    int32_t contract_multiple;
    int32_t expiry;
    int32_t market_minimum;
    int32_t market_maximum;
    int32_t relative;
    int32_t investor_range;
    double price_tick;
    double available;
    double commission_rates[6];
    double margin_rates[4];
    double account_values[44];
    char product[81];
    char max_order_ref[13];
};
using NfCallback = void (*)(void*, const NfHotEvent*, const NfEvent*);
extern "C" {
size_t nf_ctp_event_size();
size_t nf_ctp_hot_event_size();
int nf_ctp_version(const char*, char*, size_t);
void* nf_ctp_create(const char*, const char*, bool, NfCallback, void*, char*, size_t);
void nf_ctp_destroy(void*);
int nf_ctp_start(void*, const char*);
int nf_ctp_authenticate(void*, int, const char*, const char*, const char*, const char*);
int nf_ctp_login(void*, int, const char*, const char*, const char*);
int nf_ctp_settlement(void*, int, const char*, const char*);
int nf_ctp_query(void*, int, int, const char*, const char*, const char*, const char*);
int nf_ctp_prepare(void*, uint32_t, const char*, const char*, const char*, const char*, const char*);
int nf_ctp_insert(void*, uint32_t, int, uint64_t, int, int, int, int, double);
int nf_ctp_cancel(void*, uint32_t, int, uint64_t, const uint8_t*, size_t, int, int);
}
