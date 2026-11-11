#pragma once
#include <cstddef>
#include <cstdint>
struct NfMdEvent {
    int32_t kind, request, error, last, present, volume, millisec;
    int32_t bid_volume[5], ask_volume[5];
    double last_price, turnover, open_interest, upper_limit, lower_limit;
    double bid[5], ask[5];
    char symbol[81], exchange[9], trading_day[9], action_day[9], update_time[9], message[256];
};
using NfMdCallback = void (*)(void*, const NfMdEvent*);
extern "C" {
size_t nf_md_event_size();
int nf_md_version(const char*, char*, size_t);
void* nf_md_create(const char*, const char*, bool, NfMdCallback, void*, char*, size_t);
void nf_md_destroy(void*);
int nf_md_start(void*, const char*);
int nf_md_login(void*, int, const char*, const char*, const char*);
int nf_md_subscribe(void*, const char* const*, size_t);
}
