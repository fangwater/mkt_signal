// Read-only CTP 6.7.11 production market-data probe. No Trader API is linked.
// Build and usage: docs/ctp_trade.md. Credentials come only from the environment.
#include "ThostFtdcMdApi.h"
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
std::mutex output_mutex;
void emit(const std::string& fields) {
    std::lock_guard<std::mutex> lock(output_mutex);
    const auto now = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::cout << "{\"received_us\":" << now << ',' << fields << "}" << std::endl;
}
std::string json_string(const std::string& value) {
    // Only emit bounded ASCII identifiers, never credential or error-message fields.
    std::string safe;
    for (unsigned char c : value) safe += c >= 32 && c < 127 ? static_cast<char>(c) : '?';
    std::ostringstream out;
    out << std::quoted(safe);
    return out.str();
}
template <size_t N> std::string field(const char (&value)[N]) {
    return std::string(value, strnlen(value, N));
}
template <size_t N> void credential(char (&target)[N], const char* name) {
    const char* value = std::getenv(name);
    if (!value || !*value || std::strlen(value) >= N)
        throw std::runtime_error(std::string("missing or invalid environment variable ") + name);
    std::memcpy(target, value, std::strlen(value) + 1);
}
std::string price(double value) {
    if (!std::isfinite(value) || std::abs(value) >= 1e100) return "null";
    std::ostringstream out;
    out << std::setprecision(15) << value;
    return out.str();
}

class Probe final : public CThostFtdcMdSpi {
    std::mutex mutex;
    std::condition_variable changed;
    CThostFtdcReqUserLoginField login{};
    std::vector<std::string> symbols;
    bool login_attempted = false;
    bool connected = false;
    bool logged_in = false;
    bool failed = false;
    unsigned subscriptions = 0;
    unsigned ticks = 0;

    void failure(const char* stage, int error) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            failed = true;
        }
        emit("\"type\":\"error\",\"stage\":" + json_string(stage)
             + ",\"error_id\":" + std::to_string(error));
        changed.notify_all();
    }
public:
    CThostFtdcMdApi* api = nullptr;
    explicit Probe(std::vector<std::string> selected) : symbols(std::move(selected)) {
        credential(login.BrokerID, "CTP_BROKER_ID");
        credential(login.UserID, "CTP_USER_ID");
        credential(login.Password, "CTP_PASSWORD");
    }
    void OnFrontConnected() override {
        {
            std::lock_guard<std::mutex> lock(mutex);
            connected = true;
            // A failed password must never trigger automatic repeated login attempts.
            if (login_attempted) return;
            login_attempted = true;
        }
        emit("\"type\":\"front_connected\"");
        const int result = api->ReqUserLogin(&login, 1);
        if (result) failure("login_submit", result);
    }
    void OnFrontDisconnected(int reason) override {
        failure("front_disconnected", reason);
    }
    void OnRspUserLogin(CThostFtdcRspUserLoginField* response,
                        CThostFtdcRspInfoField* info, int, bool last) override {
        if (info && info->ErrorID) { failure("login", info->ErrorID); return; }
        if (!last) return;
        if (!response) { failure("missing_login_response", -1); return; }
        {
            std::lock_guard<std::mutex> lock(mutex);
            logged_in = true;
        }
        emit("\"type\":\"login_ok\",\"trading_day\":" + json_string(field(response->TradingDay))
             + ",\"login_time\":" + json_string(field(response->LoginTime)));
        std::vector<char*> ids;
        for (auto& symbol : symbols) ids.push_back(symbol.data());
        const int result = api->SubscribeMarketData(ids.data(), static_cast<int>(ids.size()));
        if (result) failure("subscribe_submit", result);
    }
    void OnRspSubMarketData(CThostFtdcSpecificInstrumentField* instrument,
                            CThostFtdcRspInfoField* info, int, bool) override {
        if (info && info->ErrorID) { failure("subscribe", info->ErrorID); return; }
        if (!instrument) { failure("missing_subscription_response", -1); return; }
        {
            std::lock_guard<std::mutex> lock(mutex);
            ++subscriptions;
        }
        emit("\"type\":\"subscribed\",\"symbol\":" + json_string(field(instrument->InstrumentID)));
        changed.notify_all();
    }
    void OnRspError(CThostFtdcRspInfoField* info, int, bool) override {
        failure("response", info ? info->ErrorID : -1);
    }
    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* tick) override {
        if (!tick) return;
        unsigned count;
        {
            std::lock_guard<std::mutex> lock(mutex);
            count = ++ticks;
        }
        if (count <= 10) {
            emit("\"type\":\"tick\",\"symbol\":" + json_string(field(tick->InstrumentID))
                 + ",\"exchange\":" + json_string(field(tick->ExchangeID))
                 + ",\"trading_day\":" + json_string(field(tick->TradingDay))
                 + ",\"action_day\":" + json_string(field(tick->ActionDay))
                 + ",\"update_time\":" + json_string(field(tick->UpdateTime))
                 + ",\"update_millisec\":" + std::to_string(tick->UpdateMillisec)
                 + ",\"last_price\":" + price(tick->LastPrice)
                 + ",\"bid1\":" + price(tick->BidPrice1)
                 + ",\"bid1_volume\":" + std::to_string(tick->BidVolume1)
                 + ",\"ask1\":" + price(tick->AskPrice1)
                 + ",\"ask1_volume\":" + std::to_string(tick->AskVolume1)
                 + ",\"volume\":" + std::to_string(tick->Volume));
        }
        changed.notify_all();
    }
    void wait(int seconds) {
        std::unique_lock<std::mutex> lock(mutex);
        changed.wait_for(lock, std::chrono::seconds(seconds), [&] {
            return failed || (logged_in && subscriptions == symbols.size() && ticks >= 3);
        });
    }
    // Call after Release(), which ends SDK callbacks before the final snapshot.
    int summary() {
        std::lock_guard<std::mutex> lock(mutex);
        emit("\"type\":\"summary\",\"connected\":" + std::string(connected ? "true" : "false")
             + ",\"logged_in\":" + (logged_in ? "true" : "false")
             + ",\"subscriptions\":" + std::to_string(subscriptions)
             + ",\"ticks\":" + std::to_string(ticks)
             + ",\"failed\":" + (failed ? "true" : "false"));
        if (failed || !logged_in || subscriptions != symbols.size()) return 2;
        return ticks >= 3 ? 0 : 3;
    }
};
} // namespace

int main(int argc, char** argv) {
    try {
        bool connect = false;
        int seconds = 20;
        std::string front, flow;
        std::vector<std::string> symbols;
        for (int i = 1; i < argc; ++i) {
            const std::string argument = argv[i];
            auto value = [&]() -> std::string {
                if (++i >= argc) throw std::runtime_error("option requires a value");
                return argv[i];
            };
            if (argument == "--connect") connect = true;
            else if (argument == "--front") front = value();
            else if (argument == "--flow-dir") flow = value();
            else if (argument == "--symbol") symbols.push_back(value());
            else if (argument == "--seconds") {
                const auto text = value();
                size_t consumed = 0;
                seconds = std::stoi(text, &consumed);
                if (consumed != text.size()) throw std::runtime_error("invalid duration");
            } else if (argument == "--help") {
                std::cout << "ctp-md-probe [--connect --front tcp://HOST:PORT --flow-dir PATH"
                             " --symbol CONTRACT [--symbol CONTRACT] [--seconds 20]]\n"
                             "Default: SDK version only; no credentials or network.\n"
                             "Connect uses the production TCP MdApi; never links TraderApi.\n"
                             "Credentials: CTP_BROKER_ID, CTP_USER_ID, CTP_PASSWORD.\n"
                             "Exit 0: subscriptions and >=3 ticks; 3: subscriptions but <3 ticks;"
                             " 2: incomplete or failed.\n";
                return 0;
            } else throw std::runtime_error("unknown option; use --help");
        }
        const char* version = CThostFtdcMdApi::GetApiVersion();
        if (!version) throw std::runtime_error("SDK returned no version");
        const std::string sdk_version(version);
        if (sdk_version.rfind("v6.7.11_", 0) != 0 && sdk_version.rfind("6.7.11_", 0) != 0)
            throw std::runtime_error("matching official CTP 6.7.11 SDK required");
        emit("\"type\":\"sdk_version\",\"value\":" + json_string(sdk_version));
        if (!connect) return 0;
        if (front.rfind("tcp://", 0) != 0 || front.size() <= 9 || front.find('\0') != std::string::npos)
            throw std::runtime_error("explicit TCP front required");
        if (flow.empty() || !std::filesystem::path(flow).is_absolute())
            throw std::runtime_error("absolute flow directory required");
        if (seconds < 1 || seconds > 300 || symbols.empty() || symbols.size() > 16)
            throw std::runtime_error("require 1..16 explicit symbols and 1..300 seconds");
        for (const auto& symbol : symbols) {
            if (symbol.empty() || symbol.size() > 80 || symbol.find_first_not_of(
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789") != std::string::npos)
                throw std::runtime_error("invalid symbol");
        }
        // A fresh flow directory keeps different tests/accounts from sharing SDK state.
        if (!std::filesystem::create_directory(flow))
            throw std::runtime_error("flow directory must not already exist; use a fresh path");
        std::filesystem::permissions(flow, std::filesystem::perms::owner_all);
        if (flow.back() != '/') flow += '/';
        Probe probe(std::move(symbols));
        probe.api = CThostFtdcMdApi::CreateFtdcMdApi(flow.c_str(), false, false, true);
        if (!probe.api) throw std::runtime_error("SDK factory returned null");
        probe.api->RegisterSpi(&probe);
        probe.api->RegisterFront(front.data());
        emit("\"type\":\"connecting\",\"front\":" + json_string(front));
        probe.api->Init();
        probe.wait(seconds);
        probe.api->RegisterSpi(nullptr);
        probe.api->Release();
        return probe.summary();
    } catch (const std::exception&) {
        // Never echo input values or a broker-supplied error message.
        std::cerr << "CTP market-data probe failed: check arguments, credential variable presence,"
                     " SDK version and the fresh flow path; use --help.\n";
        return 2;
    }
}
