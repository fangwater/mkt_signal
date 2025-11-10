//前端展示采样，均为pub
struct ResampleChannel{
    resample_positions_pub: Option<ResamplePublisher>,
    resample_exposure_pub: Option<ResamplePublisher>,
    resample_risk_pub: Option<ResamplePublisher>
}

//交易相关
struct TradeChannel{
    //信号从上游下发到pre-trade，sub
    //交易请求从pre-trade发送给trade engine，pub
}

struct AccountMonitorChannel{
    //订阅accout monitor
}


//持久化记录，包括信号记录和订单记录
struct PersistChannel{
}

