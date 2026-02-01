现在需要进行一个修改，我要新引入一个信号
pub enum SignalType {
    ArbOpen = 1,   // 套利开仓信号
    ArbHedge = 2,  // 套利对冲信号
    ArbCancel = 3, // 套利撤单信号
    ArbClose = 4,  // 套利平仓信号，和开仓信号类似，区别是如果对应方向头寸为0就不执行
}
新增3信号类型，
MMOpen 做市开仓信号
MMCancel 做市撤单信号
MMHedge 做市对冲信号

