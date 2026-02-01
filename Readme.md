现在我想在/Users/fanghaizhou/project/mkt_signal/src/strategy增加一个
src/strategy/mm_open_strategy.rs
核心逻辑如下
这个strategy用于做市模型的建仓操作，相比于arb会进行大幅度的简化。
1、没有强平模式
2、没有对冲侧，只有开仓侧
3、不需要区分mm和mt的模式
4、只有建仓的需求，当cancel和ffilled的最终状态之需要打印info日志即可

