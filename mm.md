1、现在我需要实现cancel的逻辑。
目前我的信号只有open的cancel信号，收到后撤销所有symbol的挂单。

现在我需要一个新的


2、注意cancel只通过account monitor的消息确认，trade engine的response只确定订单建立。


我还需要一个对冲余量表。在pre-trade维护。这个和敞口有区别。
敞口计算和头寸有关，这个只是为了处理对冲的尾量。
具体来说，因为订单的挂单有最小挂单量的限制，



