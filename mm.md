1、现在，我要修改MM的strategy，对于account monitor传递的消息响应。
2、由于mm是mt复制过来的，我要告诉你mm和mt的区别是什么，然后你去对应的修改。
3、对于mm，收到open 发现定时器时的时候，发送cancel消息，这一点是相同的，保持不变。区别在于对cancel消息的响应不同。
对于mt strategy，收到cancel消息的时候，代表需要关闭。且成交消息必然在cancel消息前推送，因为cancel后不会再有成交，这是account 消息流内部有序保证了这一点。
因此，此时可以关闭margin的open单，如果um的对冲单也回收了，就可以被定时器移除。
对于mm strategy，收到cancel消息的时候，才会进行对冲，不会像mm一样，实时对冲。因此，此时会开一个新的msg，发送一个ReqBinSingleForwardArbHedgeMM，给fundig rate sinal 的 mm进程，即
通过mm arb的backward的icexy，发送给信号进程。注意，此时也会出现没有活跃订单的情况，但不可以移除这个strategy，你需要设计一个类似cancel pending的机制，或者能一并设计简化更好。

这个msg需要包含这些内容：
stratey id，cancel的"E":1762093587776， "s":"SAGAUSDT", symbol， 累计成交的量是多少以及订单具体成交了多少



