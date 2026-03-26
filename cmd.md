现在我想做2件事
一、对这种延迟的情况进行兜底。保证不要产生死单子。这套逻辑需要应用在mm open和xarb的strategy
具体的，目前这个订单丢失的原因是这样
1、报单后，很久收不到具体的交易所确认，导致认为订单已经丢了，在300ms后启动一个查询，试图补状态
2、query反查失败，认为order不存在，判定为open leg fail，且open不会再报单，因此直接alive = false，尝试毙掉
3、但之后又收到了order new的状态更新，此时其实订单已经是延迟，只是延迟太大了

处理方式是简单兜底
加一个兜底机制，如果收到了order update为new的情况，且是open leg（xarb和mm open），看一下当前alive是false的情况下，再save回来。

