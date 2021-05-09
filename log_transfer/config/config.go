package config

//全局的配置信息
type LogTransfer struct {
	Kafkacfg KafkaConf `ini:"KafkaConf"`
	EScfg    ESConf    `ini:"ES"`
}

//Kafka配置信息
type KafkaConf struct {
	Address string `ini:"Address"`
	Topic   string `ini:"Topic"`
}

//ES配置信息
type ESConf struct {
	Address     string `ini:"Address"`
	ChanMaxSize int    `ini:"ChanMaxSize"`
	Nums        int    `int:"Nums"`
}
