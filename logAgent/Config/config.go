package Config

type ConfigAdd struct {
	KafkaConf
	EtcdConf
}
type KafkaConf struct {
	Address     string
	Topic       string
	ChanMaxSize int
}
type EtcdConf struct {
	Address string
	Key     string
	Timeout int
}

// type TaillogConf struct {
// 	LogFile string
// }
