package utils

import (
	"net"
	"strings"
)

//获取本机对外的IP地址
func GetOutboundIP() (IP string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	IP = strings.Split(localAddr.IP.String(), ":")[0]
	return
}
